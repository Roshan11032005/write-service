package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/segmentio/kafka-go"
)

// ═══════════════════════════════════════════════════════════════
//  Configuration
// ═══════════════════════════════════════════════════════════════

type TestConfig struct {
	KafkaBroker      string
	KafkaTopic       string
	WriteServiceURL  string
	ReadServiceURL   string
	IndexBuilderURL  string
	RocksDBShardURLs []string
	S3Endpoint       string
	S3Bucket         string
	S3AccessKey      string
	S3SecretKey      string
	S3Region         string
}

func loadTestConfig() TestConfig {
	return TestConfig{
		KafkaBroker:     getEnv("KAFKA_BROKER", "localhost:9092"),
		KafkaTopic:      getEnv("KAFKA_TOPIC", "events"),
		WriteServiceURL: getEnv("WRITE_SERVICE_URL", "http://localhost:8085"),
		ReadServiceURL:  getEnv("READ_SERVICE_URL", "http://localhost:8087"),
		IndexBuilderURL: getEnv("INDEX_BUILDER_URL", "http://localhost:8086"),
		RocksDBShardURLs: []string{
			getEnv("SHARD_0", "http://localhost:4001"),
			getEnv("SHARD_1", "http://localhost:4002"),
			getEnv("SHARD_2", "http://localhost:4003"),
		},
		S3Endpoint:  getEnv("S3_ENDPOINT", "http://localhost:9002"),
		S3Bucket:    getEnv("S3_BUCKET", "events-bucket"),
		S3AccessKey: getEnv("S3_ACCESS_KEY", "admin"),
		S3SecretKey: getEnv("S3_SECRET_KEY", "strongpassword"),
		S3Region:    getEnv("S3_REGION", "us-east-1"),
	}
}

// ═══════════════════════════════════════════════════════════════
//  Test Statistics
// ═══════════════════════════════════════════════════════════════

type TestStats struct {
	TotalTests   atomic.Int64
	PassedTests  atomic.Int64
	FailedTests  atomic.Int64
	SkippedTests atomic.Int64
	StartTime    time.Time
	mu           sync.Mutex
	failures     []string
}

func NewTestStats() *TestStats {
	return &TestStats{
		StartTime: time.Now(),
		failures:  make([]string, 0),
	}
}

func (s *TestStats) Pass(testName string) {
	s.TotalTests.Add(1)
	s.PassedTests.Add(1)
	fmt.Printf("✅ PASS: %s\n", testName)
}

func (s *TestStats) Fail(testName, reason string) {
	s.TotalTests.Add(1)
	s.FailedTests.Add(1)
	s.mu.Lock()
	s.failures = append(s.failures, fmt.Sprintf("%s: %s", testName, reason))
	s.mu.Unlock()
	fmt.Printf("❌ FAIL: %s - %s\n", testName, reason)
}

func (s *TestStats) Skip(testName, reason string) {
	s.TotalTests.Add(1)
	s.SkippedTests.Add(1)
	fmt.Printf("⏭️  SKIP: %s - %s\n", testName, reason)
}

func (s *TestStats) PrintSummary() {
	elapsed := time.Since(s.StartTime)
	fmt.Printf("\n")
	fmt.Printf("╔═══════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║                        TEST SUMMARY                           ║\n")
	fmt.Printf("╚═══════════════════════════════════════════════════════════════╝\n")
	fmt.Printf("  Total Tests:   %d\n", s.TotalTests.Load())
	fmt.Printf("  ✅ Passed:     %d\n", s.PassedTests.Load())
	fmt.Printf("  ❌ Failed:     %d\n", s.FailedTests.Load())
	fmt.Printf("  ⏭️  Skipped:    %d\n", s.SkippedTests.Load())
	fmt.Printf("  Duration:      %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("═══════════════════════════════════════════════════════════════\n")

	if s.FailedTests.Load() > 0 {
		fmt.Printf("\n❌ FAILED TESTS:\n")
		s.mu.Lock()
		for i, failure := range s.failures {
			fmt.Printf("  %d. %s\n", i+1, failure)
		}
		s.mu.Unlock()
	}

	if s.FailedTests.Load() == 0 {
		fmt.Printf("\n🎉 ALL TESTS PASSED!\n")
	}
}

// ═══════════════════════════════════════════════════════════════
//  Test Types
// ═══════════════════════════════════════════════════════════════

type QueryRequest struct {
	ReferenceIDs []string `json:"reference_ids"`
	StartTime    int64    `json:"start_time,omitempty"`
	EndTime      int64    `json:"end_time,omitempty"`
	Category     string   `json:"category,omitempty"`
	EventType    string   `json:"event_type,omitempty"`
	Limit        int      `json:"limit,omitempty"`
}

type QueryResponse struct {
	Events      []Event      `json:"events"`
	TotalEvents int          `json:"total_events"`
	QueryTimeMs int64        `json:"query_time_ms"`
	Metrics     QueryMetrics `json:"metrics"`
}

type Event struct {
	EventID     string                 `json:"event_id"`
	ReferenceID string                 `json:"reference_id"`
	Timestamp   int64                  `json:"timestamp"`
	Category    string                 `json:"category"`
	EventType   string                 `json:"event_type"`
	Data        map[string]interface{} `json:"data"`
}

type QueryMetrics struct {
	RocksDBQueriesCount  int   `json:"rocksdb_queries_count"`
	RocksDBQueryTimeMs   int64 `json:"rocksdb_query_time_ms"`
	S3FetchesCount       int   `json:"s3_fetches_count"`
	S3FetchTimeMs        int64 `json:"s3_fetch_time_ms"`
	MetadataEntriesFound int   `json:"metadata_entries_found"`
	EventsParsed         int   `json:"events_parsed"`
	BytesRead            int64 `json:"bytes_read"`
}

// ═══════════════════════════════════════════════════════════════
//  Test Runner
// ═══════════════════════════════════════════════════════════════

type TestRunner struct {
	cfg      TestConfig
	stats    *TestStats
	s3Client *s3.Client
	http     *http.Client
}

func NewTestRunner(cfg TestConfig) (*TestRunner, error) {
	stats := NewTestStats()

	awsCfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(cfg.S3Region),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.S3AccessKey, cfg.S3SecretKey, ""),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.EndpointResolver = s3.EndpointResolverFunc(
			func(region string, options s3.EndpointResolverOptions) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL:               cfg.S3Endpoint,
					HostnameImmutable: true,
					SigningRegion:     cfg.S3Region,
				}, nil
			},
		)
		o.UsePathStyle = true
	})

	return &TestRunner{
		cfg:      cfg,
		stats:    stats,
		s3Client: s3Client,
		http:     &http.Client{Timeout: 30 * time.Second},
	}, nil
}

// ═══════════════════════════════════════════════════════════════
//  Service Health Tests
// ═══════════════════════════════════════════════════════════════

func (r *TestRunner) TestServiceHealth() {
	fmt.Printf("\n╔═══════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║              SERVICE HEALTH TESTS                             ║\n")
	fmt.Printf("╚═══════════════════════════════════════════════════════════════╝\n\n")

	// Test Write Service
	resp, err := r.http.Get(r.cfg.WriteServiceURL + "/internal/health")
	if err != nil || resp.StatusCode != http.StatusOK {
		r.stats.Fail("Write Service Health", fmt.Sprintf("error: %v, status: %v", err, resp))
	} else {
		r.stats.Pass("Write Service Health")
		resp.Body.Close()
	}

	// Test Read Service
	resp, err = r.http.Get(r.cfg.ReadServiceURL + "/internal/health")
	if err != nil || resp.StatusCode != http.StatusOK {
		r.stats.Fail("Read Service Health", fmt.Sprintf("error: %v, status: %v", err, resp))
	} else {
		r.stats.Pass("Read Service Health")
		resp.Body.Close()
	}

	// Test Index Builder
	resp, err = r.http.Get(r.cfg.IndexBuilderURL + "/internal/health")
	if err != nil || resp.StatusCode != http.StatusOK {
		r.stats.Fail("Index Builder Health", fmt.Sprintf("error: %v, status: %v", err, resp))
	} else {
		r.stats.Pass("Index Builder Health")
		resp.Body.Close()
	}

	// Test RocksDB Shards
	for i, shardURL := range r.cfg.RocksDBShardURLs {
		resp, err := r.http.Get(shardURL + "/health")
		testName := fmt.Sprintf("RocksDB Shard %d Health", i)
		if err != nil || resp.StatusCode != http.StatusOK {
			r.stats.Fail(testName, fmt.Sprintf("error: %v, status: %v", err, resp))
		} else {
			r.stats.Pass(testName)
			resp.Body.Close()
		}
	}

	// Test Kafka
	conn, err := kafka.Dial("tcp", r.cfg.KafkaBroker)
	if err != nil {
		r.stats.Fail("Kafka Connectivity", err.Error())
	} else {
		conn.Close()
		r.stats.Pass("Kafka Connectivity")
	}

	// Test S3
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = r.s3Client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(r.cfg.S3Bucket),
	})
	if err != nil {
		r.stats.Fail("S3 Bucket Accessibility", err.Error())
	} else {
		r.stats.Pass("S3 Bucket Accessibility")
	}
}

// ═════════════════════���═════════════════════════════════════════
//  Kafka Write Tests
// ═══════════════════════════════════════════════════════════════

func (r *TestRunner) TestKafkaWrite() {
	fmt.Printf("\n╔═══════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║                   KAFKA WRITE TESTS                           ║\n")
	fmt.Printf("╚═══════════════════════════════════════════════════════════════╝\n\n")

	writer := &kafka.Writer{
		Addr:         kafka.TCP(r.cfg.KafkaBroker),
		Topic:        r.cfg.KafkaTopic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireOne,
	}
	defer writer.Close()

	// Test 1: Single Message Write
	testRefID := fmt.Sprintf("test-kafka-%d", time.Now().Unix())
	event := map[string]interface{}{
		"_event_id":        fmt.Sprintf("evt-%d", time.Now().UnixNano()),
		"_event_timestamp": time.Now().Format(time.RFC3339),
		"_category":        "test",
		"_event_type":      "kafka_test",
		"reference_id":     testRefID,
		"_data": map[string]interface{}{
			"test": "kafka_write",
		},
	}

	payload, _ := json.Marshal(event)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	err := writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(testRefID),
		Value: payload,
	})
	cancel()

	if err != nil {
		r.stats.Fail("Kafka Single Message Write", err.Error())
	} else {
		r.stats.Pass("Kafka Single Message Write")
	}

	// Test 2: Batch Write
	var messages []kafka.Message
	batchRefID := fmt.Sprintf("test-kafka-batch-%d", time.Now().Unix())
	for i := 0; i < 10; i++ {
		event := map[string]interface{}{
			"_event_id":        fmt.Sprintf("evt-batch-%d-%d", time.Now().UnixNano(), i),
			"_event_timestamp": time.Now().Format(time.RFC3339),
			"_category":        "test",
			"_event_type":      "kafka_batch_test",
			"reference_id":     batchRefID,
			"_data": map[string]interface{}{
				"batch_index": i,
			},
		}
		payload, _ := json.Marshal(event)
		messages = append(messages, kafka.Message{
			Key:   []byte(batchRefID),
			Value: payload,
		})
	}

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	err = writer.WriteMessages(ctx, messages...)
	cancel()

	if err != nil {
		r.stats.Fail("Kafka Batch Write (10 messages)", err.Error())
	} else {
		r.stats.Pass("Kafka Batch Write (10 messages)")
	}
}

// ═══════════════════════════════════════════════════════════════
//  Write Service Tests
// ═══════════════════════════════════════════════════════════════

func (r *TestRunner) TestWriteService() {
	fmt.Printf("\n╔═══════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║                WRITE SERVICE TESTS                            ║\n")
	fmt.Printf("╚═══════════════════════════════════════════════════════════════╝\n\n")

	// Test 1: Metrics Endpoint
	resp, err := r.http.Get(r.cfg.WriteServiceURL + "/internal/metrics")
	if err != nil {
		r.stats.Fail("Write Service Metrics", err.Error())
	} else if resp.StatusCode != http.StatusOK {
		r.stats.Fail("Write Service Metrics", fmt.Sprintf("status: %d", resp.StatusCode))
	} else {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var metrics map[string]interface{}
		if err := json.Unmarshal(body, &metrics); err != nil {
			r.stats.Fail("Write Service Metrics Parse", err.Error())
		} else {
			r.stats.Pass("Write Service Metrics")
		}
	}

	// Test 2: Force Flush
	req, _ := http.NewRequest(http.MethodPost, r.cfg.WriteServiceURL+"/internal/flush", nil)
	resp, err = r.http.Do(req)
	if err != nil {
		r.stats.Fail("Write Service Force Flush", err.Error())
	} else if resp.StatusCode != http.StatusOK {
		r.stats.Fail("Write Service Force Flush", fmt.Sprintf("status: %d", resp.StatusCode))
		resp.Body.Close()
	} else {
		r.stats.Pass("Write Service Force Flush")
		resp.Body.Close()
	}

	// Test 3: WAL Status
	resp, err = r.http.Get(r.cfg.WriteServiceURL + "/internal/wal")
	if err != nil {
		r.stats.Fail("Write Service WAL Status", err.Error())
	} else if resp.StatusCode != http.StatusOK {
		r.stats.Fail("Write Service WAL Status", fmt.Sprintf("status: %d", resp.StatusCode))
		resp.Body.Close()
	} else {
		r.stats.Pass("Write Service WAL Status")
		resp.Body.Close()
	}
}

// ═══════════════════════════════════════════════════════════════
//  Index Builder Tests
// ═══════════════════════════════════════════════════════════════

func (r *TestRunner) TestIndexBuilder() {
	fmt.Printf("\n╔═══════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║                INDEX BUILDER TESTS                            ║\n")
	fmt.Printf("╚═══════════════════════════════════════════════════════════════╝\n\n")

	// Test 1: Metrics
	resp, err := r.http.Get(r.cfg.IndexBuilderURL + "/internal/metrics")
	if err != nil {
		r.stats.Fail("Index Builder Metrics", err.Error())
	} else if resp.StatusCode != http.StatusOK {
		r.stats.Fail("Index Builder Metrics", fmt.Sprintf("status: %d", resp.StatusCode))
		resp.Body.Close()
	} else {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var metrics map[string]interface{}
		if err := json.Unmarshal(body, &metrics); err != nil {
			r.stats.Fail("Index Builder Metrics Parse", err.Error())
		} else {
			r.stats.Pass("Index Builder Metrics")
			fmt.Printf("   Segments Processed: %.0f\n", metrics["total_segments_processed"])
			fmt.Printf("   Events Indexed: %.0f\n", metrics["total_events_indexed"])
			fmt.Printf("   Compactions: %.0f\n", metrics["total_compactions"])
		}
	}

	// Test 2: Cursor Status
	resp, err = r.http.Get(r.cfg.IndexBuilderURL + "/internal/cursor")
	if err != nil {
		r.stats.Fail("Index Builder Cursor Status", err.Error())
	} else if resp.StatusCode != http.StatusOK {
		r.stats.Fail("Index Builder Cursor Status", fmt.Sprintf("status: %d", resp.StatusCode))
		resp.Body.Close()
	} else {
		r.stats.Pass("Index Builder Cursor Status")
		resp.Body.Close()
	}

	// Test 3: Shards Configuration
	resp, err = r.http.Get(r.cfg.IndexBuilderURL + "/internal/shards")
	if err != nil {
		r.stats.Fail("Index Builder Shards Config", err.Error())
	} else if resp.StatusCode != http.StatusOK {
		r.stats.Fail("Index Builder Shards Config", fmt.Sprintf("status: %d", resp.StatusCode))
		resp.Body.Close()
	} else {
		r.stats.Pass("Index Builder Shards Config")
		resp.Body.Close()
	}

	// Test 4: Force Compaction
	req, _ := http.NewRequest(http.MethodPost, r.cfg.IndexBuilderURL+"/internal/compact", nil)
	resp, err = r.http.Do(req)
	if err != nil {
		r.stats.Fail("Index Builder Force Compaction", err.Error())
	} else if resp.StatusCode != http.StatusOK {
		r.stats.Fail("Index Builder Force Compaction", fmt.Sprintf("status: %d", resp.StatusCode))
		resp.Body.Close()
	} else {
		r.stats.Pass("Index Builder Force Compaction")
		resp.Body.Close()
	}
}

// ═══════════════════════════════════════════════════════════════
//  RocksDB Shard Tests
// ═══════════════════════════════════════════════════════════════

func (r *TestRunner) TestRocksDBShards() {
	fmt.Printf("\n╔═══════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║                  ROCKSDB SHARD TESTS                          ║\n")
	fmt.Printf("╚═══════════════════════════════════════════════════════════════╝\n\n")

	for i, shardURL := range r.cfg.RocksDBShardURLs {
		testKey := fmt.Sprintf("test-key-%d-%d", i, time.Now().Unix())
		testValue := fmt.Sprintf("test-value-%d", time.Now().Unix())

		// Test 1: PUT
		putData, _ := json.Marshal(map[string]string{
			"key":   testKey,
			"value": testValue,
		})
		resp, err := r.http.Post(shardURL+"/put", "application/json", bytes.NewReader(putData))
		testName := fmt.Sprintf("Shard %d PUT", i)
		if err != nil || resp.StatusCode != http.StatusOK {
			r.stats.Fail(testName, fmt.Sprintf("error: %v, status: %v", err, resp))
			if resp != nil {
				resp.Body.Close()
			}
			continue
		}
		resp.Body.Close()
		r.stats.Pass(testName)

		// Test 2: GET
		time.Sleep(100 * time.Millisecond) // Allow write to settle
		resp, err = r.http.Get(fmt.Sprintf("%s/get?key=%s", shardURL, testKey))
		testName = fmt.Sprintf("Shard %d GET", i)
		if err != nil || resp.StatusCode != http.StatusOK {
			r.stats.Fail(testName, fmt.Sprintf("error: %v, status: %v", err, resp))
			if resp != nil {
				resp.Body.Close()
			}
			continue
		}

		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		var getResp struct {
			Status string `json:"status"`
			Value  string `json:"value"`
		}
		if err := json.Unmarshal(body, &getResp); err != nil {
			r.stats.Fail(testName+" Parse", err.Error())
			continue
		}

		if getResp.Value != testValue {
			r.stats.Fail(testName+" Value Match", fmt.Sprintf("expected %s, got %s", testValue, getResp.Value))
		} else {
			r.stats.Pass(testName)
		}

		// Test 3: Batch PUT
		batchData := []map[string]string{
			{"key": fmt.Sprintf("batch-key-%d-1", i), "value": "batch-value-1"},
			{"key": fmt.Sprintf("batch-key-%d-2", i), "value": "batch-value-2"},
			{"key": fmt.Sprintf("batch-key-%d-3", i), "value": "batch-value-3"},
		}
		batchJSON, _ := json.Marshal(batchData)
		resp, err = r.http.Post(shardURL+"/batch_put", "application/json", bytes.NewReader(batchJSON))
		testName = fmt.Sprintf("Shard %d Batch PUT", i)
		if err != nil || resp.StatusCode != http.StatusOK {
			r.stats.Fail(testName, fmt.Sprintf("error: %v, status: %v", err, resp))
			if resp != nil {
				resp.Body.Close()
			}
		} else {
			resp.Body.Close()
			r.stats.Pass(testName)
		}
	}
}

// ═══════════════════════════════════════════════════════════════
//  S3 Storage Tests
// ═══════════════════════════════════════════════════════════════

func (r *TestRunner) TestS3Storage() {
	fmt.Printf("\n╔═══════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║                    S3 STORAGE TESTS                           ║\n")
	fmt.Printf("╚═══════════════════════════════════════════════════════════════╝\n\n")

	ctx := context.Background()

	// Test 1: List Segment Files
	result, err := r.s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(r.cfg.S3Bucket),
		Prefix: aws.String("events/"),
	})
	if err != nil {
		r.stats.Fail("S3 List Segment Files", err.Error())
	} else {
		r.stats.Pass("S3 List Segment Files")
		fmt.Printf("   Found %d segment files\n", len(result.Contents))
	}

	// Test 2: List SST Files
	result, err = r.s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(r.cfg.S3Bucket),
		Prefix: aws.String("rocksdb-sst/"),
	})
	if err != nil {
		r.stats.Fail("S3 List SST Files", err.Error())
	} else {
		r.stats.Pass("S3 List SST Files")
		fmt.Printf("   Found %d SST files\n", len(result.Contents))
	}

	// Test 3: Check Cursor File
	_, err = r.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(r.cfg.S3Bucket),
		Key:    aws.String("_internal/cursor.json"),
	})
	if err != nil {
		r.stats.Fail("S3 Cursor File Exists", err.Error())
	} else {
		r.stats.Pass("S3 Cursor File Exists")
	}

	// Test 4: Verify Segment File Content
	if result != nil && len(result.Contents) > 0 {
		// Get first segment file
		firstKey := aws.ToString(result.Contents[0].Key)
		resp, err := r.s3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(r.cfg.S3Bucket),
			Key:    aws.String(firstKey),
		})
		if err != nil {
			r.stats.Fail("S3 Download Segment File", err.Error())
		} else {
			data, _ := io.ReadAll(resp.Body)
			resp.Body.Close()

			lines := strings.Split(string(data), "\n")
			validEvents := 0
			for _, line := range lines {
				if strings.TrimSpace(line) == "" {
					continue
				}
				var event map[string]interface{}
				if err := json.Unmarshal([]byte(line), &event); err == nil {
					validEvents++
				}
			}

			if validEvents > 0 {
				r.stats.Pass("S3 Segment File Content Valid")
				fmt.Printf("   File: %s\n", firstKey)
				fmt.Printf("   Valid Events: %d\n", validEvents)
			} else {
				r.stats.Fail("S3 Segment File Content Valid", "no valid events found")
			}
		}
	}
}

// ═══════════════════════════════════════════════════════════════
//  Read Service Query Tests
// ═══════════════════════════════════════════════════════════════

func (r *TestRunner) TestReadServiceQueries() {
	fmt.Printf("\n╔═══════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║              READ SERVICE QUERY TESTS                         ║\n")
	fmt.Printf("╚═══════════════════════════════════════════════════════════════╝\n\n")

	// Load test reference IDs
	refIDs := r.loadTestReferenceIDs()
	if len(refIDs) == 0 {
		r.stats.Skip("Read Service Queries", "No test reference IDs found")
		return
	}

	// Test 1: Single Reference ID Query
	req := QueryRequest{
		ReferenceIDs: []string{refIDs[0]},
	}
	resp, err := r.executeQuery(req)
	if err != nil {
		r.stats.Fail("Query Single Reference ID", err.Error())
	} else {
		r.stats.Pass("Query Single Reference ID")
		fmt.Printf("   Ref ID: %s\n", refIDs[0])
		fmt.Printf("   Events: %d\n", resp.TotalEvents)
		fmt.Printf("   Query Time: %dms\n", resp.QueryTimeMs)
		fmt.Printf("   RocksDB Queries: %d\n", resp.Metrics.RocksDBQueriesCount)
		fmt.Printf("   S3 Fetches: %d\n", resp.Metrics.S3FetchesCount)
	}

	// Test 2: Multiple Reference IDs Query
	if len(refIDs) >= 3 {
		req := QueryRequest{
			ReferenceIDs: refIDs[:3],
		}
		resp, err := r.executeQuery(req)
		if err != nil {
			r.stats.Fail("Query Multiple Reference IDs", err.Error())
		} else {
			r.stats.Pass("Query Multiple Reference IDs")
			fmt.Printf("   Ref IDs: %v\n", refIDs[:3])
			fmt.Printf("   Events: %d\n", resp.TotalEvents)
			fmt.Printf("   Query Time: %dms\n", resp.QueryTimeMs)
		}
	}

	// Test 3: Query with Time Range
	now := time.Now()
	startTime := now.Add(-24 * time.Hour).Unix()
	endTime := now.Unix()
	req = QueryRequest{
		ReferenceIDs: []string{refIDs[0]},
		StartTime:    startTime,
		EndTime:      endTime,
	}
	resp, err = r.executeQuery(req)
	if err != nil {
		r.stats.Fail("Query with Time Range", err.Error())
	} else {
		r.stats.Pass("Query with Time Range")
		fmt.Printf("   Events: %d\n", resp.TotalEvents)
	}

	// Test 4: Query with Category Filter
	req = QueryRequest{
		ReferenceIDs: []string{refIDs[0]},
		Category:     "Auth_tran",
	}
	resp, err = r.executeQuery(req)
	if err != nil {
		r.stats.Fail("Query with Category Filter", err.Error())
	} else {
		r.stats.Pass("Query with Category Filter")
		fmt.Printf("   Events: %d\n", resp.TotalEvents)
	}

	// Test 5: Query with Limit
	req = QueryRequest{
		ReferenceIDs: []string{refIDs[0]},
		Limit:        10,
	}
	resp, err = r.executeQuery(req)
	if err != nil {
		r.stats.Fail("Query with Limit", err.Error())
	} else if resp.TotalEvents > 10 {
		r.stats.Fail("Query with Limit", fmt.Sprintf("expected <=10 events, got %d", resp.TotalEvents))
	} else {
		r.stats.Pass("Query with Limit")
		fmt.Printf("   Events: %d (limit: 10)\n", resp.TotalEvents)
	}

	// Test 6: Query Non-existent Reference ID
	req = QueryRequest{
		ReferenceIDs: []string{"non-existent-ref-id-12345"},
	}
	resp, err = r.executeQuery(req)
	if err != nil {
		r.stats.Fail("Query Non-existent Reference ID", err.Error())
	} else if resp.TotalEvents != 0 {
		r.stats.Fail("Query Non-existent Reference ID", fmt.Sprintf("expected 0 events, got %d", resp.TotalEvents))
	} else {
		r.stats.Pass("Query Non-existent Reference ID")
	}

	// Test 7: Query with Invalid Request (empty reference IDs)
	req = QueryRequest{
		ReferenceIDs: []string{},
	}
	_, err = r.executeQuery(req)
	if err == nil {
		r.stats.Fail("Query with Empty Reference IDs", "expected error but got success")
	} else {
		r.stats.Pass("Query with Empty Reference IDs")
	}
}

// ═══════════════════════════════════════════════════════════════
//  End-to-End Flow Tests
// ═══════════════════════════════════════════════════════════════

func (r *TestRunner) TestEndToEndFlow() {
	fmt.Printf("\n╔═══════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║                 END-TO-END FLOW TESTS                         ║\n")
	fmt.Printf("╚═══════════════════════════════════════════════════════════════╝\n\n")

	// Test: Write → Index → Read Flow
	testRefID := fmt.Sprintf("e2e-test-%d", time.Now().Unix())
	testEventID := fmt.Sprintf("evt-e2e-%d", time.Now().UnixNano())

	// Step 1: Write event to Kafka
	fmt.Printf("   Step 1: Writing event to Kafka...\n")
	writer := &kafka.Writer{
		Addr:         kafka.TCP(r.cfg.KafkaBroker),
		Topic:        r.cfg.KafkaTopic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireOne,
	}
	defer writer.Close()

	event := map[string]interface{}{
		"_event_id":        testEventID,
		"_event_timestamp": time.Now().Format(time.RFC3339),
		"_category":        "e2e_test",
		"_event_type":      "end_to_end_test",
		"reference_id":     testRefID,
		"_data": map[string]interface{}{
			"test_field": "e2e_test_value",
			"timestamp":  time.Now().Unix(),
		},
	}

	payload, _ := json.Marshal(event)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	err := writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(testRefID),
		Value: payload,
	})
	cancel()

	if err != nil {
		r.stats.Fail("E2E: Write to Kafka", err.Error())
		return
	}
	fmt.Printf("   ✓ Event written to Kafka\n")

	// Step 2: Wait for write service to consume and flush
	fmt.Printf("   Step 2: Waiting for write service to process (30s)...\n")
	time.Sleep(30 * time.Second)

	// Trigger flush
	req, _ := http.NewRequest(http.MethodPost, r.cfg.WriteServiceURL+"/internal/flush", nil)
	r.http.Do(req)
	time.Sleep(5 * time.Second)

	// Step 3: Wait for index builder to process
	fmt.Printf("   Step 3: Waiting for index builder to process (30s)...\n")
	time.Sleep(30 * time.Second)

	// Step 4: Query the event
	fmt.Printf("   Step 4: Querying event from read service...\n")
	queryReq := QueryRequest{
		ReferenceIDs: []string{testRefID},
	}
	queryResp, err := r.executeQuery(queryReq)
	if err != nil {
		r.stats.Fail("E2E: Query Event", err.Error())
		return
	}

	if queryResp.TotalEvents == 0 {
		r.stats.Fail("E2E: Event Found", "event not found after indexing")
		return
	}

	// Step 5: Verify event content
	found := false
	for _, e := range queryResp.Events {
		if e.EventID == testEventID {
			found = true
			if e.ReferenceID != testRefID {
				r.stats.Fail("E2E: Event Reference ID", fmt.Sprintf("expected %s, got %s", testRefID, e.ReferenceID))
				return
			}
			if e.Category != "e2e_test" {
				r.stats.Fail("E2E: Event Category", fmt.Sprintf("expected e2e_test, got %s", e.Category))
				return
			}
			break
		}
	}

	if !found {
		r.stats.Fail("E2E: Event ID Match", fmt.Sprintf("event ID %s not found in results", testEventID))
		return
	}

	r.stats.Pass("End-to-End Flow: Write → Index → Read")
	fmt.Printf("   ✓ Event successfully written, indexed, and retrieved\n")
	fmt.Printf("   Ref ID: %s\n", testRefID)
	fmt.Printf("   Event ID: %s\n", testEventID)
	fmt.Printf("   Total Events: %d\n", queryResp.TotalEvents)
	fmt.Printf("   Query Time: %dms\n", queryResp.QueryTimeMs)
}

// ═══════════════════════════════════════════════════════════════
//  Performance Tests
// ═══════════════════════════════════════════════════════════════

func (r *TestRunner) TestPerformance() {
	fmt.Printf("\n╔═══════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║                   PERFORMANCE TESTS                           ║\n")
	fmt.Printf("╚═══════════════════════════════════════════════════════════════╝\n\n")

	refIDs := r.loadTestReferenceIDs()
	if len(refIDs) == 0 {
		r.stats.Skip("Performance Tests", "No test reference IDs found")
		return
	}

	// Test 1: Query Response Time
	var totalTime int64
	iterations := 10
	for i := 0; i < iterations; i++ {
		req := QueryRequest{
			ReferenceIDs: []string{refIDs[0]},
		}
		resp, err := r.executeQuery(req)
		if err == nil {
			totalTime += resp.QueryTimeMs
		}
	}
	avgTime := totalTime / int64(iterations)
	fmt.Printf("   Average Query Time: %dms (%d iterations)\n", avgTime, iterations)

	if avgTime > 5000 {
		r.stats.Fail("Query Performance", fmt.Sprintf("average query time %dms exceeds 5s threshold", avgTime))
	} else {
		r.stats.Pass("Query Performance")
	}

	// Test 2: Concurrent Queries
	concurrency := 10
	var wg sync.WaitGroup
	errors := atomic.Int64{}
	start := time.Now()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			req := QueryRequest{
				ReferenceIDs: []string{refIDs[idx%len(refIDs)]},
			}
			_, err := r.executeQuery(req)
			if err != nil {
				errors.Add(1)
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("   Concurrent Queries: %d\n", concurrency)
	fmt.Printf("   Total Time: %s\n", elapsed)
	fmt.Printf("   Errors: %d\n", errors.Load())
	fmt.Printf("   Queries/sec: %.2f\n", float64(concurrency)/elapsed.Seconds())

	if errors.Load() > 0 {
		r.stats.Fail("Concurrent Query Test", fmt.Sprintf("%d errors in %d queries", errors.Load(), concurrency))
	} else {
		r.stats.Pass("Concurrent Query Test")
	}
}

// ═══════════════════════════════════════════════════════════════
//  Data Integrity Tests
// ═══════════════════════════════════════════════════════════════

func (r *TestRunner) TestDataIntegrity() {
	fmt.Printf("\n╔═══════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║                DATA INTEGRITY TESTS                           ║\n")
	fmt.Printf("╚═══════════════════════════════════════════════════════════════╝\n\n")

	refIDs := r.loadTestReferenceIDs()
	if len(refIDs) == 0 {
		r.stats.Skip("Data Integrity Tests", "No test reference IDs found")
		return
	}

	// Test 1: Event Count Consistency
	req := QueryRequest{
		ReferenceIDs: []string{refIDs[0]},
	}
	resp1, err1 := r.executeQuery(req)
	time.Sleep(2 * time.Second)
	resp2, err2 := r.executeQuery(req)

	if err1 != nil || err2 != nil {
		r.stats.Fail("Event Count Consistency", "query failed")
	} else if resp1.TotalEvents != resp2.TotalEvents {
		r.stats.Fail("Event Count Consistency", fmt.Sprintf("count changed: %d -> %d", resp1.TotalEvents, resp2.TotalEvents))
	} else {
		r.stats.Pass("Event Count Consistency")
		fmt.Printf("   Consistent event count: %d\n", resp1.TotalEvents)
	}

	// Test 2: Event Order (timestamps should be sorted)
	if resp1 != nil && len(resp1.Events) > 1 {
		sorted := true
		for i := 1; i < len(resp1.Events); i++ {
			if resp1.Events[i].Timestamp < resp1.Events[i-1].Timestamp {
				sorted = false
				break
			}
		}
		if !sorted {
			r.stats.Fail("Event Timestamp Order", "events are not sorted by timestamp")
		} else {
			r.stats.Pass("Event Timestamp Order")
		}
	}

	// Test 3: Reference ID Accuracy
	if resp1 != nil {
		allMatch := true
		for _, event := range resp1.Events {
			if event.ReferenceID != refIDs[0] {
				allMatch = false
				break
			}
		}
		if !allMatch {
			r.stats.Fail("Reference ID Accuracy", "found events with incorrect reference ID")
		} else {
			r.stats.Pass("Reference ID Accuracy")
		}
	}

	// Test 4: Event ID Uniqueness
	if resp1 != nil && len(resp1.Events) > 0 {
		eventIDMap := make(map[string]bool)
		duplicates := 0
		for _, event := range resp1.Events {
			if eventIDMap[event.EventID] {
				duplicates++
			}
			eventIDMap[event.EventID] = true
		}
		if duplicates > 0 {
			r.stats.Fail("Event ID Uniqueness", fmt.Sprintf("found %d duplicate event IDs", duplicates))
		} else {
			r.stats.Pass("Event ID Uniqueness")
		}
	}
}

// ═══════════════════════════════════════════════════════════════
//  Helper Functions
// ═══════════════════════════════════════════════════════════════

func (r *TestRunner) executeQuery(req QueryRequest) (*QueryResponse, error) {
	data, _ := json.Marshal(req)
	resp, err := r.http.Post(r.cfg.ReadServiceURL+"/query", "application/json", bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, string(body))
	}

	body, _ := io.ReadAll(resp.Body)
	var queryResp QueryResponse
	if err := json.Unmarshal(body, &queryResp); err != nil {
		return nil, err
	}

	return &queryResp, nil
}

func (r *TestRunner) loadTestReferenceIDs() []string {
	// Try to load from file first
	data, err := os.ReadFile("test_reference_ids.txt")
	if err == nil {
		lines := strings.Split(string(data), "\n")
		var refIDs []string
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line != "" {
				refIDs = append(refIDs, line)
			}
		}
		if len(refIDs) > 0 {
			return refIDs
		}
	}

	// Fallback to hardcoded list
	return []string{
		"test-user-00000001",
		"test-user-00000002",
		"test-user-00000003",
		"test-user-00000004",
		"test-user-00000005",
	}
}

// ═══════════════════════════════════════════════════════════════
//  Main Test Runner
// ═══════════════════════════════════════════════════════════════

func main() {
	fmt.Printf("\n")
	fmt.Printf("╔═══════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║         EVENT STREAMING PIPELINE - COMPREHENSIVE TESTS       ║\n")
	fmt.Printf("╚═══════════════════════════════════════════════════════════════╝\n")

	cfg := loadTestConfig()
	runner, err := NewTestRunner(cfg)
	if err != nil {
		log.Fatalf("Failed to initialize test runner: %v", err)
	}

	fmt.Printf("\n📋 Test Configuration:\n")
	fmt.Printf("   Kafka Broker:      %s\n", cfg.KafkaBroker)
	fmt.Printf("   Write Service:     %s\n", cfg.WriteServiceURL)
	fmt.Printf("   Read Service:      %s\n", cfg.ReadServiceURL)
	fmt.Printf("   Index Builder:     %s\n", cfg.IndexBuilderURL)
	fmt.Printf("   RocksDB Shards:    %d\n", len(cfg.RocksDBShardURLs))
	fmt.Printf("   S3 Endpoint:       %s\n", cfg.S3Endpoint)
	fmt.Printf("   S3 Bucket:         %s\n", cfg.S3Bucket)

	// Run all test suites
	runner.TestServiceHealth()
	runner.TestKafkaWrite()
	runner.TestWriteService()
	runner.TestIndexBuilder()
	runner.TestRocksDBShards()
	runner.TestS3Storage()
	runner.TestReadServiceQueries()
	runner.TestDataIntegrity()
	runner.TestPerformance()
	runner.TestEndToEndFlow()

	// Print summary
	runner.stats.PrintSummary()

	// Exit with appropriate code
	if runner.stats.FailedTests.Load() > 0 {
		os.Exit(1)
	}
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
