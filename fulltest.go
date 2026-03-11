package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"net/http"
	"os"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// ─────────────────────────────────────────────
//  Colors for Output
// ─────────────────────────────────────────────

const (
	ColorReset  = "\033[0m"
	ColorRed    = "\033[31m"
	ColorGreen  = "\033[32m"
	ColorYellow = "\033[33m"
	ColorBlue   = "\033[34m"
	ColorPurple = "\033[35m"
	ColorCyan   = "\033[36m"
	ColorWhite  = "\033[37m"
)

// ─────────────────────────────────────────────
//  Test Configuration
// ─────────────────────────────────────────────

type TestConfig struct {
	KafkaBroker       string
	KafkaTopic        string
	WriteServiceURL   string
	IndexBuilderURL   string
	ReadServiceURL    string
	MinIOEndpoint     string
	MinIOAccessKey    string
	MinIOSecretKey    string
	S3Bucket          string
	RocksDBShards     []string
	TestRefIDs        []string
	EventsPerRefID    int
	WaitForPipeline   time.Duration
}

func loadTestConfig() TestConfig {
	shards := []string{
		"http://localhost:4001",
		"http://localhost:4002",
		"http://localhost:4003",
		"http://localhost:4004",
		"http://localhost:4005",
	}

	return TestConfig{
		KafkaBroker:       getEnv("KAFKA_BROKER", "localhost:9092"),
		KafkaTopic:        getEnv("KAFKA_TOPIC", "events"),
		WriteServiceURL:   getEnv("WRITE_SERVICE_URL", "http://localhost:8085"),
		IndexBuilderURL:   getEnv("INDEX_BUILDER_URL", "http://localhost:8086"),
		ReadServiceURL:    getEnv("READ_SERVICE_URL", "http://localhost:8087"),
		MinIOEndpoint:     getEnv("MINIO_ENDPOINT", "http://localhost:9002"),
		MinIOAccessKey:    getEnv("MINIO_ACCESS_KEY", "admin"),
		MinIOSecretKey:    getEnv("MINIO_SECRET_KEY", "strongpassword"),
		S3Bucket:          getEnv("S3_BUCKET", "events-bucket"),
		RocksDBShards:     shards,
		TestRefIDs:        []string{"test-sys-user-001", "test-sys-user-002", "test-sys-user-003"},
		EventsPerRefID:    5,
		WaitForPipeline:   60 * time.Second,
	}
}

// ─────────────────────────────────────────────
//  Types
// ─────────────────────────────────────────────

type KafkaMessage struct {
	EventTimestamp string                 `json:"_event_timestamp"`
	EventID        string                 `json:"_event_id"`
	Category       string                 `json:"_category"`
	EventType      string                 `json:"_event_type"`
	Version        string                 `json:"_version"`
	ReferenceID    string                 `json:"reference_id"`
	Data           map[string]interface{} `json:"_data"`
}

type QueryRequest struct {
	ReferenceIDs []string `json:"reference_ids"`
	StartTime    int64    `json:"start_time,omitempty"`
	EndTime      int64    `json:"end_time,omitempty"`
	Category     string   `json:"category,omitempty"`
	EventType    string   `json:"event_type,omitempty"`
	Limit        int      `json:"limit,omitempty"`
}

type QueryResponse struct {
	Events      []map[string]interface{} `json:"events"`
	TotalEvents int                      `json:"total_events"`
	QueryTimeMs int64                    `json:"query_time_ms"`
	Metrics     QueryMetrics             `json:"metrics"`
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

type ServiceMetrics struct {
	TotalConsumed          int64             `json:"total_consumed"`
	TotalFlushed           int64             `json:"total_flushed"`
	TotalUploads           int64             `json:"total_s3_uploads"`
	TotalSegmentsProcessed int64             `json:"total_segments_processed"`
	TotalEventsIndexed     int64             `json:"total_events_indexed"`
	TotalQueries           int64             `json:"total_queries"`
	TotalEvents            int64             `json:"total_events"`
	Cursor                 map[string]string `json:"cursor"`
}

// ─────────────────────────────────────────────
//  Test Suite
// ─────────────────────────────────────────────

type TestSuite struct {
	cfg         TestConfig
	httpClient  *http.Client
	results     []TestResult
	startTime   time.Time
	totalTests  int
	passedTests int
	failedTests int
}

type TestResult struct {
	Name     string
	Passed   bool
	Duration time.Duration
	Message  string
	Details  map[string]interface{}
}

func NewTestSuite(cfg TestConfig) *TestSuite {
	return &TestSuite{
		cfg:        cfg,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		results:    []TestResult{},
		startTime:  time.Now(),
	}
}

func (ts *TestSuite) printBanner() {
	fmt.Printf("\n%s╔═══════════════════════════════════════════════════════════════════════════╗%s\n", ColorCyan, ColorReset)
	fmt.Printf("%s║                   DISTRIBUTED EVENT SYSTEM TEST SUITE                     ║%s\n", ColorCyan, ColorReset)
	fmt.Printf("%s╚═══════════════════════════════════════════════════════════════════════════╝%s\n\n", ColorCyan, ColorReset)

	fmt.Printf("%sTest Configuration:%s\n", ColorWhite, ColorReset)
	fmt.Printf("  Kafka:         %s\n", ts.cfg.KafkaBroker)
	fmt.Printf("  Write Service: %s\n", ts.cfg.WriteServiceURL)
	fmt.Printf("  Index Builder: %s\n", ts.cfg.IndexBuilderURL)
	fmt.Printf("  Read Service:  %s\n", ts.cfg.ReadServiceURL)
	fmt.Printf("  RocksDB:       %d shards\n", len(ts.cfg.RocksDBShards))
	fmt.Printf("  S3 Bucket:     %s\n", ts.cfg.S3Bucket)
	fmt.Printf("  Test Users:    %d\n", len(ts.cfg.TestRefIDs))
	fmt.Printf("  Events/User:   %d\n\n", ts.cfg.EventsPerRefID)
}

func (ts *TestSuite) addResult(result TestResult) {
	ts.results = append(ts.results, result)
	ts.totalTests++
	if result.Passed {
		ts.passedTests++
		fmt.Printf("  %s✓%s %s (%s)\n", ColorGreen, ColorReset, result.Name, result.Duration)
		if result.Message != "" {
			fmt.Printf("    %s└─ %s%s\n", ColorGreen, result.Message, ColorReset)
		}
	} else {
		ts.failedTests++
		fmt.Printf("  %s✗%s %s\n", ColorRed, ColorReset, result.Name)
		if result.Message != "" {
			fmt.Printf("    %s└─ %s%s\n", ColorRed, result.Message, ColorReset)
		}
	}
}

func (ts *TestSuite) runTest(name string, testFunc func() (bool, string, map[string]interface{})) {
	start := time.Now()
	passed, message, details := testFunc()
	duration := time.Since(start)

	ts.addResult(TestResult{
		Name:     name,
		Passed:   passed,
		Duration: duration,
		Message:  message,
		Details:  details,
	})
}

// ─────────────────────────────────────────────
//  Test 1: Service Health Checks
// ─────────────────────────────────────────────

func (ts *TestSuite) testServiceHealth() {
	fmt.Printf("\n%s════ Phase 1: Service Health Checks ════%s\n\n", ColorYellow, ColorReset)

	services := map[string]string{
		"Kafka (via Write Service)": ts.cfg.WriteServiceURL + "/internal/health",
		"Write Service":              ts.cfg.WriteServiceURL + "/internal/health",
		"Index Builder":              ts.cfg.IndexBuilderURL + "/internal/health",
		"Read Service":               ts.cfg.ReadServiceURL + "/internal/health",
	}

	for name, url := range services {
		ts.runTest(fmt.Sprintf("Health: %s", name), func() (bool, string, map[string]interface{}) {
			resp, err := ts.httpClient.Get(url)
			if err != nil {
				return false, fmt.Sprintf("Connection failed: %v", err), nil
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				return false, fmt.Sprintf("Status: %d", resp.StatusCode), nil
			}

			var health map[string]interface{}
			json.NewDecoder(resp.Body).Decode(&health)

			return true, "Service is healthy", health
		})
	}

	// Test RocksDB shards
	for i, shardURL := range ts.cfg.RocksDBShards {
		ts.runTest(fmt.Sprintf("Health: RocksDB Shard %d", i), func() (bool, string, map[string]interface{}) {
			resp, err := ts.httpClient.Get(shardURL + "/health")
			if err != nil {
				return false, fmt.Sprintf("Connection failed: %v", err), nil
			}
			defer resp.Body.Close()

			return resp.StatusCode == http.StatusOK, "Shard is reachable", nil
		})
	}
}

// ─────────────────────────────────────────────
//  Test 2: Data Ingestion
// ─────────────────────────────────────────────

func (ts *TestSuite) testDataIngestion() {
	fmt.Printf("\n%s════ Phase 2: Data Ingestion ════%s\n\n", ColorYellow, ColorReset)

	ts.runTest("Seed Events to Kafka", func() (bool, string, map[string]interface{}) {
		writer := &kafka.Writer{
			Addr:     kafka.TCP(ts.cfg.KafkaBroker),
			Topic:    ts.cfg.KafkaTopic,
			Balancer: &kafka.Hash{},
		}
		defer writer.Close()

		var messages []kafka.Message
		totalEvents := 0

		for _, refID := range ts.cfg.TestRefIDs {
			for i := 0; i < ts.cfg.EventsPerRefID; i++ {
				event := KafkaMessage{
					EventTimestamp: time.Now().Format(time.RFC3339),
					EventID:        fmt.Sprintf("test-evt-%s-%d", refID, i),
					Category:       "Auth_tran",
					EventType:      "auth_tran",
					Version:        "1.0",
					ReferenceID:    refID,
					Data: map[string]interface{}{
						"aua":               "TEST000000",
						"authResult":        "y",
						"authType":          "F",
						"fingerMatchScore":  95.5,
						"residentStateName": "Test State",
						"test_number":       i,
					},
				}

				payload, _ := json.Marshal(event)
				messages = append(messages, kafka.Message{
					Key:   []byte(refID),
					Value: payload,
				})
				totalEvents++
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		err := writer.WriteMessages(ctx, messages...)
		if err != nil {
			return false, fmt.Sprintf("Kafka write failed: %v", err), nil
		}

		return true, fmt.Sprintf("Seeded %d events", totalEvents), map[string]interface{}{
			"total_events": totalEvents,
			"ref_ids":      len(ts.cfg.TestRefIDs),
		}
	})
}

// ──────────────────────────────────��──────────
//  Test 3: Pipeline Processing
// ─────────────────────────────────────────────

func (ts *TestSuite) testPipelineProcessing() {
	fmt.Printf("\n%s════ Phase 3: Pipeline Processing ════%s\n\n", ColorYellow, ColorReset)

	// Force flush write service
	ts.runTest("Trigger Write Service Flush", func() (bool, string, map[string]interface{}) {
		time.Sleep(5 * time.Second) // Wait for consumption

		resp, err := ts.httpClient.Post(ts.cfg.WriteServiceURL+"/internal/flush", "application/json", nil)
		if err != nil {
			return false, fmt.Sprintf("Flush failed: %v", err), nil
		}
		defer resp.Body.Close()

		return resp.StatusCode == http.StatusOK, "Flush triggered", nil
	})

	// Wait for pipeline
	fmt.Printf("\n  %sWaiting %s for pipeline to process...%s\n", ColorBlue, ts.cfg.WaitForPipeline, ColorReset)
	
	// Monitor progress
	startWait := time.Now()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			elapsed := time.Since(startWait)
			if elapsed >= ts.cfg.WaitForPipeline {
				goto DoneWaiting
			}

			// Check progress
			writeMetrics := ts.getWriteServiceMetrics()
			indexMetrics := ts.getIndexBuilderMetrics()

			fmt.Printf("  %s[%.0fs] Write: %d consumed, %d flushed | Index: %d segments, %d events%s\n",
				ColorBlue, elapsed.Seconds(),
				writeMetrics.TotalConsumed, writeMetrics.TotalFlushed,
				indexMetrics.TotalSegmentsProcessed, indexMetrics.TotalEventsIndexed,
				ColorReset)
		}
	}

DoneWaiting:
	fmt.Println()

	// Verify write service
	ts.runTest("Write Service Processed Events", func() (bool, string, map[string]interface{}) {
		metrics := ts.getWriteServiceMetrics()

		if metrics.TotalConsumed == 0 {
			return false, "No events consumed", map[string]interface{}{"metrics": metrics}
		}

		if metrics.TotalFlushed == 0 {
			return false, "No events flushed", map[string]interface{}{"metrics": metrics}
		}

		return true, fmt.Sprintf("Consumed: %d, Flushed: %d, Uploads: %d",
			metrics.TotalConsumed, metrics.TotalFlushed, metrics.TotalUploads), map[string]interface{}{"metrics": metrics}
	})

	// Verify index builder
	ts.runTest("Index Builder Indexed Events", func() (bool, string, map[string]interface{}) {
		metrics := ts.getIndexBuilderMetrics()

		if metrics.TotalSegmentsProcessed == 0 {
			return false, "No segments processed", map[string]interface{}{"metrics": metrics}
		}

		if metrics.TotalEventsIndexed == 0 {
			return false, "No events indexed", map[string]interface{}{"metrics": metrics}
		}

		return true, fmt.Sprintf("Segments: %d, Events: %d",
			metrics.TotalSegmentsProcessed, metrics.TotalEventsIndexed), map[string]interface{}{"metrics": metrics}
	})
}

// ─────────────────────────────────────────────
//  Test 4: Query Functionality
// ─────────────────────────────────────────────

func (ts *TestSuite) testQueryFunctionality() {
	fmt.Printf("\n%s════ Phase 4: Query Functionality ════%s\n\n", ColorYellow, ColorReset)

	// Test single reference ID
	ts.runTest("Query Single Reference ID", func() (bool, string, map[string]interface{}) {
		resp, err := ts.query(QueryRequest{
			ReferenceIDs: []string{ts.cfg.TestRefIDs[0]},
		})

		if err != nil {
			return false, fmt.Sprintf("Query failed: %v", err), nil
		}

		if resp.TotalEvents == 0 {
			return false, "No events found", map[string]interface{}{"response": resp}
		}

		return true, fmt.Sprintf("Found %d events in %dms (RocksDB: %dms, S3: %dms)",
			resp.TotalEvents, resp.QueryTimeMs, resp.Metrics.RocksDBQueryTimeMs, resp.Metrics.S3FetchTimeMs),
			map[string]interface{}{"response": resp}
	})

	// Test multiple reference IDs
	ts.runTest("Query Multiple Reference IDs", func() (bool, string, map[string]interface{}) {
		resp, err := ts.query(QueryRequest{
			ReferenceIDs: ts.cfg.TestRefIDs,
		})

		if err != nil {
			return false, fmt.Sprintf("Query failed: %v", err), nil
		}

		expectedMin := len(ts.cfg.TestRefIDs) * ts.cfg.EventsPerRefID / 2 // At least 50%

		if resp.TotalEvents < expectedMin {
			return false, fmt.Sprintf("Expected at least %d events, got %d", expectedMin, resp.TotalEvents),
				map[string]interface{}{"response": resp}
		}

		return true, fmt.Sprintf("Found %d events for %d users in %dms (S3 fetches: %d)",
			resp.TotalEvents, len(ts.cfg.TestRefIDs), resp.QueryTimeMs, resp.Metrics.S3FetchesCount),
			map[string]interface{}{"response": resp}
	})

	// Test time range filter
	ts.runTest("Query with Time Range", func() (bool, string, map[string]interface{}) {
		endTime := time.Now().Unix()
		startTime := endTime - 3600 // Last hour

		resp, err := ts.query(QueryRequest{
			ReferenceIDs: []string{ts.cfg.TestRefIDs[0]},
			StartTime:    startTime,
			EndTime:      endTime,
		})

		if err != nil {
			return false, fmt.Sprintf("Query failed: %v", err), nil
		}

		return true, fmt.Sprintf("Found %d events in time range", resp.TotalEvents),
			map[string]interface{}{"response": resp}
	})

	// Test category filter
	ts.runTest("Query with Category Filter", func() (bool, string, map[string]interface{}) {
		resp, err := ts.query(QueryRequest{
			ReferenceIDs: []string{ts.cfg.TestRefIDs[0]},
			Category:     "Auth_tran",
		})

		if err != nil {
			return false, fmt.Sprintf("Query failed: %v", err), nil
		}

		// Verify all events match category
		for _, event := range resp.Events {
			if category, ok := event["category"].(string); ok && category != "Auth_tran" {
				return false, "Category filter not working", nil
			}
		}

		return true, fmt.Sprintf("Category filter working (%d events)", resp.TotalEvents),
			map[string]interface{}{"response": resp}
	})

	// Test limit parameter
	ts.runTest("Query with Limit", func() (bool, string, map[string]interface{}) {
		resp, err := ts.query(QueryRequest{
			ReferenceIDs: ts.cfg.TestRefIDs,
			Limit:        3,
		})

		if err != nil {
			return false, fmt.Sprintf("Query failed: %v", err), nil
		}

		if len(resp.Events) > 3 {
			return false, fmt.Sprintf("Limit not respected: got %d events", len(resp.Events)), nil
		}

		return true, fmt.Sprintf("Limit working (returned %d events)", len(resp.Events)),
			map[string]interface{}{"response": resp}
	})
}

// ─────────────────────────────────────────────
//  Test 5: Performance & Stress
// ─────────────────────────────────────────────

func (ts *TestSuite) testPerformance() {
	fmt.Printf("\n%s════ Phase 5: Performance Tests ════%s\n\n", ColorYellow, ColorReset)

	// Concurrent queries
	ts.runTest("Concurrent Queries (10 parallel)", func() (bool, string, map[string]interface{}) {
		var wg sync.WaitGroup
		results := make(chan *QueryResponse, 10)
		errors := make(chan error, 10)

		start := time.Now()

		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				resp, err := ts.query(QueryRequest{
					ReferenceIDs: []string{ts.cfg.TestRefIDs[0]},
				})
				if err != nil {
					errors <- err
					return
				}
				results <- resp
			}()
		}

		wg.Wait()
		close(results)
		close(errors)

		duration := time.Since(start)

		errorCount := len(errors)
		successCount := len(results)

		if errorCount > 0 {
			return false, fmt.Sprintf("%d/%d queries failed", errorCount, 10), nil
		}

		avgQueryTime := int64(0)
		for resp := range results {
			avgQueryTime += resp.QueryTimeMs
		}
		avgQueryTime /= int64(successCount)

		return true, fmt.Sprintf("10 concurrent queries in %s (avg: %dms/query)",
			duration, avgQueryTime), map[string]interface{}{
			"total_duration": duration.Milliseconds(),
			"avg_query_time": avgQueryTime,
		}
	})

	// Query throughput
	ts.runTest("Query Throughput (100 sequential)", func() (bool, string, map[string]interface{}) {
		start := time.Now()
		successCount := 0

		for i := 0; i < 100; i++ {
			_, err := ts.query(QueryRequest{
				ReferenceIDs: []string{ts.cfg.TestRefIDs[i%len(ts.cfg.TestRefIDs)]},
			})
			if err == nil {
				successCount++
			}
		}

		duration := time.Since(start)
		qps := float64(successCount) / duration.Seconds()

		if successCount < 95 {
			return false, fmt.Sprintf("Only %d/100 queries succeeded", successCount), nil
		}

		return true, fmt.Sprintf("%.2f queries/sec (%d successful)", qps, successCount),
			map[string]interface{}{
				"qps":            qps,
				"success_count":  successCount,
				"total_duration": duration.Milliseconds(),
			}
	})
}

// ──────────��──────────────────────────────────
//  Helper Methods
// ─────────────────────────────────────────────

func (ts *TestSuite) query(req QueryRequest) (*QueryResponse, error) {
	payload, _ := json.Marshal(req)
	resp, err := ts.httpClient.Post(ts.cfg.ReadServiceURL+"/query", "application/json", bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, string(body))
	}

	var queryResp QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&queryResp); err != nil {
		return nil, err
	}

	return &queryResp, nil
}

func (ts *TestSuite) getWriteServiceMetrics() ServiceMetrics {
	var metrics ServiceMetrics
	resp, err := ts.httpClient.Get(ts.cfg.WriteServiceURL + "/internal/metrics")
	if err == nil {
		defer resp.Body.Close()
		json.NewDecoder(resp.Body).Decode(&metrics)
	}
	return metrics
}

func (ts *TestSuite) getIndexBuilderMetrics() ServiceMetrics {
	var metrics ServiceMetrics
	resp, err := ts.httpClient.Get(ts.cfg.IndexBuilderURL + "/internal/metrics")
	if err == nil {
		defer resp.Body.Close()
		json.NewDecoder(resp.Body).Decode(&metrics)
	}
	return metrics
}

func (ts *TestSuite) getReadServiceMetrics() ServiceMetrics {
	var metrics ServiceMetrics
	resp, err := ts.httpClient.Get(ts.cfg.ReadServiceURL + "/internal/metrics")
	if err == nil {
		defer resp.Body.Close()
		json.NewDecoder(resp.Body).Decode(&metrics)
	}
	return metrics
}

// ─────────────────────────────────────────────
//  Final Report
// ─────────────────────────────────────────────

func (ts *TestSuite) printReport() {
	duration := time.Since(ts.startTime)

	fmt.Printf("\n\n%s╔═══════════════════════════════════════════════════════════════════════════╗%s\n", ColorCyan, ColorReset)
	fmt.Printf("%s║                              TEST SUMMARY                                 ║%s\n", ColorCyan, ColorReset)
	fmt.Printf("%s╚═══════════════════════════════════════════════════════════════════════════╝%s\n\n", ColorCyan, ColorReset)

	fmt.Printf("Total Tests:    %s%d%s\n", ColorWhite, ts.totalTests, ColorReset)
	fmt.Printf("Passed:         %s%d%s\n", ColorGreen, ts.passedTests, ColorReset)
	fmt.Printf("Failed:         %s%d%s\n", ColorRed, ts.failedTests, ColorReset)
	fmt.Printf("Success Rate:   %s%.1f%%%s\n", ColorWhite, float64(ts.passedTests)/float64(ts.totalTests)*100, ColorReset)
	fmt.Printf("Total Duration: %s%s%s\n\n", ColorWhite, duration, ColorReset)

	// System metrics
	writeMetrics := ts.getWriteServiceMetrics()
	indexMetrics := ts.getIndexBuilderMetrics()
	readMetrics := ts.getReadServiceMetrics()

	fmt.Printf("%s═══ System Metrics ═══%s\n\n", ColorYellow, ColorReset)
	fmt.Printf("Write Service:\n")
	fmt.Printf("  Consumed: %d events\n", writeMetrics.TotalConsumed)
	fmt.Printf("  Flushed:  %d events\n", writeMetrics.TotalFlushed)
	fmt.Printf("  Uploads:  %d files\n\n", writeMetrics.TotalUploads)

	fmt.Printf("Index Builder:\n")
	fmt.Printf("  Segments: %d processed\n", indexMetrics.TotalSegmentsProcessed)
	fmt.Printf("  Events:   %d indexed\n\n", indexMetrics.TotalEventsIndexed)

	fmt.Printf("Read Service:\n")
	fmt.Printf("  Queries:  %d total\n", readMetrics.TotalQueries)
	fmt.Printf("  Events:   %d returned\n\n", readMetrics.TotalEvents)

	// Final verdict
	if ts.failedTests == 0 {
		fmt.Printf("%s╔═══════════════════════════════════════════════════════════════════════════╗%s\n", ColorGreen, ColorReset)
		fmt.Printf("%s║                      ✓ ALL TESTS PASSED!                                  ║%s\n", ColorGreen, ColorReset)
		fmt.Printf("%s║            Your distributed event system is working perfectly!           ║%s\n", ColorGreen, ColorReset)
		fmt.Printf("%s╚═══════════════════════════════════════════════════════════════════════════╝%s\n\n", ColorGreen, ColorReset)
	} else {
		fmt.Printf("%s╔═══════════════════════════════════════════════════════════════════════════╗%s\n", ColorRed, ColorReset)
		fmt.Printf("%s║                      ✗ SOME TESTS FAILED                                  ║%s\n", ColorRed, ColorReset)
		fmt.Printf("%s║                  Please check the errors above                            ║%s\n", ColorRed, ColorReset)
		fmt.Printf("%s╚═══════════════════════════════════════════════════════════════════════════╝%s\n\n", ColorRed, ColorReset)

		fmt.Printf("%sFailed Tests:%s\n", ColorRed, ColorReset)
		for _, result := range ts.results {
			if !result.Passed {
				fmt.Printf("  • %s: %s\n", result.Name, result.Message)
			}
		}
		fmt.Println()
	}
}

// ─────────────────────────────────────────────
//  Main
// ─────────────────────────────────────────────

func main() {
	cfg := loadTestConfig()
	ts := NewTestSuite(cfg)

	ts.printBanner()

	// Run all test phases
	ts.testServiceHealth()
	ts.testDataIngestion()
	ts.testPipelineProcessing()
	ts.testQueryFunctionality()
	ts.testPerformance()

	// Print final report
	ts.printReport()

	// Exit with appropriate code
	if ts.failedTests > 0 {
		os.Exit(1)
	}
	os.Exit(0)
}

// ─────────────────────────────────────────────
//  Utilities
// ─────────────────────────────────────────────

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}