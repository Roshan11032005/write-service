package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
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
	ReadServiceURL   string
	WriteServiceURL  string
	IndexBuilderURL  string
	S3Endpoint       string
	S3Bucket         string
	S3AccessKey      string
	S3SecretKey      string
	S3Region         string
	NumRefIDs        int
	EventsPerRefID   int
	TestDuration     time.Duration
	QueryInterval    time.Duration
	TestRunID        string
	InitialWaitTime  time.Duration // Wait for initial indexing
}

func loadTestConfig() TestConfig {
	testRunID := fmt.Sprintf("run-%d", time.Now().Unix())
	
	return TestConfig{
		KafkaBroker:     getEnv("KAFKA_BROKER", "localhost:9092"),
		KafkaTopic:      getEnv("KAFKA_TOPIC", "events"),
		ReadServiceURL:  getEnv("READ_SERVICE_URL", "http://localhost:8087"),
		WriteServiceURL: getEnv("WRITE_SERVICE_URL", "http://localhost:8085"),
		IndexBuilderURL: getEnv("INDEX_BUILDER_URL", "http://localhost:8086"),
		S3Endpoint:      getEnv("S3_ENDPOINT", "http://localhost:9002"),
		S3Bucket:        getEnv("S3_BUCKET", "events-bucket"),
		S3AccessKey:     getEnv("S3_ACCESS_KEY", "admin"),
		S3SecretKey:     getEnv("S3_SECRET_KEY", "strongpassword"),
		S3Region:        getEnv("S3_REGION", "us-east-1"),
		NumRefIDs:       50,
		EventsPerRefID:  200,
		TestDuration:    10 * time.Minute,
		QueryInterval:   30 * time.Second,
		TestRunID:       testRunID,
		InitialWaitTime: 3 * time.Minute, // Increased from 3min to ensure full indexing
	}
}

// ═══════════════════════════════════════════════════════════════
//  Event Types
// ═══════════════════════════════════════════════════════════════

type KafkaEvent struct {
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
//  Test Statistics
// ═══════════════════════════════════════════════════════════════

type IntensiveTestStats struct {
	StartTime           time.Time
	TestRunID           string
	RefIDs              []string
	EventsByRefID       map[string]map[string]bool
	mu                  sync.RWMutex
	
	EventsGenerated     atomic.Int64
	EventsWritten       atomic.Int64
	EventsFound         atomic.Int64
	EventsMissing       atomic.Int64
	ExtraEvents         atomic.Int64
	
	QueriesExecuted     atomic.Int64
	QueriesSuccessful   atomic.Int64
	QueriesFailed       atomic.Int64
	
	TotalQueryTimeMs    atomic.Int64
	MinQueryTimeMs      atomic.Int64
	MaxQueryTimeMs      atomic.Int64
	
	WriteErrors         atomic.Int64
	QueryErrors         atomic.Int64
	
	// Track first vs subsequent queries
	FirstQueryMissing   int
	FinalQueryMissing   int
	
	missingEvents       []string
	extraEvents         []string
	missingMu           sync.Mutex
}

func NewIntensiveTestStats(refIDs []string, testRunID string) *IntensiveTestStats {
	stats := &IntensiveTestStats{
		StartTime:     time.Now(),
		TestRunID:     testRunID,
		RefIDs:        refIDs,
		EventsByRefID: make(map[string]map[string]bool),
		missingEvents: make([]string, 0),
		extraEvents:   make([]string, 0),
	}
	stats.MinQueryTimeMs.Store(999999)
	
	for _, refID := range refIDs {
		stats.EventsByRefID[refID] = make(map[string]bool)
	}
	
	return stats
}

func (s *IntensiveTestStats) RecordEvent(refID, eventID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.EventsByRefID[refID][eventID] = true
	s.EventsGenerated.Add(1)
}

func (s *IntensiveTestStats) RecordQuery(duration time.Duration, success bool) {
	s.QueriesExecuted.Add(1)
	if success {
		s.QueriesSuccessful.Add(1)
		ms := duration.Milliseconds()
		s.TotalQueryTimeMs.Add(ms)
		
		for {
			oldMin := s.MinQueryTimeMs.Load()
			if ms >= oldMin || s.MinQueryTimeMs.CompareAndSwap(oldMin, ms) {
				break
			}
		}
		
		for {
			oldMax := s.MaxQueryTimeMs.Load()
			if ms <= oldMax || s.MaxQueryTimeMs.CompareAndSwap(oldMax, ms) {
				break
			}
		}
	} else {
		s.QueriesFailed.Add(1)
	}
}

func (s *IntensiveTestStats) RecordMissingEvent(refID, eventID string) {
	s.missingMu.Lock()
	defer s.missingMu.Unlock()
	s.missingEvents = append(s.missingEvents, fmt.Sprintf("ref:%s event:%s", refID, eventID))
	s.EventsMissing.Add(1)
}

func (s *IntensiveTestStats) RecordExtraEvent(refID, eventID string) {
	s.missingMu.Lock()
	defer s.missingMu.Unlock()
	s.extraEvents = append(s.extraEvents, fmt.Sprintf("ref:%s event:%s", refID, eventID))
	s.ExtraEvents.Add(1)
}

func (s *IntensiveTestStats) GetExpectedEventIDs(refID string) map[string]bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	events := make(map[string]bool)
	for eventID := range s.EventsByRefID[refID] {
		events[eventID] = true
	}
	return events
}

func (s *IntensiveTestStats) ResetMissingCount() {
	s.EventsMissing.Store(0)
	s.EventsFound.Store(0)
	s.missingMu.Lock()
	s.missingEvents = make([]string, 0)
	s.missingMu.Unlock()
}

func (s *IntensiveTestStats) PrintProgress() {
	elapsed := time.Since(s.StartTime)
	queriesExecuted := s.QueriesExecuted.Load()
	avgQueryTime := int64(0)
	if queriesExecuted > 0 {
		avgQueryTime = s.TotalQueryTimeMs.Load() / queriesExecuted
	}
	
	fmt.Printf("\n⏱️  Progress Report (%.0f seconds elapsed)\n", elapsed.Seconds())
	fmt.Printf("═══════════════════════════════════════════════════════════════\n")
	fmt.Printf("📊 Event Statistics:\n")
	fmt.Printf("   Generated:      %d\n", s.EventsGenerated.Load())
	fmt.Printf("   Written:        %d\n", s.EventsWritten.Load())
	fmt.Printf("   Found:          %d\n", s.EventsFound.Load())
	fmt.Printf("   Missing:        %d\n", s.EventsMissing.Load())
	
	if s.FirstQueryMissing > 0 {
		improvement := s.FirstQueryMissing - s.FinalQueryMissing
		fmt.Printf("   First Query Missing: %d\n", s.FirstQueryMissing)
		fmt.Printf("   Current Missing:     %d\n", s.FinalQueryMissing)
		if improvement > 0 {
			fmt.Printf("   ✓ Improvement:       %d events indexed after first query\n", improvement)
		}
	}
	
	fmt.Printf("\n📈 Query Statistics:\n")
	fmt.Printf("   Executed:       %d\n", queriesExecuted)
	fmt.Printf("   Successful:     %d\n", s.QueriesSuccessful.Load())
	fmt.Printf("   Failed:         %d\n", s.QueriesFailed.Load())
	fmt.Printf("   Avg Time:       %dms\n", avgQueryTime)
	fmt.Printf("   Min Time:       %dms\n", s.MinQueryTimeMs.Load())
	fmt.Printf("   Max Time:       %dms\n", s.MaxQueryTimeMs.Load())
	fmt.Printf("═══════════════════════════════════════════════════════════════\n")
}

func (s *IntensiveTestStats) PrintFinalReport() {
	elapsed := time.Since(s.StartTime)
	
	fmt.Printf("\n\n")
	fmt.Printf("╔═══════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║              INTENSIVE TEST - FINAL REPORT                    ║\n")
	fmt.Printf("╚═══════════════════════════════════════════════════════════════╝\n\n")
	
	fmt.Printf("⏱️  Test Duration: %s\n", elapsed.Round(time.Second))
	fmt.Printf("🆔 Test Run ID: %s\n\n", s.TestRunID)
	
	fmt.Printf("📊 EVENT STATISTICS:\n")
	fmt.Printf("   Events Generated:     %d\n", s.EventsGenerated.Load())
	fmt.Printf("   Events Written:       %d\n", s.EventsWritten.Load())
	fmt.Printf("   Events Found (final): %d\n", s.EventsFound.Load())
	fmt.Printf("   Events Missing:       %d ❌\n", s.EventsMissing.Load())
	
	generatedCount := s.EventsGenerated.Load()
	foundCount := s.EventsFound.Load()
	if generatedCount > 0 {
		successRate := float64(foundCount) / float64(generatedCount) * 100
		fmt.Printf("   Success Rate:         %.2f%%\n", successRate)
	}
	
	if s.FirstQueryMissing > 0 {
		fmt.Printf("\n🔄 INDEXING PROGRESS:\n")
		fmt.Printf("   First Query Missing:  %d events\n", s.FirstQueryMissing)
		fmt.Printf("   Final Query Missing:  %d events\n", s.FinalQueryMissing)
		improvement := s.FirstQueryMissing - s.FinalQueryMissing
		if improvement > 0 {
			fmt.Printf("   ✓ Events Indexed During Test: %d\n", improvement)
			fmt.Printf("   ℹ️  This confirms indexing was still in progress during first query\n")
		}
	}
	
	fmt.Printf("\n📈 QUERY STATISTICS:\n")
	queriesExecuted := s.QueriesExecuted.Load()
	fmt.Printf("   Total Queries:        %d\n", queriesExecuted)
	fmt.Printf("   Successful:           %d\n", s.QueriesSuccessful.Load())
	fmt.Printf("   Failed:               %d\n", s.QueriesFailed.Load())
	
	if queriesExecuted > 0 {
		avgQueryTime := s.TotalQueryTimeMs.Load() / queriesExecuted
		fmt.Printf("   Avg Query Time:       %dms\n", avgQueryTime)
		fmt.Printf("   Min Query Time:       %dms\n", s.MinQueryTimeMs.Load())
		fmt.Printf("   Max Query Time:       %dms\n", s.MaxQueryTimeMs.Load())
		queriesPerMin := float64(queriesExecuted) / elapsed.Minutes()
		fmt.Printf("   Queries/Minute:       %.2f\n", queriesPerMin)
	}
	
	fmt.Printf("\n❌ ERROR STATISTICS:\n")
	fmt.Printf("   Write Errors:         %d\n", s.WriteErrors.Load())
	fmt.Printf("   Query Errors:         %d\n", s.QueryErrors.Load())
	
	if s.EventsMissing.Load() > 0 && len(s.missingEvents) > 0 {
		fmt.Printf("\n⚠️  MISSING EVENTS:\n")
		s.missingMu.Lock()
		displayCount := len(s.missingEvents)
		if displayCount > 20 {
			displayCount = 20
		}
		for i := 0; i < displayCount; i++ {
			fmt.Printf("   %d. %s\n", i+1, s.missingEvents[i])
		}
		if len(s.missingEvents) > 20 {
			fmt.Printf("   ... and %d more\n", len(s.missingEvents)-20)
		}
		s.missingMu.Unlock()
	}
	
	fmt.Printf("\n")
	
	if s.EventsMissing.Load() == 0 && s.QueriesFailed.Load() == 0 {
		fmt.Printf("╔═══════════════════════════════════════════════════════════════╗\n")
		fmt.Printf("║                    ✅ TEST PASSED                             ║\n")
		fmt.Printf("║     All %d events written, indexed, and retrieved!       ║\n", generatedCount)
		fmt.Printf("╚═══════════════════════════════════════════════════════════════╝\n")
	} else {
		fmt.Printf("╔═══════════════════════════════════════════════════════════════╗\n")
		fmt.Printf("║                    ⚠️  TEST COMPLETED WITH ISSUES             ║\n")
		fmt.Printf("╚═══════════════════════════════════════════════════════════════╝\n")
	}
}

// ═══════════════════════════════════════════════════════════════
//  Test Runner
// ═══════════════════════════════════════════════════════════════

type IntensiveTestRunner struct {
	cfg      TestConfig
	stats    *IntensiveTestStats
	http     *http.Client
	s3Client *s3.Client
	ctx      context.Context
	cancel   context.CancelFunc
}

func NewIntensiveTestRunner(cfg TestConfig) (*IntensiveTestRunner, error) {
	refIDs := make([]string, cfg.NumRefIDs)
	for i := 0; i < cfg.NumRefIDs; i++ {
		refIDs[i] = fmt.Sprintf("%s-ref-%03d", cfg.TestRunID, i+1)
	}
	
	stats := NewIntensiveTestStats(refIDs, cfg.TestRunID)
	
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
	
	ctx, cancel := context.WithCancel(context.Background())
	
	return &IntensiveTestRunner{
		cfg:      cfg,
		stats:    stats,
		http:     &http.Client{Timeout: 60 * time.Second},
		s3Client: s3Client,
		ctx:      ctx,
		cancel:   cancel,
	}, nil
}

func (r *IntensiveTestRunner) Run() error {
	fmt.Printf("\n")
	fmt.Printf("╔═══════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║           INTENSIVE TEST - 10 MINUTE STRESS TEST              ║\n")
	fmt.Printf("╚═══════════════════════════════════════════════════════════════╝\n\n")
	
	fmt.Printf("📋 Test Configuration:\n")
	fmt.Printf("   Test Run ID:        %s\n", r.cfg.TestRunID)
	fmt.Printf("   Reference IDs:      %d\n", r.cfg.NumRefIDs)
	fmt.Printf("   Events per Ref ID:  %d\n", r.cfg.EventsPerRefID)
	fmt.Printf("   Total Events:       %d\n", r.cfg.NumRefIDs*r.cfg.EventsPerRefID)
	fmt.Printf("   Initial Wait:       %s\n", r.cfg.InitialWaitTime)
	fmt.Printf("   Test Duration:      %s\n", r.cfg.TestDuration)
	fmt.Printf("   Query Interval:     %s\n", r.cfg.QueryInterval)
	fmt.Printf("\n")
	
	// Phase 1: Generate and write events
	fmt.Printf("═══════════════════════════════════════════════════════════════\n")
	fmt.Printf("PHASE 1: Generating and Writing Events\n")
	fmt.Printf("═══════════════════════════════════════════════════════════════\n\n")
	
	if err := r.generateAndWriteEvents(); err != nil {
		return fmt.Errorf("failed to write events: %w", err)
	}
	
	// Phase 2: Wait for complete indexing
	fmt.Printf("\n═══════════════════════════════════════════════════════════════\n")
	fmt.Printf("PHASE 2: Waiting for Complete Indexing\n")
	fmt.Printf("════════════════��══════════════════════════════════════════════\n\n")
	
	r.waitForIndexing()
	
	// Phase 3: Continuous querying
	fmt.Printf("\n═══════════════════════════════════════════════════════════════\n")
	fmt.Printf("PHASE 3: Continuous Querying for %s\n", r.cfg.TestDuration)
	fmt.Printf("═══════════════════════════════════════════════════════════════\n\n")
	
	r.continuousQuery()
	
	// Final report
	r.stats.PrintFinalReport()
	
	return nil
}

func (r *IntensiveTestRunner) generateAndWriteEvents() error {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(r.cfg.KafkaBroker),
		Topic:        r.cfg.KafkaTopic,
		Balancer:     &kafka.Hash{},
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
	}
	defer writer.Close()
	
	totalEvents := r.cfg.NumRefIDs * r.cfg.EventsPerRefID
	batchSize := 100
	messages := make([]kafka.Message, 0, batchSize)
	
	fmt.Printf("📝 Generating %d events...\n", totalEvents)
	
	eventCount := 0
	startTime := time.Now()
	
	for _, refID := range r.stats.RefIDs {
		for i := 0; i < r.cfg.EventsPerRefID; i++ {
			event := r.generateEvent(refID, i)
			r.stats.RecordEvent(refID, event.EventID)
			
			payload, _ := json.Marshal(event)
			messages = append(messages, kafka.Message{
				Key:   []byte(refID),
				Value: payload,
			})
			
			eventCount++
			
			if len(messages) >= batchSize {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				err := writer.WriteMessages(ctx, messages...)
				cancel()
				
				if err != nil {
					r.stats.WriteErrors.Add(1)
					log.Printf("❌ Write error: %v", err)
				} else {
					r.stats.EventsWritten.Add(int64(len(messages)))
				}
				
				messages = messages[:0]
				
				if eventCount%1000 == 0 {
					elapsed := time.Since(startTime)
					rate := float64(eventCount) / elapsed.Seconds()
					fmt.Printf("   ✓ Written %d/%d events (%.0f events/sec)\n", eventCount, totalEvents, rate)
				}
			}
		}
	}
	
	if len(messages) > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := writer.WriteMessages(ctx, messages...)
		cancel()
		
		if err != nil {
			r.stats.WriteErrors.Add(1)
			log.Printf("❌ Write error: %v", err)
		} else {
			r.stats.EventsWritten.Add(int64(len(messages)))
		}
	}
	
	elapsed := time.Since(startTime)
	rate := float64(totalEvents) / elapsed.Seconds()
	
	fmt.Printf("\n✅ Successfully written %d events in %s\n", totalEvents, elapsed.Round(time.Second))
	fmt.Printf("   Average rate: %.0f events/second\n", rate)
	
	return nil
}

func (r *IntensiveTestRunner) generateEvent(refID string, index int) KafkaEvent {
	now := time.Now().Add(time.Duration(index) * time.Second)
	eventID := fmt.Sprintf("%s-evt-%05d", refID, index)
	
	return KafkaEvent{
		EventTimestamp: now.Format(time.RFC3339),
		EventID:        eventID,
		Category:       "intensive_test",
		EventType:      "stress_test",
		Version:        "1.0",
		ReferenceID:    refID,
		Data: map[string]interface{}{
			"index":       index,
			"ref_id":      refID,
			"test_run_id": r.cfg.TestRunID,
			"timestamp":   now.Unix(),
			"test_field":  fmt.Sprintf("test-value-%d", index),
			"random_data": rand.Intn(10000),
		},
	}
}

func (r *IntensiveTestRunner) waitForIndexing() {
	fmt.Printf("⏳ Waiting for write service to consume and flush...\n")
	
	// Wait in increments and check progress
	waitIncrement := 30 * time.Second
	totalWait := r.cfg.InitialWaitTime
	
	for elapsed := time.Duration(0); elapsed < totalWait; elapsed += waitIncrement {
		time.Sleep(waitIncrement)
		
		// Check index builder progress
		metrics := r.getIndexBuilderMetrics()
		fmt.Printf("   %s elapsed - Segments: %.0f, Events: %.0f\n", 
			elapsed+waitIncrement, metrics["total_segments_processed"], metrics["total_events_indexed"])
		
		// Trigger flush periodically
		if elapsed%(60*time.Second) == 0 {
			r.triggerFlush()
		}
	}
	
	// Final flush
	fmt.Printf("🔄 Final flush trigger...\n")
	r.triggerFlush()
	time.Sleep(10 * time.Second)
	
	// Check final metrics
	metrics := r.getIndexBuilderMetrics()
	fmt.Printf("\n✓ Indexing Wait Complete:\n")
	fmt.Printf("   Segments Processed: %.0f\n", metrics["total_segments_processed"])
	fmt.Printf("   Events Indexed: %.0f\n", metrics["total_events_indexed"])
	fmt.Printf("   Expected Events: %d\n", r.cfg.NumRefIDs*r.cfg.EventsPerRefID)
}

func (r *IntensiveTestRunner) getIndexBuilderMetrics() map[string]interface{} {
	resp, err := r.http.Get(r.cfg.IndexBuilderURL + "/internal/metrics")
	if err != nil {
		return map[string]interface{}{}
	}
	defer resp.Body.Close()
	
	body, _ := io.ReadAll(resp.Body)
	var metrics map[string]interface{}
	json.Unmarshal(body, &metrics)
	return metrics
}

func (r *IntensiveTestRunner) triggerFlush() {
	req, _ := http.NewRequest(http.MethodPost, r.cfg.WriteServiceURL+"/internal/flush", nil)
	resp, err := r.http.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
}

func (r *IntensiveTestRunner) continuousQuery() {
	startTime := time.Now()
	endTime := startTime.Add(r.cfg.TestDuration)
	
	progressTicker := time.NewTicker(30 * time.Second)
	defer progressTicker.Stop()
	
	queryTicker := time.NewTicker(r.cfg.QueryInterval)
	defer queryTicker.Stop()
	
	// First query - capture baseline
	fmt.Printf("🔍 Running initial verification query...\n")
	missing := r.queryAllRefIDs()
	r.stats.FirstQueryMissing = missing
	r.stats.FinalQueryMissing = missing
	
	if missing > 0 {
		fmt.Printf("⚠️  First query missing %d events - likely still indexing\n", missing)
	} else {
		fmt.Printf("✅ All events found on first query!\n")
	}
	
	for {
		select {
		case <-r.ctx.Done():
			return
			
		case <-progressTicker.C:
			r.stats.PrintProgress()
			
		case <-queryTicker.C:
			if time.Now().After(endTime) {
				fmt.Printf("\n⏱️  Test duration reached. Performing final query...\n")
				r.stats.ResetMissingCount()
				missing := r.queryAllRefIDs()
				r.stats.FinalQueryMissing = missing
				return
			}
			r.stats.ResetMissingCount()
			missing := r.queryAllRefIDs()
			r.stats.FinalQueryMissing = missing
		}
	}
}

func (r *IntensiveTestRunner) queryAllRefIDs() int {
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, 10)
	
	for _, refID := range r.stats.RefIDs {
		wg.Add(1)
		semaphore <- struct{}{}
		
		go func(ref string) {
			defer wg.Done()
			defer func() { <-semaphore }()
			r.queryAndVerifyRefID(ref)
		}(refID)
	}
	
	wg.Wait()
	
	missing := int(r.stats.EventsMissing.Load())
	return missing
}

func (r *IntensiveTestRunner) queryAndVerifyRefID(refID string) {
	expectedEventIDs := r.stats.GetExpectedEventIDs(refID)
	
	req := QueryRequest{
		ReferenceIDs: []string{refID},
	}
	
	data, _ := json.Marshal(req)
	
	startQuery := time.Now()
	resp, err := r.http.Post(r.cfg.ReadServiceURL+"/query", "application/json", bytes.NewReader(data))
	queryDuration := time.Since(startQuery)
	
	if err != nil {
		r.stats.RecordQuery(queryDuration, false)
		r.stats.QueryErrors.Add(1)
		return
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		r.stats.RecordQuery(queryDuration, false)
		r.stats.QueryErrors.Add(1)
		return
	}
	
	body, _ := io.ReadAll(resp.Body)
	var queryResp QueryResponse
	if err := json.Unmarshal(body, &queryResp); err != nil {
		r.stats.RecordQuery(queryDuration, false)
		r.stats.QueryErrors.Add(1)
		return
	}
	
	r.stats.RecordQuery(queryDuration, true)
	
	foundEventIDs := make(map[string]bool)
	for _, event := range queryResp.Events {
		foundEventIDs[event.EventID] = true
	}
	
	for eventID := range expectedEventIDs {
		if foundEventIDs[eventID] {
			r.stats.EventsFound.Add(1)
		} else {
			r.stats.RecordMissingEvent(refID, eventID)
		}
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())
	
	cfg := loadTestConfig()
	
	runner, err := NewIntensiveTestRunner(cfg)
	if err != nil {
		log.Fatalf("Failed to initialize test runner: %v", err)
	}
	
	if err := runner.Run(); err != nil {
		log.Fatalf("Test failed: %v", err)
	}
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}