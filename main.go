package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"
)

// ─────────────────────────────────────────────
//  Config
// ─────────────────────────────────────────────

type Config struct {
	InstanceID            string
	KafkaBroker           string
	KafkaTopic            string
	KafkaGroupID          string
	BufferLimit           int
	BufferSizeLimit       int64
	FlushInterval         time.Duration
	WALDirectory          string
	WALWriteInterval      time.Duration
	S3Endpoint            string
	S3Bucket              string
	S3AccessKey           string
	S3SecretKey           string
	S3Region              string
	HTTPPort              string
	NumConsumers          int
	KafkaBatchSize        int
	KafkaCommitBatch      int
	EnableCompression     bool
	EnableDeduplication   bool
	DeduplicationWindow   time.Duration
	OTELEndpoint          string
	OTELServiceName       string
	OTELServiceVersion    string
}

func loadConfig() Config {
	instanceID := os.Getenv("INSTANCE_ID")
	if instanceID == "" {
		instanceID = generateOrLoadInstanceID()
	}

	return Config{
		InstanceID:          instanceID,
		KafkaBroker:         getEnv("KAFKA_BROKER", "localhost:9092"),
		KafkaTopic:          getEnv("KAFKA_TOPIC", "events"),
		KafkaGroupID:        getEnv("KAFKA_GROUP_ID", "write-service-group"),
		BufferLimit:         getEnvInt("BUFFER_LIMIT", 10000),
		BufferSizeLimit:     getEnvInt64("BUFFER_SIZE_LIMIT", 20*1024*1024),
		FlushInterval:       getEnvDuration("FLUSH_INTERVAL", 10*time.Second),
		WALDirectory:        getEnv("WAL_DIR", fmt.Sprintf("./wal/%s", instanceID)),
		WALWriteInterval:    getEnvDuration("WAL_WRITE_INTERVAL", 500*time.Millisecond),
		S3Endpoint:          getEnv("S3_ENDPOINT", "http://localhost:9002"),
		S3Bucket:            getEnv("S3_BUCKET", "events-bucket"),
		S3AccessKey:         getEnv("S3_ACCESS_KEY", "admin"),
		S3SecretKey:         getEnv("S3_SECRET_KEY", "strongpassword"),
		S3Region:            getEnv("S3_REGION", "us-east-1"),
		HTTPPort:            getEnv("HTTP_PORT", "8085"),
		NumConsumers:        getEnvInt("NUM_CONSUMERS", 3),
		KafkaBatchSize:      getEnvInt("KAFKA_BATCH_SIZE", 100),
		KafkaCommitBatch:    getEnvInt("KAFKA_COMMIT_BATCH", 500),
		EnableCompression:   getEnvBool("ENABLE_COMPRESSION", false),
		EnableDeduplication: getEnvBool("ENABLE_DEDUPLICATION", true),
		DeduplicationWindow: getEnvDuration("DEDUPLICATION_WINDOW", 5*time.Minute),
		OTELEndpoint:        getEnv("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4318"),
		OTELServiceName:     getEnv("OTEL_SERVICE_NAME", "write-service"),
		OTELServiceVersion:  getEnv("OTEL_SERVICE_VERSION", "1.0.0"),
	}
}

func generateOrLoadInstanceID() string {
	idFile := "./instance_id"
	if data, err := os.ReadFile(idFile); err == nil {
		return string(data)
	}
	newID := uuid.New().String()[:8]
	os.WriteFile(idFile, []byte(newID), 0644)
	return newID
}

// ─────────────────────────────────────────────
//  OpenTelemetry Setup
// ─────────────────────────────────────────────

type OTELMetrics struct {
	eventsConsumed          metric.Int64Counter
	eventsFlushed           metric.Int64Counter
	duplicatesFiltered      metric.Int64Counter
	dlqEvents               metric.Int64Counter
	s3Uploads               metric.Int64Counter
	failedFlushes           metric.Int64Counter
	kafkaFetchDuration      metric.Float64Histogram
	eventParseDuration      metric.Float64Histogram
	walWriteDuration        metric.Float64Histogram
	flushDuration           metric.Float64Histogram
	s3UploadDuration        metric.Float64Histogram
	eventProcessingDuration metric.Float64Histogram
}

func initOpenTelemetry(cfg Config) (func(context.Context) error, *OTELMetrics, error) {
	ctx := context.Background()

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(cfg.OTELServiceName),
			semconv.ServiceVersion(cfg.OTELServiceVersion),
			semconv.ServiceInstanceID(cfg.InstanceID),
		),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create resource: %w", err)
	}

	traceExporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint(cfg.OTELEndpoint),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create trace exporter: %w", err)
	}

	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tracerProvider)

	metricExporter, err := otlpmetrichttp.New(ctx,
		otlpmetrichttp.WithEndpoint(cfg.OTELEndpoint),
		otlpmetrichttp.WithInsecure(),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create metric exporter: %w", err)
	}

	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter,
			sdkmetric.WithInterval(10*time.Second))),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(meterProvider)

	meter := meterProvider.Meter(cfg.OTELServiceName)
	metrics := &OTELMetrics{}

	metrics.eventsConsumed, _ = meter.Int64Counter("events.consumed.total")
	metrics.eventsFlushed, _ = meter.Int64Counter("events.flushed.total")
	metrics.duplicatesFiltered, _ = meter.Int64Counter("events.duplicates.filtered.total")
	metrics.dlqEvents, _ = meter.Int64Counter("events.dlq.total")
	metrics.s3Uploads, _ = meter.Int64Counter("s3.uploads.total")
	metrics.failedFlushes, _ = meter.Int64Counter("flush.failures.total")

	metrics.kafkaFetchDuration, _ = meter.Float64Histogram("kafka.fetch.duration", metric.WithUnit("s"))
	metrics.eventParseDuration, _ = meter.Float64Histogram("event.parse.duration", metric.WithUnit("s"))
	metrics.walWriteDuration, _ = meter.Float64Histogram("wal.write.duration", metric.WithUnit("s"))
	metrics.flushDuration, _ = meter.Float64Histogram("flush.duration", metric.WithUnit("s"))
	metrics.s3UploadDuration, _ = meter.Float64Histogram("s3.upload.duration", metric.WithUnit("s"))
	metrics.eventProcessingDuration, _ = meter.Float64Histogram("event.processing.duration", metric.WithUnit("s"))

	shutdown := func(ctx context.Context) error {
		if err := tracerProvider.Shutdown(ctx); err != nil {
			return fmt.Errorf("tracer provider shutdown: %w", err)
		}
		if err := meterProvider.Shutdown(ctx); err != nil {
			return fmt.Errorf("meter provider shutdown: %w", err)
		}
		return nil
	}

	log.Printf("[%s] OpenTelemetry initialized — endpoint: %s", cfg.InstanceID, cfg.OTELEndpoint)
	return shutdown, metrics, nil
}

// ─────────────────────────────────────────────
//  Event
// ─────────────────────────────────────────────

type KafkaEnvelope struct {
	EventID     string `json:"_event_id"`
	EventTS     string `json:"_event_timestamp"`
	Category    string `json:"_category"`
	EventType   string `json:"_event_type"`
	ReferenceID string `json:"reference_id"`
}

type Event struct {
	EventID     string `json:"event_id"`
	ReferenceID string `json:"reference_id"`
	Timestamp   int64  `json:"timestamp"`
	Category    string `json:"category"`
	EventType   string `json:"event_type"`
	Payload     []byte `json:"payload"`
}

// ─────────────────────────────────────────────
//  Deduplication Cache
// ─────────────────────────────────────────────

type DeduplicationCache struct {
	mu      sync.RWMutex
	cache   map[string]int64
	window  time.Duration
	enabled bool
	tracer  trace.Tracer
	metrics *OTELMetrics
}

func NewDeduplicationCache(enabled bool, window time.Duration, tracer trace.Tracer, metrics *OTELMetrics) *DeduplicationCache {
	dc := &DeduplicationCache{
		cache:   make(map[string]int64),
		window:  window,
		enabled: enabled,
		tracer:  tracer,
		metrics: metrics,
	}
	if enabled {
		go dc.cleanupLoop()
	}
	return dc
}

func (dc *DeduplicationCache) IsDuplicate(ctx context.Context, eventID string, timestamp int64) bool {
	ctx, span := dc.tracer.Start(ctx, "deduplication.check",
		trace.WithAttributes(attribute.String("event.id", eventID)))
	defer span.End()

	if !dc.enabled {
		span.SetAttributes(attribute.Bool("deduplication.enabled", false))
		return false
	}

	dc.mu.RLock()
	_, exists := dc.cache[eventID]
	dc.mu.RUnlock()

	span.SetAttributes(
		attribute.Bool("deduplication.duplicate", exists),
		attribute.Int("deduplication.cache_size", len(dc.cache)),
	)

	if exists {
		return true
	}

	dc.mu.Lock()
	dc.cache[eventID] = timestamp
	dc.mu.Unlock()

	return false
}

func (dc *DeduplicationCache) cleanupLoop() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		dc.cleanup()
	}
}

func (dc *DeduplicationCache) cleanup() {
	now := time.Now().Unix()
	windowSec := int64(dc.window.Seconds())

	dc.mu.Lock()
	defer dc.mu.Unlock()

	for eventID, ts := range dc.cache {
		if now-ts > windowSec {
			delete(dc.cache, eventID)
		}
	}
}

func (dc *DeduplicationCache) Size() int {
	dc.mu.RLock()
	defer dc.mu.RUnlock()
	return len(dc.cache)
}

// ─────────────────────────────────────────────
//  Service
// ─────────────────────────────────────────────

type WriteService struct {
	cfg                Config
	s3Client           *s3.Client
	mu                 sync.RWMutex
	buffer             []Event
	bufferSize         int64
	lastFlush          time.Time
	walSeq             int64
	walDirty           atomic.Bool
	walInFlight        atomic.Bool
	reader             *kafka.Reader
	dedupCache         *DeduplicationCache
	flushInProgress    atomic.Bool
	flushMutex         sync.Mutex
	totalConsumed      atomic.Int64
	totalFlushed       atomic.Int64
	totalS3Uploads     atomic.Int64
	failedFlushes      atomic.Int64
	dlqCount           atomic.Int64
	duplicatesFiltered atomic.Int64
	lastSecondCount    atomic.Int64
	currentEPS         atomic.Int64
	totalBytesBuffered atomic.Int64
	ctx                context.Context
	cancel             context.CancelFunc
	wg                 sync.WaitGroup
	bufferPool         sync.Pool
	tracer             trace.Tracer
	metrics            *OTELMetrics
	shutdownOTEL       func(context.Context) error
}

func NewWriteService(cfg Config) (*WriteService, error) {
	ctx, cancel := context.WithCancel(context.Background())

	shutdownOTEL, metrics, err := initOpenTelemetry(cfg)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to initialize OpenTelemetry: %w", err)
	}

	tracer := otel.Tracer(cfg.OTELServiceName)

	svc := &WriteService{
		cfg:          cfg,
		lastFlush:    time.Now(),
		ctx:          ctx,
		cancel:       cancel,
		tracer:       tracer,
		metrics:      metrics,
		shutdownOTEL: shutdownOTEL,
		bufferPool: sync.Pool{
			New: func() interface{} {
				return bytes.NewBuffer(make([]byte, 0, 65536))
			},
		},
	}

	svc.dedupCache = NewDeduplicationCache(cfg.EnableDeduplication, cfg.DeduplicationWindow, tracer, metrics)

	meter := otel.GetMeterProvider().Meter(cfg.OTELServiceName)

	meter.Int64ObservableGauge("buffer.events",
		metric.WithInt64Callback(func(ctx context.Context, observer metric.Int64Observer) error {
			svc.mu.RLock()
			defer svc.mu.RUnlock()
			observer.Observe(int64(len(svc.buffer)))
			return nil
		}))

	meter.Int64ObservableGauge("buffer.bytes",
		metric.WithInt64Callback(func(ctx context.Context, observer metric.Int64Observer) error {
			svc.mu.RLock()
			defer svc.mu.RUnlock()
			observer.Observe(svc.bufferSize)
			return nil
		}))

	meter.Int64ObservableGauge("deduplication.cache.size",
		metric.WithInt64Callback(func(ctx context.Context, observer metric.Int64Observer) error {
			observer.Observe(int64(svc.dedupCache.Size()))
			return nil
		}))

	return svc, nil
}

func (s *WriteService) Start() error {
	ctx, span := s.tracer.Start(s.ctx, "service.start")
	defer span.End()

	log.Printf("[%s] Write service starting", s.cfg.InstanceID)
	log.Printf("[%s] Configuration: Consumers=%d, BufferLimit=%d events, BufferSizeLimit=%d MB, FlushInterval=%s",
		s.cfg.InstanceID, s.cfg.NumConsumers, s.cfg.BufferLimit,
		s.cfg.BufferSizeLimit/(1024*1024), s.cfg.FlushInterval)

	if err := s.initWALDirectory(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "WAL init failed")
		return fmt.Errorf("WAL init failed: %w", err)
	}

	if err := s.initS3(ctx); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "S3 init failed")
		return fmt.Errorf("S3 init failed: %w", err)
	}

	if err := s.recoverFromWAL(ctx); err != nil {
		log.Printf("[%s] WAL recovery error (non-fatal): %v", s.cfg.InstanceID, err)
		span.AddEvent("wal.recovery.error", trace.WithAttributes(attribute.String("error", err.Error())))
	}

	s.initKafkaReader()

	s.wg.Add(s.cfg.NumConsumers + 3)

	for i := 0; i < s.cfg.NumConsumers; i++ {
		consumerID := i
		go s.kafkaConsumerLoop(consumerID)
	}

	go s.walWriterLoop()
	go s.flushSchedulerLoop()
	go s.metricsLoop()
	go s.startHTTPServer()

	span.SetStatus(codes.Ok, "Service started successfully")
	log.Printf("[%s] Write service started", s.cfg.InstanceID)
	return nil
}

func (s *WriteService) Stop() {
	log.Printf("[%s] Shutting down gracefully...", s.cfg.InstanceID)

	_, span := s.tracer.Start(context.Background(), "service.shutdown")
	defer span.End()

	s.cancel()
	s.reader.Close()

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		span.AddEvent("graceful.shutdown.complete")
	case <-time.After(30 * time.Second):
		log.Printf("[%s] Shutdown timeout reached", s.cfg.InstanceID)
		span.AddEvent("graceful.shutdown.timeout")
	}

	log.Printf("[%s] Final flush on shutdown...", s.cfg.InstanceID)
	s.flushToS3()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.shutdownOTEL(shutdownCtx); err != nil {
		log.Printf("[%s] OTEL shutdown error: %v", s.cfg.InstanceID, err)
	}

	span.SetStatus(codes.Ok, "Shutdown complete")
	log.Printf("[%s] Shutdown complete", s.cfg.InstanceID)
}

func (s *WriteService) initS3(ctx context.Context) error {
	_, span := s.tracer.Start(ctx, "s3.init")
	defer span.End()

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(s.cfg.S3Region),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				s.cfg.S3AccessKey, s.cfg.S3SecretKey, "",
			),
		),
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to load AWS config")
		return err
	}

	s.s3Client = s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.EndpointResolver = s3.EndpointResolverFunc(
			func(region string, options s3.EndpointResolverOptions) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL:               s.cfg.S3Endpoint,
					HostnameImmutable: true,
					SigningRegion:     s.cfg.S3Region,
				}, nil
			},
		)
		o.UsePathStyle = true
	})

	_, err = s.s3Client.HeadBucket(context.Background(), &s3.HeadBucketInput{
		Bucket: aws.String(s.cfg.S3Bucket),
	})
	if err != nil {
		log.Printf("[%s] Bucket not found, creating...", s.cfg.InstanceID)
		span.AddEvent("s3.bucket.creating")
		_, err = s.s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
			Bucket: aws.String(s.cfg.S3Bucket),
		})
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "Failed to create bucket")
			return fmt.Errorf("create bucket: %w", err)
		}
	}

	span.SetAttributes(
		attribute.String("s3.bucket", s.cfg.S3Bucket),
		attribute.String("s3.endpoint", s.cfg.S3Endpoint),
	)
	span.SetStatus(codes.Ok, "S3 initialized")

	log.Printf("[%s] S3 ready — bucket: %s endpoint: %s",
		s.cfg.InstanceID, s.cfg.S3Bucket, s.cfg.S3Endpoint)
	return nil
}

func (s *WriteService) initWALDirectory() error {
	if err := os.MkdirAll(s.cfg.WALDirectory, 0755); err != nil {
		return err
	}
	log.Printf("[%s] WAL directory: %s", s.cfg.InstanceID, s.cfg.WALDirectory)
	return nil
}

func (s *WriteService) walPath(seq int64) string {
	return filepath.Join(s.cfg.WALDirectory, fmt.Sprintf("wal-%d.json", seq))
}

func (s *WriteService) recoverFromWAL(ctx context.Context) error {
	_, span := s.tracer.Start(ctx, "wal.recovery")
	defer span.End()

	files, err := filepath.Glob(filepath.Join(s.cfg.WALDirectory, "wal-*.json"))
	if err != nil {
		span.RecordError(err)
		return err
	}
	if len(files) == 0 {
		log.Printf("[%s] No WAL files found, starting fresh", s.cfg.InstanceID)
		span.AddEvent("wal.recovery.no_files")
		return nil
	}

	sort.Strings(files)
	totalRecovered := 0
	duplicatesSkipped := 0

	span.SetAttributes(attribute.Int("wal.files.count", len(files)))

	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			log.Printf("[%s] Skipping unreadable WAL file %s: %v", s.cfg.InstanceID, f, err)
			continue
		}
		var events []Event
		if err := json.Unmarshal(data, &events); err != nil {
			log.Printf("[%s] Skipping corrupt WAL file %s: %v", s.cfg.InstanceID, f, err)
			continue
		}

		for _, e := range events {
			if !s.dedupCache.IsDuplicate(ctx, e.EventID, e.Timestamp) {
				s.buffer = append(s.buffer, e)
				s.bufferSize += int64(len(e.Payload))
				totalRecovered++
			} else {
				duplicatesSkipped++
			}
		}

		log.Printf("[%s] Recovered %d events from %s (skipped %d duplicates)",
			s.cfg.InstanceID, len(events)-duplicatesSkipped, filepath.Base(f), duplicatesSkipped)
	}

	span.SetAttributes(
		attribute.Int("wal.recovered.events", totalRecovered),
		attribute.Int("wal.recovered.duplicates_skipped", duplicatesSkipped),
		attribute.Int64("wal.recovered.bytes", s.bufferSize),
	)

	if totalRecovered > 0 {
		log.Printf("[%s] Total recovered: %d events (%.1f MB) — will flush immediately",
			s.cfg.InstanceID, totalRecovered, float64(s.bufferSize)/(1024*1024))
		go s.flushToS3()
	}

	span.SetStatus(codes.Ok, "WAL recovery complete")
	return nil
}

func (s *WriteService) writeWAL() {
	ctx, span := s.tracer.Start(s.ctx, "wal.write")
	defer span.End()
	startTime := time.Now()

	if !s.walDirty.Load() || s.walInFlight.Load() {
		span.AddEvent("wal.write.skipped")
		return
	}
	if !s.walInFlight.CompareAndSwap(false, true) {
		span.AddEvent("wal.write.already_in_flight")
		return
	}
	defer s.walInFlight.Store(false)

	s.mu.RLock()
	if len(s.buffer) == 0 {
		s.mu.RUnlock()
		span.AddEvent("wal.write.empty_buffer")
		return
	}

	snapshot := make([]Event, len(s.buffer))
	copy(snapshot, s.buffer)
	s.mu.RUnlock()

	span.SetAttributes(
		attribute.Int("wal.events.count", len(snapshot)),
		attribute.Int64("wal.sequence", s.walSeq),
	)

	buf := s.bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer s.bufferPool.Put(buf)

	encoder := json.NewEncoder(buf)
	if err := encoder.Encode(snapshot); err != nil {
		log.Printf("[%s] WAL marshal error: %v", s.cfg.InstanceID, err)
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to marshal events")
		return
	}

	path := s.walPath(s.walSeq)
	tmp := path + ".tmp"

	if err := atomicWriteFile(tmp, buf.Bytes(), 0644); err != nil {
		log.Printf("[%s] WAL write error: %v", s.cfg.InstanceID, err)
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to write WAL file")
		return
	}

	if err := os.Rename(tmp, path); err != nil {
		log.Printf("[%s] WAL rename error: %v", s.cfg.InstanceID, err)
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to rename WAL file")
		return
	}

	s.walDirty.Store(false)

	duration := time.Since(startTime).Seconds()
	s.metrics.walWriteDuration.Record(ctx, duration)

	span.SetAttributes(
		attribute.Int("wal.bytes.written", buf.Len()),
		attribute.Float64("wal.duration.seconds", duration),
	)
	span.SetStatus(codes.Ok, "WAL write complete")
}

func atomicWriteFile(filename string, data []byte, perm os.FileMode) error {
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}

	_, err = f.Write(data)
	if err1 := f.Sync(); err1 != nil && err == nil {
		err = err1
	}
	if err1 := f.Close(); err1 != nil && err == nil {
		err = err1
	}
	return err
}

func (s *WriteService) walWriterLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.cfg.WALWriteInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			s.writeWAL()
			return
		case <-ticker.C:
			s.writeWAL()
		}
	}
}

func (s *WriteService) initKafkaReader() {
	s.reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{s.cfg.KafkaBroker},
		Topic:          s.cfg.KafkaTopic,
		GroupID:        s.cfg.KafkaGroupID,
		MinBytes:       10e3,
		MaxBytes:       10e6,
		MaxWait:        100 * time.Millisecond,
		CommitInterval: 0,
		StartOffset:    kafka.FirstOffset,
		QueueCapacity:  1000,
		ErrorLogger: kafka.LoggerFunc(func(s string, a ...interface{}) {
			log.Printf("[kafka] "+s, a...)
		}),
	})
	log.Printf("[%s] Kafka reader ready — broker: %s topic: %s group: %s",
		s.cfg.InstanceID, s.cfg.KafkaBroker, s.cfg.KafkaTopic, s.cfg.KafkaGroupID)
}

func (s *WriteService) kafkaConsumerLoop(consumerID int) {
	defer s.wg.Done()
	log.Printf("[%s] Kafka consumer #%d started", s.cfg.InstanceID, consumerID)

	var pendingMessages []kafka.Message

	for {
		select {
		case <-s.ctx.Done():
			log.Printf("[%s] Kafka consumer #%d stopping", s.cfg.InstanceID, consumerID)
			if len(pendingMessages) > 0 {
				s.writeWAL()
				if err := s.reader.CommitMessages(s.ctx, pendingMessages...); err != nil {
					log.Printf("[%s] Final commit error: %v", s.cfg.InstanceID, err)
				}
			}
			return
		default:
		}

		ctx, span := s.tracer.Start(s.ctx, "kafka.consume.batch",
			trace.WithAttributes(attribute.Int("consumer.id", consumerID)))

		if s.flushInProgress.Load() {
			span.AddEvent("consumer.paused.flush_in_progress")
			span.End()
			time.Sleep(100 * time.Millisecond)
			continue
		}

		s.mu.RLock()
		bufLen := len(s.buffer)
		bufBytes := s.bufferSize
		s.mu.RUnlock()

		eventThreshold := int(float64(s.cfg.BufferLimit) * 0.85)
		byteThreshold := int64(float64(s.cfg.BufferSizeLimit) * 0.85)

		if bufLen >= eventThreshold || bufBytes >= byteThreshold {
			if bufLen%1000 == 0 {
				log.Printf("[%s] Consumer #%d: Buffer nearly full (events: %d/%d, bytes: %.1fMB/%.1fMB), pausing",
					s.cfg.InstanceID, consumerID, bufLen, s.cfg.BufferLimit,
					float64(bufBytes)/(1024*1024), float64(s.cfg.BufferSizeLimit)/(1024*1024))
			}
			span.AddEvent("consumer.paused.buffer_threshold",
				trace.WithAttributes(
					attribute.Int("buffer.events", bufLen),
					attribute.Int64("buffer.bytes", bufBytes),
				))
			span.End()
			time.Sleep(100 * time.Millisecond)
			continue
		}

		fetchStart := time.Now()
		fetchCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		messages, err := s.fetchMessageBatch(fetchCtx, s.cfg.KafkaBatchSize)
		cancel()
		fetchDuration := time.Since(fetchStart).Seconds()

		s.metrics.kafkaFetchDuration.Record(ctx, fetchDuration,
			metric.WithAttributes(
				attribute.Int("consumer.id", consumerID),
				attribute.Int("batch.size", len(messages)),
			))

		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				span.End()
				continue
			}
			if errors.Is(err, io.EOF) {
				span.AddEvent("consumer.eof")
				span.End()
				return
			}
			log.Printf("[%s] Consumer #%d: Kafka fetch error: %v", s.cfg.InstanceID, consumerID, err)
			span.RecordError(err)
			span.SetStatus(codes.Error, "Kafka fetch failed")
			span.End()
			time.Sleep(1 * time.Second)
			continue
		}

		if len(messages) == 0 {
			span.AddEvent("consumer.no_messages")
			span.End()
			continue
		}

		span.SetAttributes(
			attribute.Int("kafka.messages.fetched", len(messages)),
			attribute.Float64("kafka.fetch.duration", fetchDuration),
		)

		events := make([]Event, 0, len(messages))
		duplicates := 0

		for _, msg := range messages {
			parseStart := time.Now()
			event, err := s.parseKafkaMessage(ctx, msg.Value)
			parseDuration := time.Since(parseStart).Seconds()
			s.metrics.eventParseDuration.Record(ctx, parseDuration)

			if err != nil {
				s.dlqCount.Add(1)
				s.metrics.dlqEvents.Add(ctx, 1)
				span.AddEvent("event.parse.failed",
					trace.WithAttributes(attribute.String("error", err.Error())))
				continue
			}

			if s.dedupCache.IsDuplicate(ctx, event.EventID, event.Timestamp) {
				duplicates++
				continue
			}

			events = append(events, event)
		}

		if duplicates > 0 {
			s.duplicatesFiltered.Add(int64(duplicates))
			s.metrics.duplicatesFiltered.Add(ctx, int64(duplicates))
			span.AddEvent("events.duplicates.filtered",
				trace.WithAttributes(attribute.Int("count", duplicates)))
		}

		if len(events) > 0 {
			var batchBytes int64
			for _, e := range events {
				batchBytes += int64(len(e.Payload))
			}

			s.mu.Lock()

			newEventCount := len(s.buffer) + len(events)
			newByteCount := s.bufferSize + batchBytes

			if newEventCount > s.cfg.BufferLimit || newByteCount > s.cfg.BufferSizeLimit {
				s.mu.Unlock()
				log.Printf("[%s] Consumer #%d: Buffer would exceed limit, triggering immediate flush (events: %d→%d, bytes: %.1fMB→%.1fMB)",
					s.cfg.InstanceID, consumerID, len(s.buffer), newEventCount,
					float64(s.bufferSize)/(1024*1024), float64(newByteCount)/(1024*1024))

				span.AddEvent("buffer.limit.exceeded",
					trace.WithAttributes(
						attribute.Int("buffer.events.current", len(s.buffer)),
						attribute.Int("buffer.events.new", newEventCount),
						attribute.Int64("buffer.bytes.current", s.bufferSize),
						attribute.Int64("buffer.bytes.new", newByteCount),
					))

				go s.flushToS3()
				span.End()
				time.Sleep(500 * time.Millisecond)
				continue
			}

			s.buffer = append(s.buffer, events...)
			s.bufferSize += batchBytes
			currentBufferSize := len(s.buffer)
			currentBufferBytes := s.bufferSize
			s.mu.Unlock()

			s.walDirty.Store(true)
			s.totalConsumed.Add(int64(len(events)))
			s.lastSecondCount.Add(int64(len(events)))
			s.totalBytesBuffered.Store(currentBufferBytes)

			s.metrics.eventsConsumed.Add(ctx, int64(len(events)))

			span.AddEvent("events.buffered",
				trace.WithAttributes(
					attribute.Int("events.count", len(events)),
					attribute.Int64("events.bytes", batchBytes),
					attribute.Int("buffer.total.events", currentBufferSize),
					attribute.Int64("buffer.total.bytes", currentBufferBytes),
				))

			eventPct := float64(currentBufferSize) / float64(s.cfg.BufferLimit) * 100
			bytePct := float64(currentBufferBytes) / float64(s.cfg.BufferSizeLimit) * 100
			if eventPct > 70 || bytePct > 70 {
				log.Printf("[%s] Consumer #%d: Buffer at %d/%d events (%.1f%%), %.1f/%.1f MB (%.1f%%)",
					s.cfg.InstanceID, consumerID, currentBufferSize, s.cfg.BufferLimit, eventPct,
					float64(currentBufferBytes)/(1024*1024), float64(s.cfg.BufferSizeLimit)/(1024*1024), bytePct)
			}
		}

		pendingMessages = append(pendingMessages, messages...)

		if len(pendingMessages) >= s.cfg.KafkaCommitBatch {
			s.writeWAL()
			if err := s.reader.CommitMessages(ctx, pendingMessages...); err != nil {
				log.Printf("[%s] Consumer #%d: Kafka commit error: %v", s.cfg.InstanceID, consumerID, err)
				span.RecordError(err)
			} else {
				span.AddEvent("kafka.messages.committed",
					trace.WithAttributes(attribute.Int("count", len(pendingMessages))))
			}
			pendingMessages = pendingMessages[:0]
		}

		span.SetStatus(codes.Ok, "Batch consumed")
		span.End()
	}
}

func (s *WriteService) fetchMessageBatch(ctx context.Context, maxMessages int) ([]kafka.Message, error) {
	messages := make([]kafka.Message, 0, maxMessages)

	for i := 0; i < maxMessages; i++ {
		msg, err := s.reader.FetchMessage(ctx)
		if err != nil {
			if len(messages) > 0 {
				return messages, nil
			}
			return nil, err
		}
		messages = append(messages, msg)

		select {
		case <-ctx.Done():
			return messages, nil
		default:
		}
	}

	return messages, nil
}

func (s *WriteService) parseKafkaMessage(ctx context.Context, raw []byte) (Event, error) {
	_, span := s.tracer.Start(ctx, "event.parse")
	defer span.End()

	span.SetAttributes(attribute.Int("message.size.bytes", len(raw)))

	var env KafkaEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Invalid JSON")
		return Event{}, fmt.Errorf("invalid JSON")
	}

	if env.EventID == "" {
		err := fmt.Errorf("missing _event_id")
		span.RecordError(err)
		span.SetStatus(codes.Error, "Missing event ID")
		return Event{}, err
	}
	if env.ReferenceID == "" {
		err := fmt.Errorf("missing reference_id")
		span.RecordError(err)
		span.SetStatus(codes.Error, "Missing reference ID")
		return Event{}, err
	}
	if env.EventTS == "" {
		err := fmt.Errorf("missing _event_timestamp")
		span.RecordError(err)
		span.SetStatus(codes.Error, "Missing timestamp")
		return Event{}, err
	}

	ts, err := parseTimestamp(env.EventTS)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Invalid timestamp")
		return Event{}, fmt.Errorf("invalid _event_timestamp: %w", err)
	}

	event := Event{
		EventID:     env.EventID,
		ReferenceID: env.ReferenceID,
		Timestamp:   ts,
		Category:    env.Category,
		EventType:   env.EventType,
		Payload:     raw,
	}

	span.SetAttributes(
		attribute.String("event.id", event.EventID),
		attribute.String("event.category", event.Category),
		attribute.String("event.type", event.EventType),
		attribute.String("event.reference_id", event.ReferenceID),
	)
	span.SetStatus(codes.Ok, "Event parsed")

	return event, nil
}

func (s *WriteService) flushSchedulerLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			if s.flushInProgress.Load() {
				s.mu.RLock()
				bufLen := len(s.buffer)
				bufBytes := s.bufferSize
				timeSinceFlush := time.Since(s.lastFlush)
				s.mu.RUnlock()

				if timeSinceFlush > time.Minute {
					log.Printf("[%s] WARNING: Flush running for %s (buffer: %d events, %.1f MB)",
						s.cfg.InstanceID, timeSinceFlush, bufLen, float64(bufBytes)/(1024*1024))
				}
				continue
			}

			s.mu.RLock()
			bufLen := len(s.buffer)
			bufBytes := s.bufferSize
			timeSinceFlush := time.Since(s.lastFlush)
			s.mu.RUnlock()

			shouldFlush := bufLen >= s.cfg.BufferLimit ||
				bufBytes >= s.cfg.BufferSizeLimit ||
				(bufLen > 0 && timeSinceFlush >= s.cfg.FlushInterval)

			if shouldFlush {
				go s.flushToS3()
			}
		}
	}
}

func (s *WriteService) flushToS3() {
	ctx, span := s.tracer.Start(s.ctx, "flush.to_s3")
	defer span.End()
	flushStart := time.Now()

	s.flushMutex.Lock()
	defer s.flushMutex.Unlock()

	if !s.flushInProgress.CompareAndSwap(false, true) {
		span.AddEvent("flush.already_in_progress")
		return
	}
	defer s.flushInProgress.Store(false)

	time.Sleep(50 * time.Millisecond)

	s.mu.Lock()
	if len(s.buffer) == 0 {
		s.mu.Unlock()
		span.AddEvent("flush.empty_buffer")
		return
	}
	events := make([]Event, len(s.buffer))
	copy(events, s.buffer)
	flushSize := s.bufferSize
	oldWALSeq := s.walSeq
	s.walSeq++
	s.buffer = nil
	s.bufferSize = 0
	s.lastFlush = time.Now()
	s.walDirty.Store(true)
	s.mu.Unlock()

	span.SetAttributes(
		attribute.Int("flush.events.count", len(events)),
		attribute.Int64("flush.size.bytes", flushSize),
		attribute.Int64("flush.wal.sequence", oldWALSeq),
	)

	log.Printf("[%s] Flushing %d events (%.2f MB) to S3 (WAL seq %d)",
		s.cfg.InstanceID, len(events), float64(flushSize)/(1024*1024), oldWALSeq)

	buf := s.bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer s.bufferPool.Put(buf)

	for _, e := range events {
		buf.Write(e.Payload)
		buf.WriteByte('\n')
	}

	timestamp := time.Now().Unix()
	segmentID := uuid.New().String()[:8]
	objectKey := fmt.Sprintf("events/%d/%s/%s.ndjson", timestamp, s.cfg.InstanceID, segmentID)

	uploadCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()

	uploadStart := time.Now()

	putInput := &s3.PutObjectInput{
		Bucket:      aws.String(s.cfg.S3Bucket),
		Key:         aws.String(objectKey),
		Body:        bytes.NewReader(buf.Bytes()),
		ContentType: aws.String("application/x-ndjson"),
		Metadata: map[string]string{
			"instance-id": s.cfg.InstanceID,
			"event-count": fmt.Sprintf("%d", len(events)),
			"wal-seq":     fmt.Sprintf("%d", oldWALSeq),
			"flush-ts":    fmt.Sprintf("%d", timestamp),
			"segment-id":  segmentID,
			"size-bytes":  fmt.Sprintf("%d", buf.Len()),
		},
	}

	_, err := s.s3Client.PutObject(uploadCtx, putInput)
	uploadDuration := time.Since(uploadStart).Seconds()

	s.metrics.s3UploadDuration.Record(ctx, uploadDuration,
		metric.WithAttributes(
			attribute.Int("upload.events", len(events)),
			attribute.Int("upload.bytes", buf.Len()),
		))

	if err != nil {
		log.Printf("[%s] S3 upload FAILED after %s: %v",
			s.cfg.InstanceID, time.Duration(uploadDuration*float64(time.Second)), err)

		span.RecordError(err)
		span.SetStatus(codes.Error, "S3 upload failed")
		span.SetAttributes(attribute.Float64("s3.upload.duration", uploadDuration))

		s.mu.Lock()
		s.buffer = append(events, s.buffer...)
		s.bufferSize += flushSize
		s.walSeq--
		s.mu.Unlock()
		s.failedFlushes.Add(1)
		s.metrics.failedFlushes.Add(ctx, 1)
		return
	}

	log.Printf("[%s] ✓ Uploaded %d events → s3://%s/%s (%.2f MB in %s = %.1f MB/s)",
		s.cfg.InstanceID, len(events), s.cfg.S3Bucket, objectKey,
		float64(buf.Len())/(1024*1024), time.Duration(uploadDuration*float64(time.Second)),
		float64(buf.Len())/(1024*1024)/uploadDuration)

	if uploadDuration > 20 {
		log.Printf("[%s] WARNING: Slow S3 upload (%.1fs)", s.cfg.InstanceID, uploadDuration)
		span.AddEvent("s3.upload.slow",
			trace.WithAttributes(attribute.Float64("duration.seconds", uploadDuration)))
	}

	span.SetAttributes(
		attribute.String("s3.object.key", objectKey),
		attribute.Int("s3.upload.bytes", buf.Len()),
		attribute.Float64("s3.upload.duration", uploadDuration),
		attribute.Float64("s3.upload.throughput.mbps", float64(buf.Len())/(1024*1024)/uploadDuration),
	)

	if err := os.Remove(s.walPath(oldWALSeq)); err != nil && !os.IsNotExist(err) {
		log.Printf("[%s] WAL delete warning: %v", s.cfg.InstanceID, err)
		span.AddEvent("wal.delete.failed",
			trace.WithAttributes(attribute.String("error", err.Error())))
	}

	s.totalS3Uploads.Add(1)
	s.totalFlushed.Add(int64(len(events)))
	s.metrics.s3Uploads.Add(ctx, 1)
	s.metrics.eventsFlushed.Add(ctx, int64(len(events)))

	flushTotalDuration := time.Since(flushStart).Seconds()
	s.metrics.flushDuration.Record(ctx, flushTotalDuration,
		metric.WithAttributes(
			attribute.Int("flush.events", len(events)),
			attribute.Int("flush.bytes", buf.Len()),
		))

	avgProcessingTime := flushTotalDuration / float64(len(events))
	s.metrics.eventProcessingDuration.Record(ctx, avgProcessingTime)

	span.SetAttributes(
		attribute.Float64("flush.total.duration", flushTotalDuration),
		attribute.Float64("event.avg.processing.time", avgProcessingTime),
	)
	span.SetStatus(codes.Ok, "Flush complete")
}

func (s *WriteService) metricsLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			count := s.lastSecondCount.Swap(0)
			s.currentEPS.Store(count)

			if count > 0 {
				s.mu.RLock()
				bufBytes := s.bufferSize
				s.mu.RUnlock()
				log.Printf("[%s] Throughput: %d events/sec (buffer: %.1f MB, dedup: %d)",
					s.cfg.InstanceID, count, float64(bufBytes)/(1024*1024), s.dedupCache.Size())
			}
		}
	}
}

func (s *WriteService) startHTTPServer() {
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/health", s.handleHealth)
	mux.HandleFunc("/internal/metrics", s.handleMetrics)
	mux.HandleFunc("/internal/flush", s.handleForceFlush)
	mux.HandleFunc("/internal/wal", s.handleWALStatus)

	addr := ":" + s.cfg.HTTPPort
	log.Printf("[%s] HTTP server on %s", s.cfg.InstanceID, addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Printf("[%s] HTTP server error: %v", s.cfg.InstanceID, err)
	}
}

func (s *WriteService) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	bufLen := len(s.buffer)
	bufBytes := s.bufferSize
	s.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":         "ok",
		"instance_id":    s.cfg.InstanceID,
		"buffer_events":  bufLen,
		"buffer_mb":      float64(bufBytes) / (1024 * 1024),
		"events_per_sec": s.currentEPS.Load(),
	})
}

func (s *WriteService) handleMetrics(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	bufLen := len(s.buffer)
	bufBytes := s.bufferSize
	timeSinceFlush := time.Since(s.lastFlush).Seconds()
	s.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"instance_id":         s.cfg.InstanceID,
		"total_consumed":      s.totalConsumed.Load(),
		"total_flushed":       s.totalFlushed.Load(),
		"events_per_sec":      s.currentEPS.Load(),
		"total_s3_uploads":    s.totalS3Uploads.Load(),
		"failed_flushes":      s.failedFlushes.Load(),
		"dlq_count":           s.dlqCount.Load(),
		"duplicates_filtered": s.duplicatesFiltered.Load(),
		"buffer_events":       bufLen,
		"buffer_mb":           float64(bufBytes) / (1024 * 1024),
		"buffer_limit_events": s.cfg.BufferLimit,
		"buffer_limit_mb":     float64(s.cfg.BufferSizeLimit) / (1024 * 1024),
		"last_flush_ago_s":    timeSinceFlush,
		"flush_in_progress":   s.flushInProgress.Load(),
		"wal_seq":             s.walSeq,
		"wal_dirty":           s.walDirty.Load(),
		"dedup_cache_size":    s.dedupCache.Size(),
	})
}

func (s *WriteService) handleForceFlush(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	log.Printf("[%s] Force flush triggered via HTTP", s.cfg.InstanceID)
	go s.flushToS3()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "flush initiated"})
}

func (s *WriteService) handleWALStatus(w http.ResponseWriter, r *http.Request) {
	path := s.walPath(s.walSeq)
	info, err := os.Stat(path)
	size := int64(0)
	if err == nil {
		size = info.Size()
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"wal_seq":   s.walSeq,
		"wal_path":  path,
		"wal_size":  size,
		"wal_dirty": s.walDirty.Load(),
	})
}

func main() {
	cfg := loadConfig()

	svc, err := NewWriteService(cfg)
	if err != nil {
		log.Fatalf("Failed to create write service: %v", err)
	}

	if err := svc.Start(); err != nil {
		log.Fatalf("Failed to start write service: %v", err)
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit

	svc.Stop()
}

func parseTimestamp(s string) (int64, error) {
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t.Unix(), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t.Unix(), nil
	}
	if t, err := time.Parse("2006-01-02T15:04:05", s); err == nil {
		return t.Unix(), nil
	}
	return 0, fmt.Errorf("cannot parse timestamp: %q", s)
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	var i int
	fmt.Sscanf(v, "%d", &i)
	return i
}

func getEnvInt64(key string, def int64) int64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	var i int64
	fmt.Sscanf(v, "%d", &i)
	return i
}

func getEnvDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

func getEnvBool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	return v == "true" || v == "1" || v == "yes"
}