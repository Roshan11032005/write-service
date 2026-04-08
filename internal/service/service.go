// Package service wires together all sub-packages and owns the main
// consume → buffer → flush lifecycle.
package service

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"write-service/config"
	"write-service/internal/buffer"
	"write-service/internal/dedup"
	"write-service/internal/event"
	kafkapkg "write-service/internal/kafka"
	appmetrics "write-service/internal/otel"
	"write-service/internal/s3store"
	"write-service/internal/wal"
)

// WriteService is the top-level service object.
type WriteService struct {
	cfg    *config.Config
	reader *kafkapkg.Reader
	buf    *buffer.Buffer
	wal    *wal.WAL
	s3     *s3store.Client
	dedup  *dedup.Cache
	otel   *appmetrics.Metrics
	tracer trace.Tracer

	// Atomics exposed to the HTTP metrics handler
	TotalConsumed      atomic.Int64
	TotalFlushed       atomic.Int64
	TotalS3Uploads     atomic.Int64
	FailedFlushes      atomic.Int64
	DLQCount           atomic.Int64
	DuplicatesFiltered atomic.Int64
	CurrentEPS         atomic.Int64
	lastSecondCount    atomic.Int64

	flushInProgress atomic.Bool
	flushMu         sync.Mutex
	lastFlush       time.Time
	lastFlushMu     sync.RWMutex

	bufferPool sync.Pool

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New constructs a WriteService from already-initialised dependencies.
func New(
	cfg *config.Config,
	reader *kafkapkg.Reader,
	buf *buffer.Buffer,
	w *wal.WAL,
	s3 *s3store.Client,
	dc *dedup.Cache,
	m *appmetrics.Metrics,
	tracer trace.Tracer,
) *WriteService {
	ctx, cancel := context.WithCancel(context.Background())

	svc := &WriteService{
		cfg:       cfg,
		reader:    reader,
		buf:       buf,
		wal:       w,
		s3:        s3,
		dedup:     dc,
		otel:      m,
		tracer:    tracer,
		lastFlush: time.Now(),
		ctx:       ctx,
		cancel:    cancel,
		bufferPool: sync.Pool{
			New: func() any {
				return bytes.NewBuffer(make([]byte, 0, 64<<10))
			},
		},
	}

	// Register observable gauges against the global meter
	meter := otel.GetMeterProvider().Meter(cfg.OTELServiceName)
	_, _ = meter.Int64ObservableGauge("buffer.events",
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(int64(svc.buf.Len()))
			return nil
		}))
	_, _ = meter.Int64ObservableGauge("buffer.bytes",
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(svc.buf.ByteSize())
			return nil
		}))
	_, _ = meter.Int64ObservableGauge("dedup.cache.size",
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(int64(svc.dedup.Size()))
			return nil
		}))

	return svc
}

// Start launches background goroutines.
func (s *WriteService) Start() error {
	ctx, span := s.tracer.Start(s.ctx, "service.start")
	defer span.End()

	log.Printf("[service] starting — instance=%s consumers=%d bufferLimit=%d",
		s.cfg.InstanceID, s.cfg.NumConsumers, s.cfg.BufferLimit)

	recovered, err := s.wal.Recover(ctx)
	if err != nil {
		log.Printf("[service] WAL recovery error (non-fatal): %v", err)
	}
	if len(recovered) > 0 {
		var recBytes int64
		for _, e := range recovered {
			recBytes += int64(len(e.Payload))
		}
		s.buf.Prepend(recovered, recBytes)
		log.Printf("[service] recovered %d events from WAL (%.1f MB) — flushing immediately",
			len(recovered), float64(recBytes)/(1024*1024))
		go s.flushToS3()
	}

	s.wg.Add(s.cfg.NumConsumers + 3)
	for i := 0; i < s.cfg.NumConsumers; i++ {
		go s.consumerLoop(i)
	}
	go s.walWriterLoop()
	go s.flushSchedulerLoop()
	go s.metricsLoop()

	span.SetStatus(codes.Ok, "started")
	log.Printf("[service] started")
	return nil
}

// Stop signals all goroutines, waits for them, then does a final flush.
func (s *WriteService) Stop() {
	log.Printf("[service] shutting down…")
	_, span := s.tracer.Start(context.Background(), "service.shutdown")
	defer span.End()

	s.cancel()
	s.reader.Close()

	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()

	select {
	case <-done:
		span.AddEvent("graceful.shutdown.complete")
	case <-time.After(30 * time.Second):
		log.Printf("[service] shutdown timeout — forcing final flush")
		span.AddEvent("graceful.shutdown.timeout")
	}

	log.Printf("[service] final flush…")
	s.flushToS3()
	span.SetStatus(codes.Ok, "shutdown complete")
}

// LastFlush returns the time of the most recent successful S3 flush.
func (s *WriteService) LastFlush() time.Time {
	s.lastFlushMu.RLock()
	defer s.lastFlushMu.RUnlock()
	return s.lastFlush
}

// FlushInProgress returns true if a flush goroutine is currently running.
func (s *WriteService) FlushInProgress() bool { return s.flushInProgress.Load() }

// WALInstance returns the WAL (used by the HTTP handler).
func (s *WriteService) WALInstance() *wal.WAL { return s.wal }

// DedupCache returns the dedup cache (used by the HTTP handler).
func (s *WriteService) DedupCache() *dedup.Cache { return s.dedup }

// ForceFlush triggers an async S3 flush (called by the HTTP handler).
func (s *WriteService) ForceFlush() { go s.flushToS3() }

// ─── Consumer loop ───────────────────────────────────────────────────────────
func (s *WriteService) consumerLoop(id int) {
	defer s.wg.Done()
	log.Printf("[service] consumer #%d started", id)

	var pending []*ckafka.Message

	for {
		select {
		case <-s.ctx.Done():
			// On shutdown, write WAL and commit remaining messages
			s.wal.Write(s.ctx, s.bufSnapshot())
			if len(pending) > 0 {
				_ = s.reader.CommitMessages(context.Background(), pending) // fixed
			}
			log.Printf("[service] consumer #%d stopped", id)
			return
		default:
		}

		if s.flushInProgress.Load() || s.buf.NearCapacity() {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		ctx, span := s.tracer.Start(s.ctx, "consumer.batch",
			trace.WithAttributes(attribute.Int("consumer.id", id)))

		events, msgs, dups, err := s.reader.FetchBatch(ctx, s.cfg.KafkaBatchSize, s.dedup.IsDuplicate)
		if err != nil {
			log.Printf("[service] consumer #%d fetch error: %v", id, err)
			span.RecordError(err)
			span.End()
			time.Sleep(time.Second)
			continue
		}

		if dups > 0 {
			s.DuplicatesFiltered.Add(int64(dups))
			s.otel.DuplicatesFiltered.Add(ctx, int64(dups))
		}

		if len(events) > 0 {
			var batchBytes int64
			for _, e := range events {
				batchBytes += int64(len(e.Payload))
			}

			_, _, wouldExceed := s.buf.Add(events, batchBytes)
			if wouldExceed {
				span.AddEvent("buffer.limit.exceeded")
				span.End()
				go s.flushToS3()
				time.Sleep(500 * time.Millisecond)
				continue
			}

			s.wal.MarkDirty()
			s.TotalConsumed.Add(int64(len(events)))
			s.lastSecondCount.Add(int64(len(events)))
			s.otel.EventsConsumed.Add(ctx, int64(len(events)))

			span.SetAttributes(
				attribute.Int("events.count", len(events)),
				attribute.Int64("events.bytes", batchBytes),
			)
		}

		if len(msgs) > 0 {
			pending = append(pending, msgs...)
		}

		if len(pending) >= s.cfg.KafkaCommitBatch {
			s.wal.Write(ctx, s.bufSnapshot())
			if err := s.reader.CommitMessages(ctx, pending); err != nil { // fixed
				log.Printf("[service] consumer #%d commit error: %v", id, err)
			}
			pending = pending[:0]
		}

		span.SetStatus(codes.Ok, "ok")
		span.End()
	}
}

// ─── WAL writer loop ─────────────────────────────────────────────────────────

func (s *WriteService) walWriterLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.cfg.WALWriteInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			s.wal.Write(s.ctx, s.bufSnapshot())
			return
		case <-ticker.C:
			s.wal.Write(s.ctx, s.bufSnapshot())
		}
	}
}

// ─── Flush scheduler loop ────────────────────────────────────────────────────

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
				continue
			}
			count, _ := s.buf.Snapshot()
			if s.buf.ShouldFlush() || (count > 0 && time.Since(s.LastFlush()) >= s.cfg.FlushInterval) {
				go s.flushToS3()
			}
		}
	}
}

// ─── Metrics loop ────────────────────────────────────────────────────────────

func (s *WriteService) metricsLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			count := s.lastSecondCount.Swap(0)
			s.CurrentEPS.Store(count)
			if count > 0 {
				log.Printf("[service] %d events/sec  buffer=%.1fMB  dedup=%d",
					count, float64(s.buf.ByteSize())/(1024*1024), s.dedup.Size())
			}
		}
	}
}

// ─── S3 flush ────────────────────────────────────────────────────────────────

func (s *WriteService) flushToS3() {
	ctx, span := s.tracer.Start(s.ctx, "flush.s3")
	defer span.End()
	start := time.Now()

	s.flushMu.Lock()
	defer s.flushMu.Unlock()

	if !s.flushInProgress.CompareAndSwap(false, true) {
		span.AddEvent("flush.already_in_progress")
		return
	}
	defer s.flushInProgress.Store(false)

	time.Sleep(50 * time.Millisecond) // let consumers finish current batch

	events, flushBytes := s.buf.Drain()
	if len(events) == 0 {
		span.AddEvent("flush.empty")
		return
	}

	oldSeq := s.wal.AdvanceSeq()
	s.wal.MarkDirty()

	log.Printf("[service] flushing %d events (%.2f MB) seq=%d",
		len(events), float64(flushBytes)/(1024*1024), oldSeq)

	// Build NDJSON
	buf := s.bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer s.bufferPool.Put(buf)

	for _, e := range events {
		buf.Write(e.Payload)
		buf.WriteByte('\n')
	}

	ts := time.Now().Unix()
	segID := uuid.New().String()[:8]
	key := fmt.Sprintf("events/%d/%s/%s.ndjson", ts, s.cfg.InstanceID, segID)

	uploadCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()

	uploadStart := time.Now()
	uploadErr := s.s3.PutObject(uploadCtx, key, "application/x-ndjson", buf.Bytes(), map[string]string{
		"instance-id": s.cfg.InstanceID,
		"event-count": fmt.Sprintf("%d", len(events)),
		"wal-seq":     fmt.Sprintf("%d", oldSeq),
		"flush-ts":    fmt.Sprintf("%d", ts),
		"segment-id":  segID,
		"size-bytes":  fmt.Sprintf("%d", buf.Len()),
	})
	uploadElapsed := time.Since(uploadStart).Seconds()

	s.otel.S3UploadDuration.Record(ctx, uploadElapsed,
		metric.WithAttributes(
			attribute.Int("upload.events", len(events)),
			attribute.Int("upload.bytes", buf.Len()),
		))

	if uploadErr != nil {
		log.Printf("[service] S3 upload FAILED (%.2fs): %v", uploadElapsed, uploadErr)
		span.RecordError(uploadErr)
		span.SetStatus(codes.Error, "s3 upload failed")
		s.buf.Prepend(events, flushBytes)
		s.wal.RollbackSeq()
		s.FailedFlushes.Add(1)
		s.otel.FailedFlushes.Add(ctx, 1)
		return
	}

	mbps := float64(buf.Len()) / (1024 * 1024) / uploadElapsed
	log.Printf("[service] ✓ %d events → s3://%s/%s (%.2f MB  %.1fs  %.1f MB/s)",
		len(events), s.cfg.S3Bucket, key, float64(buf.Len())/(1024*1024), uploadElapsed, mbps)

	s.wal.Delete(oldSeq)

	s.lastFlushMu.Lock()
	s.lastFlush = time.Now()
	s.lastFlushMu.Unlock()

	s.TotalS3Uploads.Add(1)
	s.TotalFlushed.Add(int64(len(events)))
	s.otel.S3Uploads.Add(ctx, 1)
	s.otel.EventsFlushed.Add(ctx, int64(len(events)))

	totalElapsed := time.Since(start).Seconds()
	s.otel.FlushDuration.Record(ctx, totalElapsed)
	s.otel.EventProcessingDuration.Record(ctx, totalElapsed/float64(len(events)))

	span.SetAttributes(
		attribute.String("s3.key", key),
		attribute.Float64("s3.upload.seconds", uploadElapsed),
		attribute.Float64("s3.upload.mbps", mbps),
	)
	span.SetStatus(codes.Ok, "flush complete")
}

// ─── helpers ─────────────────────────────────────────────────────────────────

// bufSnapshot returns a copy of buffer contents without draining it.
// Used by the WAL writer which needs to persist what's in memory without
// removing it from the active buffer.
func (s *WriteService) bufSnapshot() []event.Event {
	events, bytes := s.buf.Drain()
	if len(events) > 0 {
		s.buf.Prepend(events, bytes)
	}
	return events
}

// Buffer returns the event buffer (used by the HTTP handler).
func (s *WriteService) Buffer() *buffer.Buffer { return s.buf }