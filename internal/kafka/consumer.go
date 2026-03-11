// Package kafka wraps segmentio/kafka-go to provide a typed consumer that
// emits parsed events to the rest of the service.
package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	kgo "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"write-service/config"
	"write-service/internal/event"
	appmetrics "write-service/internal/otel"
)

// Reader wraps a kafka-go reader.
type Reader struct {
	r       *kgo.Reader
	cfg     *config.Config
	tracer  trace.Tracer
	metrics *appmetrics.Metrics
}

// NewReader creates and configures a kafka-go Reader.
func NewReader(cfg *config.Config, tracer trace.Tracer, metrics *appmetrics.Metrics) *Reader {
	r := kgo.NewReader(kgo.ReaderConfig{
		Brokers:        []string{cfg.KafkaBroker},
		Topic:          cfg.KafkaTopic,
		GroupID:        cfg.KafkaGroupID,
		MinBytes:       10e3,
		MaxBytes:       10e6,
		MaxWait:        100 * time.Millisecond,
		CommitInterval: 0,
		StartOffset:    kgo.FirstOffset,
		QueueCapacity:  1000,
		ErrorLogger: kgo.LoggerFunc(func(s string, a ...interface{}) {
			log.Printf("[kafka] "+s, a...)
		}),
	})

	log.Printf("[kafka] reader ready — broker=%s topic=%s group=%s",
		cfg.KafkaBroker, cfg.KafkaTopic, cfg.KafkaGroupID)

	return &Reader{r: r, cfg: cfg, tracer: tracer, metrics: metrics}
}

// Close shuts the underlying kafka-go reader down.
func (r *Reader) Close() { r.r.Close() }

// FetchBatch reads up to maxMessages from Kafka, parses them into Events, and
// deduplicates using the provided function.
// Returns (nil, nil, 0, nil) when the context is cancelled or times out.
func (r *Reader) FetchBatch(
	ctx context.Context,
	maxMessages int,
	isDuplicate func(ctx context.Context, eventID string, ts int64) bool,
) (events []event.Event, msgs []kgo.Message, duplicates int, err error) {
	fetchCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	fetchStart := time.Now()
	raw, fetchErr := r.fetchRaw(fetchCtx, maxMessages)
	elapsed := time.Since(fetchStart).Seconds()

	r.metrics.KafkaFetchDuration.Record(ctx, elapsed,
		metric.WithAttributes(attribute.Int("batch.size", len(raw))))

	if fetchErr != nil {
		if errors.Is(fetchErr, context.DeadlineExceeded) ||
			errors.Is(fetchErr, context.Canceled) ||
			errors.Is(fetchErr, io.EOF) {
			return nil, nil, 0, nil
		}
		return nil, nil, 0, fmt.Errorf("kafka fetch: %w", fetchErr)
	}
	if len(raw) == 0 {
		return nil, nil, 0, nil
	}

	msgs = raw
	events = make([]event.Event, 0, len(raw))

	for _, msg := range raw {
		parseStart := time.Now()
		e, parseErr := parseMessage(ctx, msg.Value, r.tracer)
		r.metrics.EventParseDuration.Record(ctx, time.Since(parseStart).Seconds())

		if parseErr != nil {
			r.metrics.DLQEvents.Add(ctx, 1)
			log.Printf("[kafka] DLQ: %v", parseErr)
			continue
		}

		if isDuplicate(ctx, e.EventID, e.Timestamp) {
			duplicates++
			continue
		}

		events = append(events, e)
	}

	return events, msgs, duplicates, nil
}

// CommitMessages commits a slice of raw kafka messages.
func (r *Reader) CommitMessages(ctx context.Context, msgs []kgo.Message) error {
	return r.r.CommitMessages(ctx, msgs...)
}

// ─── helpers ──────────────────────────────────────────────────────────────────

func (r *Reader) fetchRaw(ctx context.Context, max int) ([]kgo.Message, error) {
	msgs := make([]kgo.Message, 0, max)
	for i := 0; i < max; i++ {
		msg, err := r.r.FetchMessage(ctx)
		if err != nil {
			if len(msgs) > 0 {
				return msgs, nil
			}
			return nil, err
		}
		msgs = append(msgs, msg)
		select {
		case <-ctx.Done():
			return msgs, nil
		default:
		}
	}
	return msgs, nil
}

func parseMessage(ctx context.Context, raw []byte, tracer trace.Tracer) (event.Event, error) {
	_, span := tracer.Start(ctx, "event.parse",
		trace.WithAttributes(attribute.Int("message.bytes", len(raw))))
	defer span.End()

	var env event.KafkaEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "invalid JSON")
		return event.Event{}, fmt.Errorf("invalid JSON: %w", err)
	}

	switch {
	case env.EventID == "":
		return event.Event{}, fmt.Errorf("missing _event_id")
	case env.ReferenceID == "":
		return event.Event{}, fmt.Errorf("missing reference_id")
	case env.EventTS == "":
		return event.Event{}, fmt.Errorf("missing _event_timestamp")
	}

	ts, err := parseTimestamp(env.EventTS)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "invalid timestamp")
		return event.Event{}, fmt.Errorf("invalid _event_timestamp: %w", err)
	}

	e := event.Event{
		EventID:     env.EventID,
		ReferenceID: env.ReferenceID,
		Timestamp:   ts,
		Category:    env.Category,
		EventType:   env.EventType,
		Payload:     raw,
	}

	span.SetAttributes(
		attribute.String("event.id", e.EventID),
		attribute.String("event.reference_id", e.ReferenceID),
		attribute.String("event.category", e.Category),
	)
	span.SetStatus(codes.Ok, "parsed")
	return e, nil
}

func parseTimestamp(s string) (int64, error) {
	for _, f := range []string{time.RFC3339, time.RFC3339Nano, "2006-01-02T15:04:05"} {
		if t, err := time.Parse(f, s); err == nil {
			return t.Unix(), nil
		}
	}
	return 0, fmt.Errorf("cannot parse timestamp %q", s)
}