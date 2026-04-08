// Package kafka wraps confluent-kafka-go to provide a typed consumer that
// emits parsed events to the rest of the service.
package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"write-service/config"
	"write-service/internal/event"
	"write-service/kafkautil"
	"write-service/model"
	appmetrics "write-service/internal/otel"
)

// Reader wraps a confluent-kafka-go consumer.
type Reader struct {
	consumer *ckafka.Consumer
	cfg      *config.Config
	tracer   trace.Tracer
	metrics  *appmetrics.Metrics
}

// NewReader creates and configures a confluent-kafka-go consumer.
func NewReader(cfg *config.Config, tracer trace.Tracer, metrics *appmetrics.Metrics) (*Reader, error) {
	consumer, err := kafkautil.CreateConsumer(&model.KafkaConfig{
		BootstrapServers: cfg.KafkaBootstrapServers,
		GroupId:          cfg.KafkaGroupID,
		AutoOffsetReset:  cfg.KafkaAutoOffsetReset,
	})
	if err != nil {
		return nil, fmt.Errorf("kafka consumer: %w", err)
	}

	if err := consumer.SubscribeTopics([]string{cfg.KafkaTopic}, nil); err != nil {
		consumer.Close()
		return nil, fmt.Errorf("kafka subscribe: %w", err)
	}

	log.Printf("[kafka] reader ready — brokers=%v topic=%s group=%s",
		cfg.KafkaBootstrapServers, cfg.KafkaTopic, cfg.KafkaGroupID)

	return &Reader{consumer: consumer, cfg: cfg, tracer: tracer, metrics: metrics}, nil
}

// Close shuts the underlying confluent-kafka-go consumer down.
func (r *Reader) Close() { r.consumer.Close() }

// FetchBatch reads up to maxMessages from Kafka, parses them into Events, and
// deduplicates using the provided function.
// Returns (nil, nil, 0, nil) when the context is cancelled or times out.
func (r *Reader) FetchBatch(
	ctx context.Context,
	maxMessages int,
	isDuplicate func(ctx context.Context, eventID string, ts int64) bool,
) (events []event.Event, msgs []*ckafka.Message, duplicates int, err error) {
	fetchStart := time.Now()
	raw, fetchErr := r.fetchRaw(ctx, maxMessages)
	elapsed := time.Since(fetchStart).Seconds()

	r.metrics.KafkaFetchDuration.Record(ctx, elapsed,
		metric.WithAttributes(attribute.Int("batch.size", len(raw))))

	if fetchErr != nil {
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

// CommitMessages commits offsets for a slice of consumed messages.
func (r *Reader) CommitMessages(ctx context.Context, msgs []*ckafka.Message) error {
	offsets := make([]ckafka.TopicPartition, len(msgs))
	for i, msg := range msgs {
		offsets[i] = ckafka.TopicPartition{
			Topic:     msg.TopicPartition.Topic,
			Partition: msg.TopicPartition.Partition,
			Offset:    msg.TopicPartition.Offset + 1,
		}
	}
	_, err := r.consumer.CommitOffsets(offsets)
	return err
}

// ─── helpers ──────────────────────────────────────────────────────────────────

func (r *Reader) fetchRaw(ctx context.Context, max int) ([]*ckafka.Message, error) {
	msgs := make([]*ckafka.Message, 0, max)
	deadline := time.Now().Add(2 * time.Second)

	for i := 0; i < max; i++ {
		select {
		case <-ctx.Done():
			return msgs, nil
		default:
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			return msgs, nil
		}

		ev := r.consumer.Poll(int(remaining.Milliseconds()))
		if ev == nil {
			return msgs, nil
		}

		switch e := ev.(type) {
		case *ckafka.Message:
			if e.TopicPartition.Error != nil {
				if len(msgs) > 0 {
					return msgs, nil
				}
				return nil, e.TopicPartition.Error
			}
			msgs = append(msgs, e)
		case ckafka.Error:
			if len(msgs) > 0 {
				return msgs, nil
			}
			return nil, e
		}
	}

	return msgs, nil
}

func parseMessage(ctx context.Context, raw []byte, tracer trace.Tracer) (event.Event, error) {
	_, span := tracer.Start(ctx, "event.parse",
		trace.WithAttributes(attribute.Int("message.bytes", len(raw))))
	defer span.End()

	var authEvent model.AuthTranEvent
	if err := json.Unmarshal(raw, &authEvent); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "invalid JSON")
		return event.Event{}, fmt.Errorf("invalid JSON: %w", err)
	}

	switch {
	case authEvent.EventID == "":
		return event.Event{}, fmt.Errorf("missing _event_id")
	case authEvent.EventTimestamp == "":
		return event.Event{}, fmt.Errorf("missing _event_timestamp")
	}

	ts, err := parseTimestamp(authEvent.EventTimestamp)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "invalid timestamp")
		return event.Event{}, fmt.Errorf("invalid _event_timestamp: %w", err)
	}

	referenceID := ""
	if authEvent.Data != nil {
		referenceID = authEvent.Data.HashUID
	}

	e := event.Event{
		EventID:     authEvent.EventID,
		ReferenceID: referenceID,
		Timestamp:   ts,
		Category:    authEvent.Category,
		EventType:   authEvent.EventType,
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