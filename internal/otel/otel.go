// Package otel initialises the OpenTelemetry tracer and meter providers and
// exposes typed metric handles used throughout the service.
package otel

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"

	"write-service/config"
)

// Metrics groups every instrument the service records against.
type Metrics struct {
	// Counters
	EventsConsumed     metric.Int64Counter
	EventsFlushed      metric.Int64Counter
	DuplicatesFiltered metric.Int64Counter
	DLQEvents          metric.Int64Counter
	S3Uploads          metric.Int64Counter
	FailedFlushes      metric.Int64Counter

	// Histograms
	KafkaFetchDuration      metric.Float64Histogram
	EventParseDuration      metric.Float64Histogram
	WALWriteDuration        metric.Float64Histogram
	FlushDuration           metric.Float64Histogram
	S3UploadDuration        metric.Float64Histogram
	EventProcessingDuration metric.Float64Histogram
}

// Provider bundles a Tracer, the Metrics, and a shutdown function.
type Provider struct {
	Tracer   trace.Tracer
	Metrics  *Metrics
	shutdown func(context.Context) error
}

// Shutdown flushes and stops all exporters. Call on service shutdown.
func (p *Provider) Shutdown(ctx context.Context) error {
	return p.shutdown(ctx)
}

// Init sets up tracing + metrics exporters, registers them globally, and
// returns a ready Provider.
func Init(cfg *config.Config) (*Provider, error) {
	ctx := context.Background()

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(cfg.OTELServiceName),
			semconv.ServiceVersion(cfg.OTELServiceVersion),
			semconv.ServiceInstanceID(cfg.InstanceID),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("otel: create resource: %w", err)
	}

	// ── Tracing ───────────────────────────────────────────────────────────────
	traceExp, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint(cfg.OTELEndpoint),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("otel: trace exporter: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)

	// ── Metrics ───────────────────────────────────────────────────────────────
	metricExp, err := otlpmetrichttp.New(ctx,
		otlpmetrichttp.WithEndpoint(cfg.OTELEndpoint),
		otlpmetrichttp.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("otel: metric exporter: %w", err)
	}

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp,
			sdkmetric.WithInterval(10*time.Second))),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(mp)

	m, err := buildMetrics(mp.Meter(cfg.OTELServiceName))
	if err != nil {
		return nil, err
	}

	shutdown := func(ctx context.Context) error {
		if err := tp.Shutdown(ctx); err != nil {
			return fmt.Errorf("otel: tracer shutdown: %w", err)
		}
		if err := mp.Shutdown(ctx); err != nil {
			return fmt.Errorf("otel: meter shutdown: %w", err)
		}
		return nil
	}

	log.Printf("[otel] initialised — endpoint=%s service=%s", cfg.OTELEndpoint, cfg.OTELServiceName)

	return &Provider{
		Tracer:   tp.Tracer(cfg.OTELServiceName),
		Metrics:  m,
		shutdown: shutdown,
	}, nil
}

func buildMetrics(meter metric.Meter) (*Metrics, error) {
	var (
		m   Metrics
		err error
	)

	if m.EventsConsumed, err = meter.Int64Counter("events.consumed.total"); err != nil {
		return nil, err
	}
	if m.EventsFlushed, err = meter.Int64Counter("events.flushed.total"); err != nil {
		return nil, err
	}
	if m.DuplicatesFiltered, err = meter.Int64Counter("events.duplicates.filtered.total"); err != nil {
		return nil, err
	}
	if m.DLQEvents, err = meter.Int64Counter("events.dlq.total"); err != nil {
		return nil, err
	}
	if m.S3Uploads, err = meter.Int64Counter("s3.uploads.total"); err != nil {
		return nil, err
	}
	if m.FailedFlushes, err = meter.Int64Counter("flush.failures.total"); err != nil {
		return nil, err
	}
	if m.KafkaFetchDuration, err = meter.Float64Histogram("kafka.fetch.duration",
		metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if m.EventParseDuration, err = meter.Float64Histogram("event.parse.duration",
		metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if m.WALWriteDuration, err = meter.Float64Histogram("wal.write.duration",
		metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if m.FlushDuration, err = meter.Float64Histogram("flush.duration",
		metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if m.S3UploadDuration, err = meter.Float64Histogram("s3.upload.duration",
		metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if m.EventProcessingDuration, err = meter.Float64Histogram("event.processing.duration",
		metric.WithUnit("s")); err != nil {
		return nil, err
	}

	return &m, nil
}