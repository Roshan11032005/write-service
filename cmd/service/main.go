package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"write-service/config"
	"write-service/internal/buffer"
	"write-service/internal/dedup"
	"write-service/internal/httpserver"
	kafkapkg "write-service/internal/kafka"
	appelotel "write-service/internal/otel"
	"write-service/internal/s3store"
	"write-service/internal/service"
	"write-service/internal/wal"
)

func main() {
	// ── Config ─────────────────────────────────────────────────────────────────
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	// ── OpenTelemetry ──────────────────────────────────────────────────────────
	otelProvider, err := appelotel.Init(cfg)
	if err != nil {
		log.Fatalf("otel init: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := otelProvider.Shutdown(ctx); err != nil {
			log.Printf("otel shutdown: %v", err)
		}
	}()

	// ── S3 ─────────────────────────────────────────────────────────────────────
	s3Client, err := s3store.New(context.Background(), cfg)
	if err != nil {
		log.Fatalf("s3 init: %v", err)
	}

	// ── WAL ────────────────────────────────────────────────────────────────────
	w, err := wal.New(cfg.WALDirectory, otelProvider.Tracer, otelProvider.Metrics)
	if err != nil {
		log.Fatalf("wal init: %v", err)
	}

	// ── Buffer ─────────────────────────────────────────────────────────────────
	buf := buffer.New(cfg.BufferLimit, cfg.BufferSizeLimit)

	// ── Deduplication ──────────────────────────────────────────────────────────
	dc := dedup.New(cfg.EnableDeduplication, cfg.DeduplicationWindow,
		otelProvider.Tracer, otelProvider.Metrics)

	// ── Kafka reader ───────────────────────────────────────────────────────────
	reader, err := kafkapkg.NewReader(cfg, otelProvider.Tracer, otelProvider.Metrics)
	if err != nil {
		log.Fatalf("kafka init: %v", err)
	}

	// ── Write service ──────────────────────────────────────────────────────────
	svc := service.New(cfg, reader, buf, w, s3Client, dc, otelProvider.Metrics, otelProvider.Tracer)

	if err := svc.Start(); err != nil {
		log.Fatalf("service start: %v", err)
	}
	defer svc.Stop()

	// ── Operational HTTP server ────────────────────────────────────────────────
	httpSrv := httpserver.New(cfg, svc)
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil {
			log.Fatalf("http server: %v", err)
		}
	}()

	log.Printf("  write-service started — instance=%s http=:%d kafka=%v",
		cfg.InstanceID, cfg.HTTPPort, cfg.KafkaBootstrapServers)

	// ── Graceful shutdown ──────────────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Printf("[main] signal received — shutting down")
}