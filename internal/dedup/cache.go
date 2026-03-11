// Package dedup provides a time-windowed in-memory deduplication cache.
package dedup

import (
	"context"
	"log"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	appmetrics "write-service/internal/otel"
)

// Cache tracks event IDs seen within a configurable time window.
// All methods are safe for concurrent use.
type Cache struct {
	mu      sync.RWMutex
	cache   map[string]int64 // eventID → unix timestamp
	window  time.Duration
	enabled bool
	tracer  trace.Tracer
	metrics *appmetrics.Metrics
}

// New returns a ready Cache. If enabled is false, IsDuplicate always returns
// false without touching the map.
func New(enabled bool, window time.Duration, tracer trace.Tracer, metrics *appmetrics.Metrics) *Cache {
	dc := &Cache{
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

// IsDuplicate returns true if eventID was seen within the dedup window.
// On a miss it records the event so future calls with the same ID return true.
func (dc *Cache) IsDuplicate(ctx context.Context, eventID string, timestamp int64) bool {
	_, span := dc.tracer.Start(ctx, "dedup.check",
		trace.WithAttributes(attribute.String("event.id", eventID)))
	defer span.End()

	if !dc.enabled {
		span.SetAttributes(attribute.Bool("dedup.enabled", false))
		return false
	}

	dc.mu.RLock()
	_, exists := dc.cache[eventID]
	dc.mu.RUnlock()

	span.SetAttributes(
		attribute.Bool("dedup.duplicate", exists),
		attribute.Int("dedup.cache_size", dc.Size()),
	)

	if exists {
		return true
	}

	dc.mu.Lock()
	dc.cache[eventID] = timestamp
	dc.mu.Unlock()

	return false
}

// Size returns the current number of entries in the cache.
func (dc *Cache) Size() int {
	dc.mu.RLock()
	defer dc.mu.RUnlock()
	return len(dc.cache)
}

// ─── Background cleanup ───────────────────────────────────────────────────────

func (dc *Cache) cleanupLoop() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		dc.cleanup()
	}
}

func (dc *Cache) cleanup() {
	now := time.Now().Unix()
	windowSec := int64(dc.window.Seconds())

	dc.mu.Lock()
	defer dc.mu.Unlock()

	before := len(dc.cache)
	for id, ts := range dc.cache {
		if now-ts > windowSec {
			delete(dc.cache, id)
		}
	}
	after := len(dc.cache)

	if removed := before - after; removed > 0 {
		log.Printf("[dedup] cleanup: removed %d expired entries (%d remaining)", removed, after)
	}
}