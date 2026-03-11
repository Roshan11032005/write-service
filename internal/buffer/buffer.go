// Package buffer provides a thread-safe in-memory event buffer with dual
// limits: maximum event count and maximum byte size.
package buffer

import (
	"sync"
	"sync/atomic"

	"write-service/internal/event"
)

// Buffer is a thread-safe event accumulator.
type Buffer struct {
	mu         sync.RWMutex
	events     []event.Event
	byteSize   int64
	limitCount int
	limitBytes int64

	// Exported atomic counters read by the HTTP metrics handler without locking.
	TotalBytesBuffered atomic.Int64
}

// New returns a Buffer with the given count and byte limits.
func New(limitCount int, limitBytes int64) *Buffer {
	return &Buffer{
		limitCount: limitCount,
		limitBytes: limitBytes,
	}
}

// Add appends events to the buffer and returns the new counts.
// Returns (newCount, newBytes, wouldExceed) — the caller decides what to do
// when wouldExceed is true.
func (b *Buffer) Add(events []event.Event, batchBytes int64) (count int, bytes int64, wouldExceed bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	newCount := len(b.events) + len(events)
	newBytes := b.byteSize + batchBytes

	if newCount > b.limitCount || newBytes > b.limitBytes {
		return len(b.events), b.byteSize, true
	}

	b.events = append(b.events, events...)
	b.byteSize += batchBytes
	b.TotalBytesBuffered.Store(b.byteSize)

	return len(b.events), b.byteSize, false
}

// Drain atomically removes all events and returns them along with their byte size.
// Returns nil, 0 if the buffer is empty.
func (b *Buffer) Drain() ([]event.Event, int64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.events) == 0 {
		return nil, 0
	}

	events := make([]event.Event, len(b.events))
	copy(events, b.events)
	bytes := b.byteSize

	b.events = nil
	b.byteSize = 0
	b.TotalBytesBuffered.Store(0)

	return events, bytes
}

// Prepend inserts events at the front of the buffer (used to restore events
// after a failed S3 upload).
func (b *Buffer) Prepend(events []event.Event, bytes int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.events = append(events, b.events...)
	b.byteSize += bytes
	b.TotalBytesBuffered.Store(b.byteSize)
}

// Len returns the current event count.
func (b *Buffer) Len() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.events)
}

// ByteSize returns the current buffer size in bytes.
func (b *Buffer) ByteSize() int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.byteSize
}

// Snapshot returns current count and byte size without draining.
func (b *Buffer) Snapshot() (count int, bytes int64) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.events), b.byteSize
}

// ShouldFlush returns true when either limit is reached or the count
// threshold (85%) is exceeded.
func (b *Buffer) ShouldFlush() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.events) >= b.limitCount || b.byteSize >= b.limitBytes
}

// NearCapacity returns true when the buffer is at 85% of either limit.
func (b *Buffer) NearCapacity() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	eventPct := float64(len(b.events)) / float64(b.limitCount)
	bytePct := float64(b.byteSize) / float64(b.limitBytes)
	return eventPct >= 0.85 || bytePct >= 0.85
}