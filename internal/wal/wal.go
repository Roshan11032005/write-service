// Package wal manages the Write-Ahead Log used to survive crashes before
// an S3 flush completes.
//
// Files are written atomically (write tmp → fsync → rename) so a partial
// write never leaves a corrupt WAL entry.
package wal

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"write-service/internal/event"
	appmetrics "write-service/internal/otel"
)

// WAL manages write-ahead log files on disk.
type WAL struct {
	dir     string
	seq     int64
	dirty   atomic.Bool
	inFlight atomic.Bool
	tracer  trace.Tracer
	metrics *appmetrics.Metrics
}

// New returns a WAL rooted at dir (created if absent).
func New(dir string, tracer trace.Tracer, metrics *appmetrics.Metrics) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("wal: create dir %q: %w", dir, err)
	}
	log.Printf("[wal] directory: %s", dir)
	return &WAL{dir: dir, tracer: tracer, metrics: metrics}, nil
}

// Path returns the file path for a given sequence number.
func (w *WAL) Path(seq int64) string {
	return filepath.Join(w.dir, fmt.Sprintf("wal-%d.json", seq))
}

// Seq returns the current WAL sequence number.
func (w *WAL) Seq() int64 { return w.seq }

// AdvanceSeq increments the sequence counter and returns the old value.
func (w *WAL) AdvanceSeq() int64 {
	old := w.seq
	w.seq++
	return old
}

// RollbackSeq decrements the sequence counter (called when S3 upload fails).
func (w *WAL) RollbackSeq() { w.seq-- }

// MarkDirty signals that the buffer has unflushed changes.
func (w *WAL) MarkDirty() { w.dirty.Store(true) }

// IsDirty returns true if there are unflushed changes.
func (w *WAL) IsDirty() bool { return w.dirty.Load() }

// Delete removes the WAL file for seq (best-effort, logs on failure).
func (w *WAL) Delete(seq int64) {
	path := w.Path(seq)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		log.Printf("[wal] delete %s warning: %v", path, err)
	}
}

// Write serialises events to the current WAL file atomically.
// It is a no-op if the buffer has not been marked dirty or a write is already
// in progress.
func (w *WAL) Write(ctx context.Context, events []event.Event) {
	ctx, span := w.tracer.Start(ctx, "wal.write")
	defer span.End()

	if !w.dirty.Load() || w.inFlight.Load() {
		span.AddEvent("wal.write.skipped")
		return
	}
	if !w.inFlight.CompareAndSwap(false, true) {
		span.AddEvent("wal.write.already_in_flight")
		return
	}
	defer w.inFlight.Store(false)

	if len(events) == 0 {
		span.AddEvent("wal.write.empty")
		return
	}

	start := time.Now()

	data, err := json.Marshal(events)
	if err != nil {
		log.Printf("[wal] marshal error: %v", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, "marshal failed")
		return
	}

	path := w.Path(w.seq)
	tmp := path + ".tmp"

	if err := atomicWrite(tmp, data); err != nil {
		log.Printf("[wal] write error: %v", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, "write failed")
		return
	}
	if err := os.Rename(tmp, path); err != nil {
		log.Printf("[wal] rename error: %v", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, "rename failed")
		return
	}

	w.dirty.Store(false)

	elapsed := time.Since(start).Seconds()
	w.metrics.WALWriteDuration.Record(ctx, elapsed)

	span.SetAttributes(
		attribute.Int("wal.events", len(events)),
		attribute.Int("wal.bytes", len(data)),
		attribute.Int64("wal.seq", w.seq),
		attribute.Float64("wal.duration.seconds", elapsed),
	)
	span.SetStatus(codes.Ok, "wal write complete")
}

// Recover reads all WAL files in the directory, returns all events found.
// Files that cannot be read or decoded are skipped with a log warning.
func (w *WAL) Recover(ctx context.Context) ([]event.Event, error) {
	_, span := w.tracer.Start(ctx, "wal.recover")
	defer span.End()

	pattern := filepath.Join(w.dir, "wal-*.json")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("wal: glob: %w", err)
	}
	if len(files) == 0 {
		log.Printf("[wal] no WAL files found — starting fresh")
		span.AddEvent("wal.recover.no_files")
		return nil, nil
	}

	sort.Strings(files)
	span.SetAttributes(attribute.Int("wal.files", len(files)))

	var all []event.Event
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			log.Printf("[wal] skipping unreadable file %s: %v", f, err)
			continue
		}
		var events []event.Event
		if err := json.Unmarshal(data, &events); err != nil {
			log.Printf("[wal] skipping corrupt file %s: %v", f, err)
			continue
		}
		log.Printf("[wal] recovered %d events from %s", len(events), filepath.Base(f))
		all = append(all, events...)
	}

	span.SetAttributes(attribute.Int("wal.recovered.events", len(all)))
	span.SetStatus(codes.Ok, "wal recovery complete")
	return all, nil
}

// FileInfo returns the size of the current WAL file (0 if it does not exist).
func (w *WAL) FileInfo() (path string, size int64) {
	path = w.Path(w.seq)
	if info, err := os.Stat(path); err == nil {
		size = info.Size()
	}
	return path, size
}

// ─── helpers ─────────────────────────────────────────────────────────────────

// atomicWrite writes data to filename with fsync before close.
func atomicWrite(filename string, data []byte) error {
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(data)
	syncErr := f.Sync()
	closeErr := f.Close()
	if writeErr != nil {
		return writeErr
	}
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}