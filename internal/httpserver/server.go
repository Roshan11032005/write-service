// Package httpserver exposes the internal operational HTTP endpoints.
package httpserver

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"write-service/config"
	"write-service/internal/service"
)

// Server is the operational HTTP server.
type Server struct {
	cfg *config.Config
	svc *service.WriteService
}

// New returns a configured Server.
func New(cfg *config.Config, svc *service.WriteService) *Server {
	return &Server{cfg: cfg, svc: svc}
}

// ListenAndServe starts the HTTP server (blocks).
func (s *Server) ListenAndServe() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/health", s.handleHealth)
	mux.HandleFunc("/internal/metrics", s.handleMetrics)
	mux.HandleFunc("/internal/flush", s.handleForceFlush)
	mux.HandleFunc("/internal/wal", s.handleWAL)

	addr := fmt.Sprintf(":%d", s.cfg.HTTPPort)
	log.Printf("[http] operational server on %s", addr)
	return http.ListenAndServe(addr, mux)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, map[string]any{
		"status":         "ok",
		"instance_id":    s.cfg.InstanceID,
		"buffer_events":  s.svc.Buffer().Len(),
		"buffer_mb":      fmt.Sprintf("%.2f", float64(s.svc.Buffer().ByteSize())/(1024*1024)),
		"events_per_sec": s.svc.CurrentEPS.Load(),
	})
}

func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	count, bytes := s.svc.Buffer().Snapshot()
	writeJSON(w, map[string]any{
		"instance_id":         s.cfg.InstanceID,
		"total_consumed":      s.svc.TotalConsumed.Load(),
		"total_flushed":       s.svc.TotalFlushed.Load(),
		"events_per_sec":      s.svc.CurrentEPS.Load(),
		"total_s3_uploads":    s.svc.TotalS3Uploads.Load(),
		"failed_flushes":      s.svc.FailedFlushes.Load(),
		"dlq_count":           s.svc.DLQCount.Load(),
		"duplicates_filtered": s.svc.DuplicatesFiltered.Load(),
		"buffer_events":       count,
		"buffer_mb":           fmt.Sprintf("%.2f", float64(bytes)/(1024*1024)),
		"buffer_limit_events": s.cfg.BufferLimit,
		"buffer_limit_mb":     fmt.Sprintf("%.0f", float64(s.cfg.BufferSizeLimit)/(1024*1024)),
		"last_flush_ago_s":    fmt.Sprintf("%.1f", time.Since(s.svc.LastFlush()).Seconds()),
		"flush_in_progress":   s.svc.FlushInProgress(),
		"wal_seq":             s.svc.WALInstance().Seq(),
		"wal_dirty":           s.svc.WALInstance().IsDirty(),
		"dedup_cache_size":    s.svc.DedupCache().Size(),
	})
}

func (s *Server) handleForceFlush(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	log.Printf("[http] force flush triggered")
	s.svc.ForceFlush()
	writeJSON(w, map[string]string{"status": "flush initiated"})
}

func (s *Server) handleWAL(w http.ResponseWriter, _ *http.Request) {
	path, size := s.svc.WALInstance().FileInfo()
	writeJSON(w, map[string]any{
		"wal_seq":   s.svc.WALInstance().Seq(),
		"wal_path":  path,
		"wal_size":  size,
		"wal_dirty": s.svc.WALInstance().IsDirty(),
	})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("[http] encode error: %v", err)
	}
}