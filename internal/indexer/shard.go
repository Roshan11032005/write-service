package indexer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// KeyValue is a single entry destined for a RocksDB shard.
type KeyValue struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

// ShardWriter abstracts writes to RocksDB shards.
// Swap this interface for gRPC once proto generation is wired up (see proto/rocksdb.proto).
type ShardWriter interface {
	// BatchPut writes a batch of KVs to the specified shard.
	BatchPut(ctx context.Context, shardID uint32, kvs []KeyValue) error
	// Snapshot tells the shard to flush memtable → SST and upload to the given S3 key.
	Snapshot(ctx context.Context, shardID uint32, sstS3Key string) error
	// Compact tells the shard to merge delta SSTs into baseline.
	Compact(ctx context.Context, shardID uint32) error
	// Close releases all connections.
	Close() error
}

// ─── HTTP+JSON implementation ────────────────────────────────────────────────
// Production-ready transport using keep-alive connection pooling.
// Replace with gRPC client (proto/rocksdb.proto) for lower serialization overhead.

// HTTPShardWriter implements ShardWriter over HTTP+JSON.
type HTTPShardWriter struct {
	addrs  []string // base URL per shard (e.g. http://host:7000)
	client *http.Client
}

// NewHTTPShardWriter creates a shard writer with connection pooling tuned for high throughput.
func NewHTTPShardWriter(addrs []string) *HTTPShardWriter {
	return &HTTPShardWriter{
		addrs: addrs,
		client: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 32,
				MaxConnsPerHost:     64,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

type batchPutRequest struct {
	ShardID uint32     `json:"shard_id"`
	Pairs   []KeyValue `json:"pairs"`
}

func (w *HTTPShardWriter) BatchPut(ctx context.Context, shardID uint32, kvs []KeyValue) error {
	body, err := json.Marshal(batchPutRequest{ShardID: shardID, Pairs: kvs})
	if err != nil {
		return fmt.Errorf("marshal batch: %w", err)
	}

	addr := w.shardAddr(shardID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, addr+"/batch-put", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := w.client.Do(req)
	if err != nil {
		return fmt.Errorf("batch put shard %d: %w", shardID, err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("batch put shard %d: HTTP %d", shardID, resp.StatusCode)
	}
	return nil
}

type snapshotRequest struct {
	ShardID uint32 `json:"shard_id"`
	S3Key   string `json:"s3_key"`
}

func (w *HTTPShardWriter) Snapshot(ctx context.Context, shardID uint32, sstS3Key string) error {
	body, _ := json.Marshal(snapshotRequest{ShardID: shardID, S3Key: sstS3Key})

	addr := w.shardAddr(shardID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, addr+"/snapshot", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := w.client.Do(req)
	if err != nil {
		return fmt.Errorf("snapshot shard %d: %w", shardID, err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("snapshot shard %d: HTTP %d", shardID, resp.StatusCode)
	}
	return nil
}

type compactRequest struct {
	ShardID uint32 `json:"shard_id"`
}

func (w *HTTPShardWriter) Compact(ctx context.Context, shardID uint32) error {
	body, _ := json.Marshal(compactRequest{ShardID: shardID})

	addr := w.shardAddr(shardID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, addr+"/compact", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := w.client.Do(req)
	if err != nil {
		return fmt.Errorf("compact shard %d: %w", shardID, err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("compact shard %d: HTTP %d", shardID, resp.StatusCode)
	}
	return nil
}

func (w *HTTPShardWriter) Close() error {
	w.client.CloseIdleConnections()
	return nil
}

func (w *HTTPShardWriter) shardAddr(shardID uint32) string {
	return w.addrs[shardID%uint32(len(w.addrs))]
}
