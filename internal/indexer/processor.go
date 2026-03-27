package indexer

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Processor downloads and parses NDJSON segments, builds RocksDB KVs,
// routes them to shards via consistent hash, and writes in batches.
type Processor struct {
	s3     *s3.Client
	shards ShardWriter
	cfg    *BuilderConfig

	TotalIndexed atomic.Int64
	TotalErrors  atomic.Int64
}

// NewProcessor creates a Processor bound to the given S3 client and shard writer.
func NewProcessor(s3Client *s3.Client, shards ShardWriter, cfg *BuilderConfig) *Processor {
	return &Processor{
		s3:     s3Client,
		shards: shards,
		cfg:    cfg,
	}
}

// ProcessResult reports the outcome of processing a single segment.
type ProcessResult struct {
	Segment Segment
	Indexed int64
	Err     error
}

// ProcessBatch fans segments out to cfg.Parallelism workers (default 8).
// Returns one ProcessResult per segment in the same order as the input.
func (p *Processor) ProcessBatch(ctx context.Context, segments []Segment) []ProcessResult {
	results := make([]ProcessResult, len(segments))

	segCh := make(chan int, len(segments))
	for i := range segments {
		segCh <- i
	}
	close(segCh)

	var wg sync.WaitGroup
	for w := 0; w < p.cfg.Parallelism; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range segCh {
				indexed, err := p.processSegment(ctx, segments[idx])
				results[idx] = ProcessResult{
					Segment: segments[idx],
					Indexed: indexed,
					Err:     err,
				}
			}
		}()
	}
	wg.Wait()
	return results
}

// indexFields is the minimal struct for extracting the 3 required fields per NDJSON line.
type indexFields struct {
	EventID     string `json:"_event_id"`
	ReferenceID string `json:"reference_id"`
	EventTS     string `json:"_event_timestamp"`
}

// processSegment handles one segment end-to-end:
//  1. Streaming GetObject download
//  2. Line-by-line NDJSON parse
//  3. Build RocksDB key: ref:<reference_id>:time:<ts>:txn:<event_id>
//  4. Build RocksDB value: <bucket>|<key>|<start_byte>|<end_byte>
//  5. Route to shard via consistent hash on "ref:<reference_id>"
//  6. BatchPut when shard buffer reaches cfg.BatchSize
func (p *Processor) processSegment(ctx context.Context, seg Segment) (int64, error) {
	resp, err := p.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(seg.Bucket),
		Key:    aws.String(seg.Key),
	})
	if err != nil {
		return 0, fmt.Errorf("get %s/%s: %w", seg.Bucket, seg.Key, err)
	}
	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 256<<10), 4<<20) // 256 KB initial, 4 MB max line

	batches := make(map[uint32][]KeyValue)
	var offset int64
	var indexed int64

	for scanner.Scan() {
		line := scanner.Bytes()
		startByte := offset
		endByte := offset + int64(len(line))
		offset = endByte + 1 // +1 for the newline stripped by Scanner

		var f indexFields
		if err := json.Unmarshal(line, &f); err != nil {
			log.Printf("[processor] malformed JSON at byte %d in %s/%s: %v",
				startByte, seg.Bucket, seg.Key, err)
			p.TotalErrors.Add(1)
			continue
		}
		if f.EventID == "" || f.ReferenceID == "" {
			continue
		}

		ts := parseTimestamp(f.EventTS)

		rocksKey := []byte(fmt.Sprintf("ref:%s:time:%d:txn:%s", f.ReferenceID, ts, f.EventID))
		rocksVal := []byte(fmt.Sprintf("%s|%s|%d|%d", seg.Bucket, seg.Key, startByte, endByte))

		shardID := ShardForRef(f.ReferenceID, p.cfg.NumShards)
		batches[shardID] = append(batches[shardID], KeyValue{Key: rocksKey, Value: rocksVal})

		if len(batches[shardID]) >= p.cfg.BatchSize {
			if err := p.writeBatchWithRetry(ctx, shardID, batches[shardID]); err != nil {
				return indexed, fmt.Errorf("batch put shard %d: %w", shardID, err)
			}
			indexed += int64(len(batches[shardID]))
			p.TotalIndexed.Add(int64(len(batches[shardID])))
			batches[shardID] = batches[shardID][:0]
		}
	}

	if err := scanner.Err(); err != nil {
		return indexed, fmt.Errorf("scan %s/%s: %w", seg.Bucket, seg.Key, err)
	}

	// Flush remaining partial batches
	for shardID, kvs := range batches {
		if len(kvs) == 0 {
			continue
		}
		if err := p.writeBatchWithRetry(ctx, shardID, kvs); err != nil {
			return indexed, fmt.Errorf("flush shard %d: %w", shardID, err)
		}
		indexed += int64(len(kvs))
		p.TotalIndexed.Add(int64(len(kvs)))
	}

	return indexed, nil
}

// writeBatchWithRetry retries a failed BatchPut up to 3 times with exponential backoff.
func (p *Processor) writeBatchWithRetry(ctx context.Context, shardID uint32, kvs []KeyValue) error {
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		if err = p.shards.BatchPut(ctx, shardID, kvs); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(1<<attempt) * 200 * time.Millisecond):
		}
	}
	return err
}

// parseTimestamp tries RFC3339, RFC3339Nano, bare datetime, and Unix integer.
func parseTimestamp(s string) int64 {
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t.Unix()
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t.Unix()
	}
	if t, err := time.Parse("2006-01-02T15:04:05", s); err == nil {
		return t.Unix()
	}
	if ts, err := strconv.ParseInt(s, 10, 64); err == nil {
		return ts
	}
	return 0
}
