package indexer

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
)

// Builder is the main index-builder orchestrator.
// Each poll cycle: poll → merge → process(parallel) → advance cursor → SST snapshot → compaction.
type Builder struct {
	cfg       *BuilderConfig
	poller    *Poller
	cursor    *CursorState
	processor *Processor
	shards    ShardWriter
	s3        *s3.Client

	totalIndexed  atomic.Int64
	totalSegments atomic.Int64
	totalErrors   atomic.Int64
	pollCycles    atomic.Int64
}

// BuilderStats exposes counters for health endpoints.
type BuilderStats struct {
	TotalIndexed  int64 `json:"total_indexed"`
	TotalSegments int64 `json:"total_segments"`
	TotalErrors   int64 `json:"total_errors"`
	PollCycles    int64 `json:"poll_cycles"`
}

// NewBuilder wires up the poller, processor, shard writer, and cursor.
func NewBuilder(cfg *BuilderConfig) (*Builder, error) {
	poller, err := NewPoller(cfg)
	if err != nil {
		return nil, fmt.Errorf("create poller: %w", err)
	}

	cursor, err := LoadCursor(cfg.CursorPath)
	if err != nil {
		return nil, fmt.Errorf("load cursor: %w", err)
	}

	shards := NewHTTPShardWriter(cfg.ShardAddrs)
	processor := NewProcessor(poller.S3Client(), shards, cfg)

	b := &Builder{
		cfg:       cfg,
		poller:    poller,
		cursor:    cursor,
		processor: processor,
		shards:    shards,
		s3:        poller.S3Client(),
	}

	// Ensure SST bucket exists
	if err := poller.EnsureBucket(context.Background(), cfg.SSTBucket); err != nil {
		log.Printf("[builder] warning: SST bucket %s: %v", cfg.SSTBucket, err)
	}

	return b, nil
}

// Run starts the main poll loop. Blocks until ctx is cancelled.
func (b *Builder) Run(ctx context.Context) error {
	log.Printf("[builder] starting — instance=%s buckets=%d parallel=%d shards=%d",
		b.cfg.InstanceID, len(b.cfg.Buckets), b.cfg.Parallelism, b.cfg.NumShards)

	ticker := time.NewTicker(b.cfg.PollInterval)
	defer ticker.Stop()

	// Run one cycle immediately
	if err := b.pollCycle(ctx); err != nil {
		log.Printf("[builder] initial poll cycle: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			log.Printf("[builder] shutting down — indexed=%d segments=%d errors=%d cycles=%d",
				b.totalIndexed.Load(), b.totalSegments.Load(),
				b.totalErrors.Load(), b.pollCycles.Load())
			b.shards.Close()
			return ctx.Err()
		case <-ticker.C:
			if err := b.pollCycle(ctx); err != nil {
				log.Printf("[builder] poll cycle error: %v", err)
			}
		}
	}
}

// Stats returns a snapshot of builder counters.
func (b *Builder) Stats() BuilderStats {
	return BuilderStats{
		TotalIndexed:  b.totalIndexed.Load(),
		TotalSegments: b.totalSegments.Load(),
		TotalErrors:   b.totalErrors.Load(),
		PollCycles:    b.pollCycles.Load(),
	}
}

// ─── poll cycle ──────────────────────────────────────────────────────────────

func (b *Builder) pollCycle(ctx context.Context) error {
	start := time.Now()
	b.pollCycles.Add(1)

	// Step 1: Poll all 6 buckets concurrently
	segments, err := b.poller.Poll(ctx, b.cursor)
	if err != nil {
		return fmt.Errorf("poll: %w", err)
	}
	if len(segments) == 0 {
		return nil
	}

	log.Printf("[builder] polled %d new segments", len(segments))

	// Step 2 + 3: Process 8 segments in parallel (download → parse → batch put)
	results := b.processor.ProcessBatch(ctx, segments)

	// Step 4: Advance cursors (conservative — stop at first error per bucket)
	b.advanceCursors(results)

	if err := b.cursor.Save(); err != nil {
		log.Printf("[builder] cursor save error: %v", err)
	}

	// Step 5: SST snapshot per shard
	b.snapshotShards(ctx)

	// Step 6: Compaction check
	b.checkCompaction(ctx)

	var totalIdx int64
	var totalErr int
	for _, r := range results {
		if r.Err != nil {
			totalErr++
		} else {
			totalIdx += r.Indexed
		}
	}
	b.totalIndexed.Add(totalIdx)
	b.totalSegments.Add(int64(len(segments) - totalErr))
	b.totalErrors.Add(int64(totalErr))

	log.Printf("[builder] cycle done — %d segs, %d indexed, %d errors, %.2fs",
		len(segments), totalIdx, totalErr, time.Since(start).Seconds())
	return nil
}

// advanceCursors walks each bucket's segments in order, advancing the cursor
// only up to the last contiguous success (stops at first failure to avoid gaps).
func (b *Builder) advanceCursors(results []ProcessResult) {
	// Group result indices by bucket, preserving sorted order
	bucketIndices := make(map[string][]int)
	for i, r := range results {
		bucketIndices[r.Segment.Bucket] = append(bucketIndices[r.Segment.Bucket], i)
	}

	for bucket, indices := range bucketIndices {
		lastGood := ""
		for _, idx := range indices {
			if results[idx].Err != nil {
				break // don't advance past failed segment
			}
			lastGood = results[idx].Segment.Key
		}
		if lastGood != "" {
			b.cursor.Set(bucket, lastGood)
		}
	}
}

// ─── SST snapshot ────────────────────────────────────────────────────────────

func (b *Builder) snapshotShards(ctx context.Context) {
	ts := time.Now().Unix()
	batchID := uuid.New().String()[:8]

	for shardID := uint32(0); shardID < b.cfg.NumShards; shardID++ {
		sstKey := fmt.Sprintf("%sshard-%d/delta/%d_%s_%s.sst",
			b.cfg.SSTPrefix, shardID, ts, b.cfg.InstanceID, batchID)

		if err := b.shards.Snapshot(ctx, shardID, sstKey); err != nil {
			log.Printf("[builder] snapshot shard %d: %v", shardID, err)
		}
	}
}

// ─── compaction ──────────────────────────────────────────────────────────────

func (b *Builder) checkCompaction(ctx context.Context) {
	for shardID := uint32(0); shardID < b.cfg.NumShards; shardID++ {
		prefix := fmt.Sprintf("%sshard-%d/delta/", b.cfg.SSTPrefix, shardID)

		count, err := b.countObjects(ctx, b.cfg.SSTBucket, prefix)
		if err != nil {
			log.Printf("[builder] compaction check shard %d: %v", shardID, err)
			continue
		}

		if count >= b.cfg.CompactionThreshold {
			log.Printf("[builder] compacting shard %d (%d deltas ≥ threshold %d)",
				shardID, count, b.cfg.CompactionThreshold)
			if err := b.shards.Compact(ctx, shardID); err != nil {
				log.Printf("[builder] compact shard %d: %v", shardID, err)
			}
		}
	}
}

func (b *Builder) countObjects(ctx context.Context, bucket, prefix string) (int, error) {
	var count int
	var contToken *string

	for {
		input := &s3.ListObjectsV2Input{
			Bucket:  aws.String(bucket),
			Prefix:  aws.String(prefix),
			MaxKeys: aws.Int32(1000),
		}
		if contToken != nil {
			input.ContinuationToken = contToken
		}

		resp, err := b.s3.ListObjectsV2(ctx, input)
		if err != nil {
			return 0, err
		}

		count += len(resp.Contents)

		if resp.NextContinuationToken == nil {
			break
		}
		contToken = resp.NextContinuationToken
	}

	return count, nil
}
