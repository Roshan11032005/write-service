package indexer

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// Segment represents a single NDJSON segment file in S3.
type Segment struct {
	Bucket    string
	Key       string // full S3 object key
	Timestamp int64  // Unix seconds parsed from key
	WriterID  string // instance_id from key
	SegmentID string // segment UUID from key
	Size      int64  // object size in bytes
}

// ParseSegmentKey extracts metadata from an S3 key of the form:
//
//	events/{timestamp}/{writer_id}/{segment_id}.ndjson
func ParseSegmentKey(bucket, key string, size int64) (Segment, error) {
	parts := strings.Split(key, "/")
	if len(parts) < 4 {
		return Segment{}, fmt.Errorf("invalid segment key format: %s", key)
	}

	ts, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return Segment{}, fmt.Errorf("invalid timestamp in key %s: %w", key, err)
	}

	return Segment{
		Bucket:    bucket,
		Key:       key,
		Timestamp: ts,
		WriterID:  parts[2],
		SegmentID: strings.TrimSuffix(parts[3], ".ndjson"),
		Size:      size,
	}, nil
}

// SortSegments sorts by (timestamp, writer_id, segment_id) ascending.
func SortSegments(segs []Segment) {
	sort.Slice(segs, func(i, j int) bool {
		if segs[i].Timestamp != segs[j].Timestamp {
			return segs[i].Timestamp < segs[j].Timestamp
		}
		if segs[i].WriterID != segs[j].WriterID {
			return segs[i].WriterID < segs[j].WriterID
		}
		return segs[i].SegmentID < segs[j].SegmentID
	})
}
