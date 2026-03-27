package indexer

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// BuilderConfig holds all configuration for the index builder.
type BuilderConfig struct {
	InstanceID string

	// S3 source buckets
	Buckets       []string
	S3Endpoint    string
	S3Region      string
	S3AccessKey   string
	S3SecretKey   string
	SegmentPrefix string
	PollInterval  time.Duration

	// Processing
	Parallelism int // concurrent segment workers (default 8)
	BatchSize   int // KVs per shard write (default 5000)

	// Sharding
	NumShards  uint32
	ShardAddrs []string // base URLs per shard

	// SST / Compaction
	SSTBucket           string
	SSTPrefix           string
	CompactionThreshold int

	// Cursor
	CursorPath string
}

// LoadBuilderConfig reads configuration from environment variables.
func LoadBuilderConfig() (*BuilderConfig, error) {
	bucketStr := bEnv("INDEX_BUCKETS", "events-0,events-1,events-2,events-3,events-4,events-5")
	buckets := splitTrim(bucketStr, ",")

	addrs := splitTrim(bEnv("ROCKSDB_SHARD_ADDRS", "http://localhost:7000"), ",")
	numShards := bEnvInt("INDEX_NUM_SHARDS", len(addrs))

	cfg := &BuilderConfig{
		InstanceID:    builderInstanceID(),
		Buckets:       buckets,
		S3Endpoint:    bEnv("S3_ENDPOINT", "http://localhost:9002"),
		S3Region:      bEnv("S3_REGION", "us-east-1"),
		S3AccessKey:   bEnv("S3_ACCESS_KEY", "admin"),
		S3SecretKey:   bEnv("S3_SECRET_KEY", "strongpassword"),
		SegmentPrefix: bEnv("INDEX_SEGMENT_PREFIX", "events/"),
		PollInterval:  bEnvDur("INDEX_POLL_INTERVAL", 5*time.Second),
		Parallelism:   bEnvInt("INDEX_PARALLELISM", 8),
		BatchSize:     bEnvInt("INDEX_BATCH_SIZE", 5000),
		NumShards:     uint32(numShards),
		ShardAddrs:    addrs,
		SSTBucket:     bEnv("INDEX_SST_BUCKET", "rocksdb-sst"),
		SSTPrefix:     bEnv("INDEX_SST_PREFIX", "rocksdb-sst/"),
		CompactionThreshold: bEnvInt("INDEX_COMPACTION_THRESHOLD", 32),
		CursorPath:    bEnv("INDEX_CURSOR_PATH", "./index_cursor.json"),
	}

	log.Printf("[indexer] config: buckets=%v shards=%d parallelism=%d batch=%d poll=%s",
		cfg.Buckets, cfg.NumShards, cfg.Parallelism, cfg.BatchSize, cfg.PollInterval)

	return cfg, nil
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func bEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func bEnvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return i
}

func bEnvDur(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

func splitTrim(s, sep string) []string {
	parts := strings.Split(s, sep)
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}

func builderInstanceID() string {
	if id := os.Getenv("INSTANCE_ID"); id != "" {
		return id
	}
	const idFile = "./instance_id"
	if data, err := os.ReadFile(idFile); err == nil {
		if id := strings.TrimSpace(string(data)); id != "" {
			return id
		}
	}
	id := uuid.New().String()[:8]
	if err := os.WriteFile(idFile, []byte(id), 0o644); err != nil {
		log.Printf("[indexer] warning: could not persist instance id: %v", err)
	}
	fmt.Fprintf(os.Stderr, "[indexer] generated instance_id=%s\n", id)
	return id
}
