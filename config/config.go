// Package config loads runtime configuration from (in priority order):
//  1. Existing environment variables
//  2. .env file (optional)
//  3. Built-in defaults
package config

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Config holds all runtime configuration for the write service.
type Config struct {
	// Identity
	InstanceID string

	// Kafka
	KafkaBootstrapServers []string
	KafkaTopic            string
	KafkaGroupID          string
	KafkaAutoOffsetReset  string
	KafkaBatchSize        int
	KafkaCommitBatch      int

	// Buffer
	BufferLimit     int
	BufferSizeLimit int64
	FlushInterval   time.Duration

	// WAL
	WALDirectory     string
	WALWriteInterval time.Duration

	// S3
	S3Endpoint  string
	S3Bucket    string
	S3AccessKey string
	S3SecretKey string
	S3Region    string

	// Service
	HTTPPort    int
	NumConsumers int

	// Features
	EnableCompression   bool
	EnableDeduplication bool
	DeduplicationWindow time.Duration

	// OpenTelemetry
	OTELEndpoint       string
	OTELServiceName    string
	OTELServiceVersion string
}

// Load reads .env (if present), merges with the environment, then builds Config.
func Load() (*Config, error) {
	if err := loadDotEnv(".env"); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}

	instanceID := envStr("INSTANCE_ID", "")
	if instanceID == "" {
		instanceID = generateOrLoadInstanceID()
	}

	cfg := &Config{
		InstanceID: instanceID,

		KafkaBootstrapServers: envStrSlice("KAFKA_BOOTSTRAP_SERVERS", []string{"localhost:9092"}),
		KafkaTopic:            envStr("KAFKA_TOPIC", "events"),
		KafkaGroupID:          envStr("KAFKA_GROUP_ID", "write-service-group"),
		KafkaAutoOffsetReset:  envStr("KAFKA_AUTO_OFFSET_RESET", "earliest"),
		KafkaBatchSize:        envInt("KAFKA_BATCH_SIZE", 100),
		KafkaCommitBatch:      envInt("KAFKA_COMMIT_BATCH", 500),

		BufferLimit:     envInt("BUFFER_LIMIT", 10_000),
		BufferSizeLimit: envInt64("BUFFER_SIZE_LIMIT", 20*1024*1024),
		FlushInterval:   envDuration("FLUSH_INTERVAL", 10*time.Second),

		WALDirectory:     envStr("WAL_DIR", fmt.Sprintf("./wal/%s", instanceID)),
		WALWriteInterval: envDuration("WAL_WRITE_INTERVAL", 500*time.Millisecond),

		S3Endpoint:  envStr("S3_ENDPOINT", "http://localhost:9002"),
		S3Bucket:    envStr("S3_BUCKET", "events-bucket"),
		S3AccessKey: envStr("S3_ACCESS_KEY", "admin"),
		S3SecretKey: envStr("S3_SECRET_KEY", "strongpassword"),
		S3Region:    envStr("S3_REGION", "us-east-1"),

		HTTPPort:     envInt("HTTP_PORT", 8085),
		NumConsumers: envInt("NUM_CONSUMERS", 3),

		EnableCompression:   envBool("ENABLE_COMPRESSION", false),
		EnableDeduplication: envBool("ENABLE_DEDUPLICATION", true),
		DeduplicationWindow: envDuration("DEDUPLICATION_WINDOW", 5*time.Minute),

		OTELEndpoint:       envStr("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4318"),
		OTELServiceName:    envStr("OTEL_SERVICE_NAME", "write-service"),
		OTELServiceVersion: envStr("OTEL_SERVICE_VERSION", "1.0.0"),
	}

	log.Printf("[config] instance=%s kafka=%v topic=%s consumers=%d http=%d",
		cfg.InstanceID, cfg.KafkaBootstrapServers, cfg.KafkaTopic, cfg.NumConsumers, cfg.HTTPPort)

	return cfg, nil
}

// ─── Instance ID ──────────────────────────────────────────────────────────────

func generateOrLoadInstanceID() string {
	const idFile = "./instance_id"
	if data, err := os.ReadFile(idFile); err == nil {
		return strings.TrimSpace(string(data))
	}
	id := uuid.New().String()[:8]
	_ = os.WriteFile(idFile, []byte(id), 0o644)
	return id
}

// ─── .env loader ─────────────────────────────────────────────────────────────

func loadDotEnv(path string) error {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	for line := 1; s.Scan(); line++ {
		text := strings.TrimSpace(s.Text())
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		k, v, found := strings.Cut(text, "=")
		if !found {
			log.Printf("[config] .env line %d: skipping malformed %q", line, text)
			continue
		}
		k = strings.TrimSpace(k)
		v = stripQuotes(strings.TrimSpace(v))
		if os.Getenv(k) == "" {
			_ = os.Setenv(k, v)
		}
	}
	return s.Err()
}

// ─── env helpers ─────────────────────────────────────────────────────────────

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	var i int
	fmt.Sscanf(v, "%d", &i)
	return i
}

func envInt64(key string, def int64) int64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	var i int64
	fmt.Sscanf(v, "%d", &i)
	return i
}

func envDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		log.Printf("[config] %s: invalid duration %q, using default %s", key, v, def)
		return def
	}
	return d
}

func envStrSlice(key string, def []string) []string {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	parts := strings.Split(v, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

func envBool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	return v == "true" || v == "1" || v == "yes"
}

func stripQuotes(s string) string {
	if len(s) >= 2 &&
		((s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '\'' && s[len(s)-1] == '\'')) {
		return s[1 : len(s)-1]
	}
	return s
}