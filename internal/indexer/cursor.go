package indexer

import (
	"encoding/json"
	"os"
	"sync"
)

// CursorState tracks the last processed S3 key per bucket.
// Persisted to disk as JSON for crash recovery.
type CursorState struct {
	mu      sync.RWMutex
	Cursors map[string]string `json:"cursors"` // bucket → last processed S3 key
	path    string
}

// LoadCursor reads cursor state from disk, or creates empty state if absent.
func LoadCursor(path string) (*CursorState, error) {
	state := &CursorState{
		Cursors: make(map[string]string),
		path:    path,
	}

	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return state, nil
	}
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(data, state); err != nil {
		return nil, err
	}
	return state, nil
}

// Get returns the cursor (last processed key) for a bucket.
func (c *CursorState) Get(bucket string) string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Cursors[bucket]
}

// Set updates the cursor for a bucket.
func (c *CursorState) Set(bucket, key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Cursors[bucket] = key
}

// Save atomically writes cursor state to disk (write → tmp, rename).
func (c *CursorState) Save() error {
	c.mu.RLock()
	data, err := json.MarshalIndent(struct {
		Cursors map[string]string `json:"cursors"`
	}{Cursors: c.Cursors}, "", "  ")
	c.mu.RUnlock()

	if err != nil {
		return err
	}

	tmp := c.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, c.path)
}
