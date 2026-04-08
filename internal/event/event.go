
package event

type Event struct {
	EventID     string `json:"event_id"`
	ReferenceID string `json:"reference_id"`
	Timestamp   int64  `json:"timestamp"`
	Category    string `json:"category"`
	EventType   string `json:"event_type"`
	Payload     []byte `json:"payload"` // original raw JSON bytes from Kafka
}