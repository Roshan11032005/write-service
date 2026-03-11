
package event


type KafkaEnvelope struct {
	EventID     string `json:"_event_id"`
	EventTS     string `json:"_event_timestamp"`
	Category    string `json:"_category"`
	EventType   string `json:"_event_type"`
	ReferenceID string `json:"reference_id"`
}


type Event struct {
	EventID     string `json:"event_id"`
	ReferenceID string `json:"reference_id"`
	Timestamp   int64  `json:"timestamp"`
	Category    string `json:"category"`
	EventType   string `json:"event_type"`
	Payload     []byte `json:"payload"` // original raw JSON bytes from Kafka
}