package projection

import "time"

// Event is the transport-independent event envelope passed to projections.
// Payload is intentionally opaque: it may contain JSON, protobuf bytes, or
// another application-defined encoding.
type Event struct {
	ID        string
	Category  string
	EventTime time.Time
	Payload   []byte
}
