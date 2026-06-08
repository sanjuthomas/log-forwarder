package sink

import "context"

// Sink publishes encoded log records to a destination.
type Sink interface {
	Publish(ctx context.Context, payload []byte) error
	Close() error
}
