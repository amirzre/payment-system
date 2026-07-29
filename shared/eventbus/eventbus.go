package eventbus

import "context"

// Handler is a function that processes an event.
// key is the message key (e.g., idempotency key or aggregate ID).
// payload is the raw event bytes (typically JSON-encoded protobuf or plain JSON).
type Handler func(ctx context.Context, key string, payload []byte) error

// Publisher publishes events to a topic/subject.
type Publisher interface {
	Publish(ctx context.Context, subject string, key string, event any) error
}

// Subscriber subscribes to events on a topic/subject.
type Subscriber interface {
	Subscribe(ctx context.Context, subject, consumerName string, h Handler) error
}

// PublisherSubscriber combines both interfaces.
type PublisherSubscriber interface {
	Publisher
	Subscriber
}
