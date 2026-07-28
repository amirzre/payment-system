package eventbus

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/nats-io/nats.go"
)

// NatsEventBus implements PublisherSubscriber using NATS JetStream.
type NatsEventBus struct {
	conn *nats.Conn
	js   nats.JetStreamContext
	log  *slog.Logger
}

// NewNatsEventBus creates a new NATS JetStream event bus.
func NewNatsEventBus(url string, log *slog.Logger) (*NatsEventBus, error) {
	conn, err := nats.Connect(url)
	if err != nil {
		return nil, fmt.Errorf("connecting to NATS: %w", err)
	}

	js, err := conn.JetStream()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("getting jetstream context: %w", err)
	}

	// Create streams for each event subject
	streams := []string{
		"payment.requested",
		"funds.reserved",
		"funds.reservation.failed",
		"ledger.posted",
		"payment.completed",
	}
	for _, subject := range streams {
		streamName := streamNameForSubject(subject)
		_, err := js.AddStream(&nats.StreamConfig{
			Name:     streamName,
			Subjects: []string{subject},
			Storage:  nats.FileStorage,
		})
		if err != nil {
			// Stream may already exist — not an error
			log.Info("stream info", slog.String("stream", streamName), slog.Any("error", err))
		}
	}

	return &NatsEventBus{conn: conn, js: js, log: log}, nil
}

// Publish publishes an event to the given subject.
func (n *NatsEventBus) Publish(ctx context.Context, subject string, key string, event any) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshaling event: %w", err)
	}

	msg := &nats.Msg{
		Subject: subject,
		Data:    data,
		Header: nats.Header{
			"X-Message-Key": []string{key},
		},
	}

	_, err = n.js.PublishMsg(msg)
	if err != nil {
		return fmt.Errorf("publishing to %s: %w", subject, err)
	}

	return nil
}

// Subscribe subscribes to events on the given subject.
// consumerName identifies this consumer group (e.g., "account-service").
func (n *NatsEventBus) Subscribe(ctx context.Context, subject, consumerName string, h Handler) error {
	streamName := streamNameForSubject(subject)

	// Create a durable consumer for the consumer group
	_, err := n.js.AddConsumer(streamName, &nats.ConsumerConfig{
		Durable:       consumerName,
		DeliverPolicy: nats.DeliverAllPolicy,
		AckPolicy:     nats.AckExplicitPolicy,
	})
	if err != nil {
		return fmt.Errorf("adding consumer %s to stream %s: %w", consumerName, streamName, err)
	}

	sub, err := n.js.PullSubscribe(subject, consumerName)
	if err != nil {
		return fmt.Errorf("subscribing to %s: %w", subject, err)
	}

	go func() {
		defer sub.Unsubscribe()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			msgs, err := sub.Fetch(10, nats.MaxWait(5_000_000_000)) // 5 second wait
			if err != nil {
				if err == nats.ErrTimeout || err == context.Canceled {
					continue
				}
				n.log.Error("fetching messages", slog.String("subject", subject), slog.Any("error", err))
				continue
			}

			for _, msg := range msgs {
				key := ""
				if msg.Header != nil {
					key = msg.Header.Get("X-Message-Key")
				}

				if err := h(ctx, key, msg.Data); err != nil {
					n.log.Error("handling event",
						slog.String("subject", subject),
						slog.String("key", key),
						slog.Any("error", err),
					)
					// Nak the message so it can be redelivered
					msg.Nak()
					continue
				}

				msg.Ack()
			}
		}
	}()

	return nil
}

// Close closes the NATS connection.
func (n *NatsEventBus) Close() {
	n.conn.Close()
}

// streamNameForSubject converts a subject like "payment.requested" to a stream name "PAYMENT_REQUESTED".
func streamNameForSubject(subject string) string {
	name := ""
	for _, c := range subject {
		if c == '.' {
			name += "_"
		} else if c >= 'a' && c <= 'z' {
			name += string(c - 32) // uppercase
		} else {
			name += string(c)
		}
	}
	return name
}
