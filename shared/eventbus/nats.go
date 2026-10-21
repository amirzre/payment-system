package eventbus

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/amirzre/payment-system/shared/metrics"
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// NatsEventBus implements PublisherSubscriber using NATS JetStream.
type NatsEventBus struct {
	conn *nats.Conn
	js   nats.JetStreamContext
	log  *slog.Logger
}

// NewNatsEventBus creates a new NATS JetStream event bus.
func NewNatsEventBus(url string, log *slog.Logger) (*NatsEventBus, error) {
	if log == nil {
		log = slog.Default()
	}
	conn, err := connectNats(url, log)
	if err != nil {
		return nil, fmt.Errorf("connecting to NATS: %w", err)
	}

	js, err := conn.JetStream()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("getting jetstream context: %w", err)
	}

	// Provision the events emitted by the synchronous payment flow. Existing
	// streams for dormant saga subjects are intentionally left untouched.
	streams := []string{
		"ledger.posted",
		"payment.completed",
	}
	for _, subject := range streams {
		if err := ensureStream(js, subject); err != nil {
			conn.Close()
			return nil, fmt.Errorf("setting up stream for %s: %w", subject, err)
		}
	}

	return &NatsEventBus{conn: conn, js: js, log: log}, nil
}

func connectNats(url string, log *slog.Logger) (*nats.Conn, error) {
	// Outbox delivery and durable consumers must survive broker outages longer
	// than the client's default reconnect limit without requiring a restart.
	return nats.Connect(url,
		nats.MaxReconnects(-1),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			log.Warn("NATS disconnected", slog.Any("error", err))
		}),
		nats.ReconnectHandler(func(_ *nats.Conn) {
			log.Info("NATS reconnected")
		}),
	)
}

// ensureStream preserves an existing stream's retention, limits, and data.
// It only adds the required subject; unrelated setup failures are fatal.
func ensureStream(js nats.JetStreamContext, subject string) error {
	name := streamNameForSubject(subject)
	info, err := js.StreamInfo(name)
	if errors.Is(err, nats.ErrStreamNotFound) {
		_, err = js.AddStream(&nats.StreamConfig{Name: name, Subjects: []string{subject}, Storage: nats.FileStorage})
		if err == nil {
			return nil
		}
		// Another replica may have created the stream between lookup and add.
		if !errors.Is(err, nats.ErrStreamNameAlreadyInUse) {
			return err
		}
		info, err = js.StreamInfo(name)
	}
	if err != nil {
		return err
	}
	if info.Config.Storage != nats.FileStorage {
		return fmt.Errorf("stream %s must use file storage", name)
	}
	for _, existing := range info.Config.Subjects {
		if existing == subject {
			return nil
		}
	}
	config := info.Config
	config.Subjects = append(config.Subjects, subject)
	_, err = js.UpdateStream(&config)
	return err
}

// Publish publishes an event to the given subject.
// A nonempty key must identify one event on that subject. JetStream suppresses
// duplicate publications within the stream's duplicate window; consumers still
// need durable idempotency for deliveries outside that window.
func (n *NatsEventBus) Publish(ctx context.Context, subject string, key string, event any) (publishErr error) {
	ctx, span := otel.Tracer(tracerName).Start(
		ctx,
		"nats publish",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(eventAttributes(subject)...),
	)
	defer func() {
		if publishErr != nil {
			span.SetStatus(codes.Error, "publication failed")
		}
		span.End()
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
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
	if key != "" {
		// Include the subject to avoid collisions if stream subjects are merged.
		id := sha256.Sum256([]byte(subject + "\x00" + key))
		msg.Header.Set(nats.MsgIdHdr, fmt.Sprintf("%x", id))
	}

	propagation.TraceContext{}.Inject(ctx, propagation.HeaderCarrier(msg.Header))
	_, err = n.js.PublishMsg(msg, nats.Context(ctx))
	if err != nil {
		return fmt.Errorf("publishing to %s: %w", subject, err)
	}

	return nil
}

// Subscribe subscribes to events on the given subject.
// consumerName identifies this consumer group (e.g., "account-service").
func (n *NatsEventBus) Subscribe(ctx context.Context, subject, consumerName string, h Handler) error {
	streamName := streamNameForSubject(subject)

	// Inspect first so restarts preserve the durable's cursor and configuration.
	info, err := n.js.ConsumerInfo(streamName, consumerName, nats.Context(ctx))
	if errors.Is(err, nats.ErrConsumerNotFound) {
		info, err = n.js.AddConsumer(streamName, &nats.ConsumerConfig{
			Durable: consumerName, FilterSubject: subject,
			DeliverPolicy: nats.DeliverAllPolicy, AckPolicy: nats.AckExplicitPolicy,
		}, nats.Context(ctx))
		if err != nil {
			// A concurrent replica may have won creation; bind only after the
			// resulting consumer has been inspected successfully.
			if existing, lookupErr := n.js.ConsumerInfo(streamName, consumerName, nats.Context(ctx)); lookupErr == nil {
				info, err = existing, nil
			}
		}
	}
	if err != nil {
		return fmt.Errorf("setting up consumer %s on stream %s: %w", consumerName, streamName, err)
	}
	if info.Config.DeliverSubject != "" || info.Config.AckPolicy != nats.AckExplicitPolicy ||
		(info.Config.FilterSubject != "" && info.Config.FilterSubject != subject) || len(info.Config.FilterSubjects) > 0 {
		return fmt.Errorf("consumer %s on stream %s has incompatible configuration", consumerName, streamName)
	}
	if info.Config.FilterSubject == "" {
		// Legacy consumers omitted the filter on single-subject streams. They
		// remain safe only while the stream contains exactly that subject.
		stream, err := n.js.StreamInfo(streamName, nats.Context(ctx))
		if err != nil {
			return fmt.Errorf("checking unfiltered consumer stream %s: %w", streamName, err)
		}
		if len(stream.Config.Subjects) != 1 || stream.Config.Subjects[0] != subject {
			return fmt.Errorf("consumer %s on stream %s requires a subject filter", consumerName, streamName)
		}
	}

	// Explicit binding prevents Unsubscribe from deleting the durable consumer.
	sub, err := n.js.PullSubscribe(subject, consumerName, nats.Bind(streamName, consumerName), nats.Context(ctx))
	if err != nil {
		return fmt.Errorf("subscribing to %s: %w", subject, err)
	}

	go func() {
		defer func() {
			if err := sub.Unsubscribe(); err != nil && !errors.Is(err, nats.ErrBadSubscription) {
				n.log.ErrorContext(ctx, "unsubscribing", slog.String("subject", boundedSubject(subject)))
			}
		}()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			fetchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			msgs, err := sub.Fetch(10, nats.Context(fetchCtx))
			cancel()
			if err != nil {
				if ctx.Err() != nil || n.conn.IsClosed() {
					return
				}
				if errors.Is(err, nats.ErrTimeout) || errors.Is(err, context.DeadlineExceeded) {
					continue
				}
				n.log.ErrorContext(ctx, "fetching messages", slog.String("subject", boundedSubject(subject)))
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
				}
				continue
			}

			for _, msg := range msgs {
				if ctx.Err() != nil {
					return
				}
				n.processMessage(ctx, subject, consumerName, msg, h)
			}
		}
	}()

	return nil
}

// processMessage uses each delivery's producer parent, so retries get separate
// consumer spans without changing message identity, acknowledgment or effects.
func (n *NatsEventBus) processMessage(ctx context.Context, subject, consumerName string, msg *nats.Msg, h Handler) {
	ctx = extractEventContext(ctx, propagation.HeaderCarrier(msg.Header))
	ctx, span := otel.Tracer(tracerName).Start(
		ctx,
		"nats process",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(eventAttributes(subject)...),
	)
	delivery := &deliveryResult{}
	ctx = context.WithValue(ctx, deliveryResultKey{}, delivery)
	started := time.Now()
	outcome := "error"
	defer func() {
		metrics.RecordEvent(subject, consumerName, outcome, time.Since(started).Seconds())
		if outcome == "error" {
			span.SetStatus(codes.Error, "processing failed")
		}
		span.End()
	}()
	key := msg.Header.Get("X-Message-Key")
	if err := h(ctx, key, msg.Data); err != nil {
		n.log.ErrorContext(ctx, "handling event", slog.String("subject", boundedSubject(subject)))
		if err := msg.NakWithDelay(time.Second); err != nil {
			n.log.ErrorContext(ctx, "delaying event retry", slog.String("subject", boundedSubject(subject)))
		}
		return
	}
	ackCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	err := msg.AckSync(nats.Context(ackCtx))
	cancel()
	if err != nil {
		n.log.ErrorContext(ctx, "acknowledging event", slog.String("subject", boundedSubject(subject)))
		return
	}
	outcome = "success"
	if delivery.duplicate.Load() {
		outcome = "duplicate"
	}
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
