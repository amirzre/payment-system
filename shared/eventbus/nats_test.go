package eventbus

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

func TestNatsConnectionKeepsReconnectingAndLogsLifecycle(t *testing.T) {
	url := os.Getenv("TEST_NATS_URL")
	if url == "" {
		t.Skip("set TEST_NATS_URL for connection integration tests")
	}
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	conn, err := connectNats(url, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if conn.Opts.MaxReconnect >= 0 {
		t.Fatalf("connection permanently closes after %d reconnect attempts", conn.Opts.MaxReconnect)
	}
	if conn.Opts.DisconnectedErrCB == nil || conn.Opts.ReconnectedCB == nil {
		t.Fatal("connection lifecycle logging is missing")
	}
	conn.Opts.DisconnectedErrCB(conn, errors.New("broker offline"))
	conn.Opts.ReconnectedCB(conn)
	if !strings.Contains(logs.String(), "broker offline") || !strings.Contains(logs.String(), "reconnected") {
		t.Fatalf("connection lifecycle logs missing: %s", logs.String())
	}
}

func testNats(t *testing.T) (*NatsEventBus, string) {
	t.Helper()
	url := os.Getenv("TEST_NATS_URL")
	if url == "" {
		t.Skip("set TEST_NATS_URL for isolated JetStream integration tests")
	}
	conn, err := connectNats(url, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	js, err := conn.JetStream()
	if err != nil {
		conn.Close()
		t.Fatal(err)
	}
	subject := "eventbus_test." + strings.ReplaceAll(uuid.NewString(), "-", "") + ".event"
	stream := streamNameForSubject(subject)
	if _, err = js.AddStream(&nats.StreamConfig{Name: stream, Subjects: []string{subject}, Storage: nats.MemoryStorage}); err != nil {
		conn.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := js.DeleteStream(stream); err != nil {
			t.Error(err)
		}
		conn.Close()
	})
	return &NatsEventBus{conn: conn, js: js, log: testLogger()}, subject
}

func TestNatsPublishDeduplicatesStableEventKey(t *testing.T) {
	bus, subject := testNats(t)
	for range 2 {
		if err := bus.Publish(context.Background(), subject, "event-id", json.RawMessage(`{"amount":9007199254740993}`)); err != nil {
			t.Fatal(err)
		}
	}
	info, err := bus.js.StreamInfo(streamNameForSubject(subject))
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("stable key produced %d stored messages", info.State.Msgs)
	}
	msg, err := bus.js.GetMsg(info.Config.Name, 1)
	if err != nil {
		t.Fatal(err)
	}
	if string(msg.Data) != `{"amount":9007199254740993}` {
		t.Fatalf("payload changed: %s", msg.Data)
	}
	// Unkeyed messages must not all collapse into the same deduplication ID.
	for range 2 {
		if err = bus.Publish(context.Background(), subject, "", map[string]bool{"ok": true}); err != nil {
			t.Fatal(err)
		}
	}
	info, err = bus.js.StreamInfo(info.Config.Name)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 3 {
		t.Fatalf("unkeyed events were dropped: messages=%d", info.State.Msgs)
	}
}

func TestNatsPublishHonorsCanceledContext(t *testing.T) {
	bus, subject := testNats(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := bus.Publish(ctx, subject, "canceled", map[string]bool{"ok": true}); !errors.Is(err, context.Canceled) {
		t.Fatalf("publish ignored cancellation: %v", err)
	}
	info, err := bus.js.StreamInfo(streamNameForSubject(subject))
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 0 {
		t.Fatal("canceled publish reached stream")
	}
}

func TestNatsFailedHandlerRedeliveryIsDelayed(t *testing.T) {
	bus, subject := testNats(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	deliveries := make(chan time.Time, 3)
	if err := bus.Subscribe(ctx, subject, "retry-test", func(context.Context, string, []byte) error {
		deliveries <- time.Now()
		return errors.New("retry later")
	}); err != nil {
		t.Fatal(err)
	}
	if err := bus.Publish(ctx, subject, "retry", map[string]bool{"ok": true}); err != nil {
		t.Fatal(err)
	}
	var first time.Time
	select {
	case first = <-deliveries:
	case <-time.After(3 * time.Second):
		t.Fatal("first delivery timed out")
	}
	select {
	case second := <-deliveries:
		if second.Sub(first) < 500*time.Millisecond {
			t.Fatalf("failure immediately redelivered after %v", second.Sub(first))
		}
	case <-time.After(4 * time.Second):
		t.Fatal("failed event was not retried")
	}
}

func TestNatsCancellationStopsFetchAndPreservesDurable(t *testing.T) {
	bus, subject := testNats(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	before := bus.conn.NumSubscriptions()
	if err := bus.Subscribe(ctx, subject, "durable-test", func(context.Context, string, []byte) error { return nil }); err != nil {
		t.Fatal(err)
	}
	// Allow the pull request to enter its wait before canceling.
	time.Sleep(100 * time.Millisecond)
	cancel()
	deadline := time.Now().Add(time.Second)
	for bus.conn.NumSubscriptions() != before {
		if time.Now().After(deadline) {
			t.Fatal("canceled subscriber kept waiting for fetch timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := bus.js.ConsumerInfo(streamNameForSubject(subject), "durable-test"); err != nil {
		t.Fatalf("cancellation deleted durable: %v", err)
	}
}

func TestNatsDurableReplicasAndRestart(t *testing.T) {
	bus, subject := testNats(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	delivered := make(chan string, 4)
	h := func(_ context.Context, key string, _ []byte) error { delivered <- key; return nil }
	for range 2 {
		if err := bus.Subscribe(ctx, subject, "shared-consumer", h); err != nil {
			t.Fatal(err)
		}
	}
	if err := bus.Publish(ctx, subject, "one", map[string]bool{"ok": true}); err != nil {
		t.Fatal(err)
	}
	select {
	case key := <-delivered:
		if key != "one" {
			t.Fatalf("unexpected key %q", key)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("first delivery timed out")
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		info, err := bus.js.ConsumerInfo(streamNameForSubject(subject), "shared-consumer")
		if err != nil {
			t.Fatal(err)
		}
		if info.AckFloor.Stream == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("delivery was not acknowledged")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	if err := bus.Subscribe(ctx2, subject, "shared-consumer", h); err != nil {
		t.Fatal(err)
	}
	if err := bus.Publish(ctx2, subject, "two", map[string]bool{"ok": true}); err != nil {
		t.Fatal(err)
	}
	select {
	case key := <-delivered:
		if key != "two" {
			t.Fatalf("restart replayed acknowledged message %q", key)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("restart delivery timed out")
	}
	select {
	case key := <-delivered:
		t.Fatalf("replicas delivered duplicate %q", key)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestStreamSetupPreservesExistingDataAndLimits(t *testing.T) {
	bus, subject := testNats(t)
	stream := streamNameForSubject(subject)
	if err := bus.js.DeleteStream(stream); err != nil {
		t.Fatal(err)
	}
	otherSubject := subject + ".other"
	if _, err := bus.js.AddStream(&nats.StreamConfig{Name: stream, Subjects: []string{otherSubject}, Storage: nats.FileStorage, MaxMsgs: 42}); err != nil {
		t.Fatal(err)
	}
	if _, err := bus.js.Publish(otherSubject, []byte("existing-data")); err != nil {
		t.Fatal(err)
	}
	if err := ensureStream(bus.js, subject); err != nil {
		t.Fatal(err)
	}
	if err := ensureStream(bus.js, subject); err != nil {
		t.Fatalf("restarting setup failed: %v", err)
	}
	info, err := bus.js.StreamInfo(stream)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 1 || info.Config.MaxMsgs != 42 {
		t.Fatalf("setup changed data or limits: msgs=%d max=%d", info.State.Msgs, info.Config.MaxMsgs)
	}
	if _, err := bus.js.Publish(subject, []byte("new-data")); err != nil {
		t.Fatalf("required subject was not added: %v", err)
	}
	if _, err := bus.js.Publish(otherSubject, []byte("retained-subject")); err != nil {
		t.Fatalf("existing subject was removed: %v", err)
	}
}

func TestStreamSetupRejectsVolatileExistingStream(t *testing.T) {
	bus, subject := testNats(t)
	if err := ensureStream(bus.js, subject); err == nil {
		t.Fatal("accepted memory storage for durable events")
	}
}

func TestStreamSetupReportsSubjectConflict(t *testing.T) {
	bus, subject := testNats(t)
	stream := streamNameForSubject(subject)
	if err := bus.js.DeleteStream(stream); err != nil {
		t.Fatal(err)
	}
	conflictingStream := stream + "_CONFLICT"
	if _, err := bus.js.AddStream(&nats.StreamConfig{Name: conflictingStream, Subjects: []string{subject}, Storage: nats.MemoryStorage}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := bus.js.DeleteStream(conflictingStream); err != nil {
			t.Error(err)
		}
	})
	if err := ensureStream(bus.js, subject); err == nil {
		t.Fatal("subject conflict was silently accepted")
	}
	// Restore our original stream so the shared cleanup can remove it.
	if _, err := bus.js.AddStream(&nats.StreamConfig{Name: stream, Subjects: []string{subject + ".safe"}, Storage: nats.MemoryStorage}); err != nil {
		t.Fatal(err)
	}
}

func TestNatsRejectsUnfilteredConsumerOnMultiSubjectStream(t *testing.T) {
	bus, subject := testNats(t)
	stream := streamNameForSubject(subject)
	info, err := bus.js.StreamInfo(stream)
	if err != nil {
		t.Fatal(err)
	}
	info.Config.Subjects = append(info.Config.Subjects, subject+".other")
	if _, err = bus.js.UpdateStream(&info.Config); err != nil {
		t.Fatal(err)
	}
	if _, err = bus.js.AddConsumer(stream, &nats.ConsumerConfig{Durable: "unfiltered", AckPolicy: nats.AckExplicitPolicy}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err = bus.Subscribe(ctx, subject, "unfiltered", func(context.Context, string, []byte) error { return nil }); err == nil {
		t.Fatal("subscriber could receive events from unrelated subjects")
	}
}
