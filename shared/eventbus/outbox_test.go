package eventbus

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testDatabase(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set TEST_DATABASE_URL for isolated PostgreSQL integration tests")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	schema := "eventbus_test_" + uuid.NewString()[:8]
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	db, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		db.Close()
		_, err := admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
		admin.Close()
		if err != nil {
			t.Error(err)
		}
	})
	_, err = db.Exec(ctx, `CREATE TABLE outbox (id bigserial PRIMARY KEY, subject text NOT NULL, key text NOT NULL, payload text NOT NULL, published boolean NOT NULL DEFAULT false, created_at timestamptz DEFAULT now(), published_at timestamptz);
	CREATE TABLE processed_events (consumer_name text NOT NULL, event_key text NOT NULL, PRIMARY KEY (consumer_name, event_key));
	CREATE TABLE effects (event_key text PRIMARY KEY, value integer NOT NULL)`)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

type publisherFunc func(context.Context, string, string, any) error

func (f publisherFunc) Publish(ctx context.Context, subject, key string, payload any) error {
	return f(ctx, subject, key, payload)
}
func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func seedOutbox(t *testing.T, db *pgxpool.Pool, key, payload string) {
	t.Helper()
	if _, err := db.Exec(context.Background(), `INSERT INTO outbox(subject,key,payload) VALUES('test.event',$1,$2)`, key, payload); err != nil {
		t.Fatal(err)
	}
}

func TestOutboxPreservesJSONNumbers(t *testing.T) {
	db := testDatabase(t)
	seedOutbox(t, db, "big-number", `{"amount":9007199254740993}`)
	var got []byte
	publishOutbox(context.Background(), db, publisherFunc(func(_ context.Context, _, _ string, event any) error {
		var err error
		got, err = json.Marshal(event)
		return err
	}), testLogger())
	if string(got) != `{"amount":9007199254740993}` {
		t.Fatalf("payload lost precision: %s", got)
	}
}

func TestConcurrentOutboxPollersDoNotPublishLockedRows(t *testing.T) {
	db := testDatabase(t)
	seedOutbox(t, db, "locked", `{"ok":true}`)
	entered, release, firstDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	pub := publisherFunc(func(_ context.Context, _, _ string, _ any) error {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
		return nil
	})
	go func() { publishOutbox(context.Background(), db, pub, testLogger()); close(firstDone) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("first publisher never started")
	}
	publishOutbox(context.Background(), db, pub, testLogger())
	close(release)
	select {
	case <-firstDone:
	case <-time.After(3 * time.Second):
		t.Fatal("first publisher never finished")
	}
	if calls.Load() != 1 {
		t.Fatalf("row delivered concurrently %d times", calls.Load())
	}
}

func TestOutboxFailureRemainsRetryable(t *testing.T) {
	db := testDatabase(t)
	seedOutbox(t, db, "retry", `{"ok":true}`)
	publishOutbox(context.Background(), db, publisherFunc(func(context.Context, string, string, any) error { return errors.New("offline") }), testLogger())
	var published bool
	if err := db.QueryRow(context.Background(), `SELECT published FROM outbox`).Scan(&published); err != nil {
		t.Fatal(err)
	}
	if published {
		t.Fatal("failed publish was marked published")
	}
	publishOutbox(context.Background(), db, publisherFunc(func(context.Context, string, string, any) error { return nil }), testLogger())
	if err := db.QueryRow(context.Background(), `SELECT published FROM outbox`).Scan(&published); err != nil {
		t.Fatal(err)
	}
	if !published {
		t.Fatal("retry did not mark publish")
	}
}

func TestIdempotencyRejectsEmptyKey(t *testing.T) {
	defer func() {
		if recover() != nil {
			t.Error("empty event key accessed database before validation")
		}
	}()
	h := WithIdempotency(nil, "consumer", func(context.Context, string, []byte) error { t.Fatal("empty key reached handler"); return nil })
	if err := h(context.Background(), "", nil); err == nil {
		t.Fatal("accepted empty event key")
	}
}

func TestIdempotencySerializesDuplicateDeliveries(t *testing.T) {
	db := testDatabase(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	h := WithIdempotency(db, "consumer", func(context.Context, string, []byte) error {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
		return nil
	})
	errs := make(chan error, 2)
	go func() { errs <- h(context.Background(), "same-event", nil) }()
	<-entered
	go func() { errs <- h(context.Background(), "same-event", nil) }()
	// Wait until the second delivery is blocked on the unique marker, rather than
	// relying on goroutine scheduling to reproduce the conflict.
	deadline := time.Now().Add(3 * time.Second)
	for {
		var waiting bool
		err := db.QueryRow(context.Background(), `SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE '%INSERT INTO processed_events%')`).Scan(&waiting)
		if err != nil {
			close(release)
			t.Fatal(err)
		}
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			close(release)
			t.Fatal("second delivery did not contend on marker")
		}
		time.Sleep(10 * time.Millisecond)
	}
	close(release)
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("duplicate executed handler %d times", calls.Load())
	}
}
