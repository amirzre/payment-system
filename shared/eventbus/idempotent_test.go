package eventbus

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestTransactionalIdempotencyRollsBackEffectsAndMarker(t *testing.T) {
	db := testDatabase(t)
	ctx := context.Background()
	fail := true
	h := WithTransactionalIdempotency(db, "db-consumer", func(ctx context.Context, tx pgx.Tx, key string, _ []byte) error {
		if _, err := tx.Exec(ctx, `INSERT INTO effects(event_key,value) VALUES($1,1)`, key); err != nil {
			return err
		}
		if fail {
			return errors.New("fail after effect")
		}
		return nil
	})
	if err := h(ctx, "event", nil); err == nil {
		t.Fatal("handler failure was ignored")
	}
	var effects, markers int
	if err := db.QueryRow(ctx, `SELECT (SELECT count(*) FROM effects), (SELECT count(*) FROM processed_events)`).Scan(&effects, &markers); err != nil {
		t.Fatal(err)
	}
	if effects != 0 || markers != 0 {
		t.Fatalf("failed transaction persisted effects=%d markers=%d", effects, markers)
	}
	fail = false
	if err := h(ctx, "event", nil); err != nil {
		t.Fatal(err)
	}
	if err := h(ctx, "event", nil); err != nil {
		t.Fatalf("duplicate ran handler: %v", err)
	}
	if err := db.QueryRow(ctx, `SELECT (SELECT count(*) FROM effects), (SELECT count(*) FROM processed_events)`).Scan(&effects, &markers); err != nil {
		t.Fatal(err)
	}
	if effects != 1 || markers != 1 {
		t.Fatalf("successful transaction effects=%d markers=%d", effects, markers)
	}
}

func TestInsertOutboxSharesBusinessTransaction(t *testing.T) {
	db := testDatabase(t)
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err = tx.Exec(ctx, `INSERT INTO effects(event_key,value) VALUES('atomic-event',1)`); err != nil {
		t.Fatal(err)
	}
	if err = InsertOutbox(ctx, tx, "test.event", "atomic-event", map[string]int{"amount": 25}); err != nil {
		t.Fatal(err)
	}
	if err = tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	var effects, events int
	if err = db.QueryRow(ctx, `SELECT (SELECT count(*) FROM effects), (SELECT count(*) FROM outbox)`).Scan(&effects, &events); err != nil {
		t.Fatal(err)
	}
	if effects != 0 || events != 0 {
		t.Fatalf("rollback left effects=%d events=%d", effects, events)
	}
}
