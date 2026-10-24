package consumer

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/amirzre/payment-system/shared/eventbus"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

func postedPayload(t *testing.T, tx, source, destination string) []byte {
	t.Helper()
	payload, err := json.Marshal(map[string]any{"transaction_id": tx, "source_account_id": source, "destination_account_id": destination, "amount_minor": 100, "currency": "USD"})
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func TestCacheInvalidationFailureRollsBackMarkerAndRecoveryRetries(t *testing.T) {
	databaseURL, redisURL := os.Getenv("TEST_DATABASE_URL"), os.Getenv("REDIS_TEST_URL")
	if databaseURL == "" || redisURL == "" {
		t.Skip("TEST_DATABASE_URL and REDIS_TEST_URL required for recovery integration")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	schema := "account_cache_test_" + uuid.NewString()[:8]
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Error(err)
		}
		admin.Close()
	})
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	db, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if _, err := db.Exec(ctx, `CREATE TABLE processed_events (consumer_name text NOT NULL, event_key text NOT NULL, PRIMARY KEY (consumer_name, event_key))`); err != nil {
		t.Fatal(err)
	}
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(options)
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	closed := redis.NewClient(options)
	_ = closed.Close()
	for _, tc := range []struct {
		name        string
		unavailable *redis.Client
	}{{"nil Redis", nil}, {"failed Redis DEL", closed}} {
		t.Run(tc.name, func(t *testing.T) {
			tx, source, destination := uuid.NewString(), uuid.NewString(), uuid.NewString()
			keys := []string{"balance:" + source, "balance:" + destination}
			t.Cleanup(func() { client.Del(context.Background(), keys...) })
			for _, key := range keys {
				if err := client.Set(ctx, key, "900:USD", time.Minute).Err(); err != nil {
					t.Fatal(err)
				}
			}
			payload := postedPayload(t, tx, source, destination)
			failed := eventbus.WithIdempotency(db, "account-cache", CacheInvalidationHandler(tc.unavailable, quietLogger()))
			if err := failed(ctx, tx, payload); err == nil {
				t.Fatal("failed invalidation must return an error")
			}
			var markers int
			if err := db.QueryRow(ctx, `SELECT count(*) FROM processed_events WHERE event_key=$1`, tx).Scan(&markers); err != nil {
				t.Fatal(err)
			}
			if markers != 0 {
				t.Fatalf("failed invalidation left %d processed markers", markers)
			}
			for _, key := range keys {
				if val, err := client.Get(ctx, key).Result(); err != nil || val != "900:USD" {
					t.Fatalf("failed invalidation changed cache: %q, %v", val, err)
				}
			}
			recovered := eventbus.WithIdempotency(db, "account-cache", CacheInvalidationHandler(client, quietLogger()))
			if err := recovered(ctx, tx, payload); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(ctx, `SELECT count(*) FROM processed_events WHERE event_key=$1`, tx).Scan(&markers); err != nil {
				t.Fatal(err)
			}
			if markers != 1 {
				t.Fatalf("recovered invalidation left %d processed markers, want 1", markers)
			}
			for _, key := range keys {
				if err := client.Get(ctx, key).Err(); err != redis.Nil {
					t.Fatalf("recovered invalidation did not delete %s: %v", key, err)
				}
			}
			// A duplicate committed event must not invalidate a newer cache fill.
			if err := client.Set(ctx, keys[0], "1000:USD", time.Minute).Err(); err != nil {
				t.Fatal(err)
			}
			if err := recovered(ctx, tx, payload); err != nil {
				t.Fatal(err)
			}
			if val, err := client.Get(ctx, keys[0]).Result(); err != nil || val != "1000:USD" {
				t.Fatalf("duplicate invalidated newer cache: %q, %v", val, err)
			}
		})
	}
}

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestCacheInvalidationRejectsMalformedSuppliedTransactionIdentifiers(t *testing.T) {
	url := os.Getenv("REDIS_TEST_URL")
	if url == "" {
		t.Skip("REDIS_TEST_URL is required for Redis integration")
	}
	options, err := redis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(options)
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	tx, account := uuid.NewString(), uuid.NewString()
	cacheKey := "balance:" + account
	t.Cleanup(func() { client.Del(context.Background(), cacheKey) })
	for _, tc := range []struct{ name, key, transaction string }{
		{"malformed key", "not-a-uuid", tx},
		{"nil key", uuid.Nil.String(), tx},
		{"malformed transaction", tx, "not-a-uuid"},
		{"nil transaction", tx, uuid.Nil.String()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := client.Set(context.Background(), cacheKey, "900:USD", time.Minute).Err(); err != nil {
				t.Fatal(err)
			}
			err := CacheInvalidationHandler(client, quietLogger())(context.Background(), tc.key, postedPayload(t, tc.transaction, "", account))
			if err == nil {
				t.Fatal("malformed transaction identifier must be rejected")
			}
			if val, err := client.Get(context.Background(), cacheKey).Result(); err != nil || val != "900:USD" {
				t.Fatalf("malformed event changed cache: %q, %v", val, err)
			}
		})
	}
}

func TestCacheInvalidationReturnsErrorWhenRedisUnavailable(t *testing.T) {
	tx, source, destination := uuid.NewString(), uuid.NewString(), uuid.NewString()
	closed := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	_ = closed.Close()
	for _, tc := range []struct {
		name   string
		client *redis.Client
	}{{"nil client", nil}, {"closed client", closed}} {
		t.Run(tc.name, func(t *testing.T) {
			if err := CacheInvalidationHandler(tc.client, quietLogger())(context.Background(), tx, postedPayload(t, tx, source, destination)); err == nil {
				t.Fatal("unavailable Redis must fail so delivery can be retried")
			}
		})
	}
}

func TestCacheInvalidationDeletesAffectedAccountsAndRejectsMalformedEvents(t *testing.T) {
	url := os.Getenv("REDIS_TEST_URL")
	if url == "" {
		t.Skip("REDIS_TEST_URL is required for Redis integration")
	}
	options, err := redis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(options)
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name                string
		source, destination string
		invalid             bool
	}{
		{"transfer", uuid.NewString(), uuid.NewString(), false},
		{"deposit", "", uuid.NewString(), false},
		{"invalid source", "not-a-uuid", uuid.NewString(), true},
		{"invalid destination", uuid.NewString(), "not-a-uuid", true},
		{"nil account", "", uuid.Nil.String(), true},
		{"missing accounts", "", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx := uuid.NewString()
			keys := []string{}
			for _, account := range []string{tc.source, tc.destination} {
				if account == "" {
					continue
				}
				if id, err := uuid.Parse(account); err != nil || id == uuid.Nil {
					continue
				}
				key := "balance:" + account
				keys = append(keys, key)
				if err := client.Set(context.Background(), key, "900:USD", time.Minute).Err(); err != nil {
					t.Fatal(err)
				}
			}
			t.Cleanup(func() {
				if len(keys) > 0 {
					client.Del(context.Background(), keys...)
				}
			})
			err := CacheInvalidationHandler(client, quietLogger())(context.Background(), tx, postedPayload(t, tx, tc.source, tc.destination))
			if tc.invalid {
				if err == nil {
					t.Fatal("malformed event must be rejected")
				}
				for _, key := range keys {
					if val, err := client.Get(context.Background(), key).Result(); err != nil || val != "900:USD" {
						t.Fatalf("invalid event changed cache %s: %q, %v", key, val, err)
					}
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, key := range keys {
				if err := client.Get(context.Background(), key).Err(); err != redis.Nil {
					t.Fatalf("cache key %s was not deleted: %v", key, err)
				}
			}
			if err := CacheInvalidationHandler(client, quietLogger())(context.Background(), tx, postedPayload(t, tx, tc.source, tc.destination)); err != nil {
				t.Fatalf("retry must safely invalidate already deleted keys: %v", err)
			}
		})
	}
}
