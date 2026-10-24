package app

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/amirzre/payment-system/shared/metrics"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func TestBalanceCacheRecordsHitAndMiss(t *testing.T) {
	url := os.Getenv("REDIS_TEST_URL")
	if url == "" {
		t.Skip("set REDIS_TEST_URL")
	}
	options, err := redis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	cache := redis.NewClient(options)
	defer func() { _ = cache.Close() }()
	id := uuid.New()
	key := "balance:" + id.String()
	defer cache.Del(context.Background(), key)
	registry := metrics.New("account-service")
	metrics.Activate(registry)
	defer metrics.Deactivate(registry)
	service := NewAccountService(nil, &balanceLedgerClient{}, cache)
	if _, err := service.GetBalance(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if err := cache.Set(context.Background(), key, "987:EUR", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := service.GetBalance(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	families, err := registry.Prometheus.Gather()
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]float64{}
	for _, family := range families {
		if family.GetName() != "cache_operations_total" {
			continue
		}
		for _, metric := range family.Metric {
			var operation, outcome string
			for _, label := range metric.Label {
				if label.GetName() == "operation" {
					operation = label.GetValue()
				}
				if label.GetName() == "outcome" {
					outcome = label.GetValue()
				}
			}
			values[operation+":"+outcome] = metric.GetCounter().GetValue()
		}
	}
	if values["get:hit"] != 1 || values["get:miss"] != 1 {
		t.Fatalf("missing exact cache metrics: %v", values)
	}
}
