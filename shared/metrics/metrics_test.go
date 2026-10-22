package metrics

import (
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func TestRegistryRuntimeAndBoundedLabels(t *testing.T) {
	r := New("payment-service")
	Activate(r)
	defer Deactivate(r)
	for i := 0; i < 200; i++ {
		RecordEvent(fmt.Sprint("secret", i), fmt.Sprint("consumer", i), "invalid", .01)
		RecordRetry(fmt.Sprint("/secret", i))
		RecordBreaker(fmt.Sprint("/secret", i), "open")
		RecordHTTP("GET", fmt.Sprint("/secret", i), 200, .1)
	}
	w := httptest.NewRecorder()
	promhttp.HandlerFor(r.Prometheus, promhttp.HandlerOpts{}).ServeHTTP(
		w,
		httptest.NewRequest("GET", "/metrics", nil),
	)
	body := w.Body.String()
	for _, want := range []string{
		"go_goroutines",
		"go_memstats_heap_alloc_bytes",
		"go_gc_duration_seconds",
		"process_resident_memory_bytes",
		"process_cpu_seconds_total",
		"process_open_fds",
		"http_requests_total",
		`route="other"`,
		`method="other"`,
		`subject="other"`,
		`dependency="other"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %s", want)
		}
	}
	if strings.Contains(body, "secret") || strings.Contains(body, "consumer99") {
		t.Fatal("unbounded label exposed")
	}
	if strings.Count(body, "event_processing_total{") != 1 {
		t.Fatal("unknown subjects/consumers did not collapse")
	}
}

func TestHelpersDisabled(t *testing.T) {
	RecordEvent("x", "y", "error", 1)
	RecordOutbox("x", "error", 1)
	SetOutbox("x", 1, 1)
	RecordCache("x", "hit")
	RecordRetry("x")
	RecordBreaker("x", "open")
	RegisterPoolStats("x", nil)
}
