package metrics

import (
	"fmt"
	"log/slog"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server exposes a Prometheus /metrics endpoint on the given port.
// Call Start in a goroutine — it blocks until the server fails.
func Start(logger *slog.Logger, port int) {
	reg := prometheus.NewRegistry()

	// Built-in Go + process metrics (goroutines, memory, GC, CPU)
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	addr := fmt.Sprintf(":%d", port)
	logger.Info("metrics server listening", slog.Int("port", port))

	if err := http.ListenAndServe(addr, mux); err != nil {
		logger.Error("metrics server failed", slog.Any("error", err))
	}
}
