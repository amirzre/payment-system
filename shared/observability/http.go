package observability

import (
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/amirzre/payment-system/shared/metrics"
	"github.com/google/uuid"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// HTTPHandler traces HTTP calls without capturing headers, bodies or raw URLs.
// Route templates remain bounded even when authentication short-circuits a nested mux.
func HTTPHandler(log *slog.Logger, next http.Handler) http.Handler {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		route := routeFor(r)
		span := trace.SpanFromContext(r.Context())
		span.SetName(safeHTTPMethod(r.Method) + " " + route)
		span.SetAttributes(attribute.String("http.route", route))
		requestID := r.Header.Get("X-Request-ID")
		if !validRequestID(requestID) {
			requestID = uuid.NewString()
		}
		w.Header().Set("X-Request-ID", requestID)
		if sc := span.SpanContext(); sc.IsValid() {
			w.Header().Set("X-Trace-ID", sc.TraceID().String())
		}
		rw := &responseWriter{ResponseWriter: w, status: 200}
		done := metrics.HTTPInFlight()
		defer done()
		defer func() {
			if recover() != nil {
				if !rw.wrote {
					rw.WriteHeader(http.StatusInternalServerError)
				}
				span.SetAttributes(attribute.Bool("error", true))
				if log != nil {
					log.ErrorContext(r.Context(), "HTTP handler panic recovered", slog.String("route", route))
				}
			}
			// ServeMux writes the matched pattern on the request passed into the handler.
			if p := patternRoute(r.Pattern); p != "other" {
				route = p
			}
			span.SetName(safeHTTPMethod(r.Method) + " " + route)
			span.SetAttributes(attribute.String("http.route", route))
			metrics.RecordHTTP(r.Method, route, rw.status, time.Since(start).Seconds())
			if log != nil {
				log.InfoContext(
					r.Context(),
					"HTTP request",
					slog.String("method", safeHTTPMethod(r.Method)),
					slog.String("route", route),
					slog.Int("status", rw.status),
					slog.Float64("duration_seconds", time.Since(start).Seconds()),
					slog.String("request_id", requestID),
				)
			}
		}()
		next.ServeHTTP(rw, r)
	})
	traced := otelhttp.NewHandler(
		inner,
		"HTTP request",
		otelhttp.WithPropagators(propagation.TraceContext{}),
		otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string { return safeHTTPMethod(r.Method) + " " + routeFor(r) }),
	)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/healthz", "/readyz", "/metrics":
			next.ServeHTTP(w, r)
		default:
			traced.ServeHTTP(w, r)
		}
	})
}

func validRequestID(v string) bool {
	if len(v) < 1 || len(v) > 64 {
		return false
	}
	for _, r := range v {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_') {
			return false
		}
	}
	return true
}

func safeHTTPMethod(v string) string {
	switch v {
	case "GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD":
		return v
	}
	return "other"
}

func patternRoute(pattern string) string {
	if i := strings.IndexByte(pattern, ' '); i >= 0 {
		pattern = pattern[i+1:]
	}
	return metrics.Route(pattern)
}

func routeFor(r *http.Request) string {
	p := r.URL.Path
	switch {
	case r.Method == "POST" && (p == "/v1/auth/register" || p == "/v1/auth/login" || p == "/v1/accounts" || p == "/v1/payments"):
		return p
	case r.Method == "GET" && strings.HasPrefix(p, "/v1/accounts/") && len(strings.Split(p, "/")) == 4 && strings.Split(p, "/")[3] != "":
		return "/v1/accounts/{id}"
	case r.Method == "GET" && strings.HasPrefix(p, "/v1/payments/") && len(strings.Split(p, "/")) == 4 && strings.Split(p, "/")[3] != "":
		return "/v1/payments/{id}"
	case r.Method == "POST" && strings.HasPrefix(p, "/v1/accounts/") && strings.HasSuffix(p, "/deposit") && len(strings.Split(p, "/")) == 5 && strings.Split(p, "/")[3] != "":
		return "/v1/accounts/{id}/deposit"
	}
	return "other"
}

type responseWriter struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (w *responseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *responseWriter) WriteHeader(code int) {
	if !w.wrote {
		w.status = code
		w.wrote = true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *responseWriter) Write(b []byte) (int, error) {
	if !w.wrote {
		w.WriteHeader(200)
	}
	return w.ResponseWriter.Write(b)
}
