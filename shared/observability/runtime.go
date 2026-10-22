// Package observability initializes bounded metrics and authenticated telemetry.
package observability

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/amirzre/payment-system/shared/metrics"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc/credentials"
)

type Runtime struct {
	Registry    *metrics.Registry
	provider    *sdktrace.TracerProvider
	server      *http.Server
	listener    net.Listener
	once        sync.Once
	shutdownErr error
}

// Init leaves disabled telemetry usable without any certificate files. An
// enabled listener/exporter fails closed on invalid security configuration.
// ctx only bounds initialization; callers drain application work before Shutdown.
func Init(ctx context.Context, service string) (*Runtime, error) {
	switch service {
	case "api-gateway", "account-service", "auth-service", "ledger-service", "payment-service":
	default:
		return nil, errors.New("unknown telemetry service")
	}
	addr := os.Getenv("METRICS_ADDR")
	var metricsTLS *tls.Config
	var err error
	if addr != "" {
		metricsTLS, err = metricsTLSConfig()
		if err != nil {
			return nil, err
		}
	}
	endpoint := envTrace("ENDPOINT")
	var exporter sdktrace.SpanExporter
	if endpoint != "" {
		opts, e := exportOptions(endpoint)
		if e != nil {
			return nil, e
		}
		exporter, e = otlptracegrpc.New(ctx, opts...)
		if e != nil {
			return nil, errors.New("initialize authenticated OTLP exporter")
		}
	}
	sampler, err := configuredSampler()
	if err != nil {
		if exporter != nil {
			_ = exporter.Shutdown(ctx)
		}
		return nil, err
	}
	attrs := []attribute.KeyValue{attribute.String("service.name", service)}
	for _, pair := range []struct{ env, key string }{
		{"SERVICE_VERSION", "service.version"},
		{"APP_ENV", "deployment.environment.name"},
		{"POD_NAME", "k8s.pod.name"},
		{"POD_NAMESPACE", "k8s.namespace.name"},
	} {
		if v := os.Getenv(pair.env); v != "" {
			attrs = append(attrs, attribute.String(pair.key, v))
		}
	}
	options := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(resource.NewSchemaless(attrs...)),
		sdktrace.WithSampler(sampler),
		sdktrace.WithRawSpanLimits(sdktrace.SpanLimits{
			AttributeValueLengthLimit:   256,
			AttributeCountLimit:         32,
			EventCountLimit:             0,
			LinkCountLimit:              8,
			AttributePerEventCountLimit: 0,
			AttributePerLinkCountLimit:  0,
		}),
	}
	if exporter != nil {
		options = append(
			options,
			sdktrace.WithBatcher(
				safeExporter{exporter},
				sdktrace.WithMaxQueueSize(512),
				sdktrace.WithMaxExportBatchSize(128),
				sdktrace.WithBatchTimeout(time.Second),
				sdktrace.WithExportTimeout(3*time.Second),
			),
		)
	}
	r := &Runtime{Registry: metrics.New(service), provider: sdktrace.NewTracerProvider(options...)}
	if addr != "" {
		listener, e := net.Listen("tcp", addr)
		if e != nil {
			_ = r.provider.Shutdown(ctx)
			return nil, errors.New("listen on metrics address")
		}
		r.listener = tls.NewListener(listener, metricsTLS)
		mux := http.NewServeMux()
		mux.Handle(
			"GET /metrics",
			promhttp.HandlerFor(
				r.Registry.Prometheus,
				promhttp.HandlerOpts{Timeout: 5 * time.Second, MaxRequestsInFlight: 4},
			),
		)
		r.server = &http.Server{
			Handler:           mux,
			ReadHeaderTimeout: 3 * time.Second,
			ReadTimeout:       5 * time.Second,
			WriteTimeout:      10 * time.Second,
			IdleTimeout:       30 * time.Second,
			MaxHeaderBytes:    8192,
		}
		go func() { _ = r.server.Serve(r.listener) }()
	}
	otel.SetTracerProvider(r.provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	metrics.Activate(r.Registry)
	return r, nil
}

// Shutdown caps cleanup even if the caller supplies an unbounded context.
func (r *Runtime) Shutdown(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.once.Do(func() {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		metrics.Deactivate(r.Registry)
		var serverErr error
		if r.server != nil {
			serverErr = r.server.Shutdown(ctx)
			if serverErr != nil {
				_ = r.server.Close()
			}
		}
		r.shutdownErr = errors.Join(serverErr, r.provider.Shutdown(ctx))
	})
	return r.shutdownErr
}

func metricsTLSConfig() (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(os.Getenv("METRICS_TLS_CERT_FILE"), os.Getenv("METRICS_TLS_KEY_FILE"))
	if err != nil {
		return nil, errors.New("metrics requires a valid server certificate and key")
	}
	ca, err := readCA(os.Getenv("METRICS_CLIENT_CA_FILE"))
	if err != nil {
		return nil, errors.New("metrics requires a valid scraper client CA")
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    ca,
	}, nil
}

func readCA(path string) (*x509.CertPool, error) {
	if path == "" {
		return nil, errors.New("missing CA file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.New("read CA certificate")
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(data) {
		return nil, errors.New("invalid CA certificate")
	}
	return pool, nil
}

func envTrace(suffix string) string {
	if v, ok := os.LookupEnv("OTEL_EXPORTER_OTLP_TRACES_" + suffix); ok && v != "" {
		return v
	}
	return os.Getenv("OTEL_EXPORTER_OTLP_" + suffix)
}

func exportOptions(endpoint string) ([]otlptracegrpc.Option, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, errors.New("OTLP endpoint must be an HTTPS gRPC authority without credentials, path or query")
	}
	for _, key := range []string{"OTEL_EXPORTER_OTLP_INSECURE", "OTEL_EXPORTER_OTLP_TRACES_INSECURE"} {
		if v := os.Getenv(key); v != "" {
			insecure, e := strconv.ParseBool(v)
			if e != nil || insecure {
				return nil, errors.New("insecure OTLP export is prohibited")
			}
		}
	}
	if p := envTrace("PROTOCOL"); p != "" && p != "grpc" {
		return nil, errors.New("OTLP traces require grpc protocol")
	}
	ca, err := readCA(envTrace("CERTIFICATE"))
	if err != nil {
		return nil, errors.New("OTLP requires a valid server CA certificate")
	}
	cert, err := tls.LoadX509KeyPair(envTrace("CLIENT_CERTIFICATE"), envTrace("CLIENT_KEY"))
	if err != nil {
		return nil, errors.New("OTLP requires a valid client certificate and key")
	}
	tlsCfg := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		RootCAs:      ca,
		Certificates: []tls.Certificate{cert},
		ServerName:   u.Hostname(),
	}
	return []otlptracegrpc.Option{
		otlptracegrpc.WithEndpointURL(endpoint),
		otlptracegrpc.WithTLSCredentials(credentials.NewTLS(tlsCfg)),
		otlptracegrpc.WithTimeout(3 * time.Second),
		otlptracegrpc.WithRetry(otlptracegrpc.RetryConfig{
			Enabled:         true,
			InitialInterval: time.Second,
			MaxInterval:     2 * time.Second,
			MaxElapsedTime:  3 * time.Second,
		}),
	}, nil
}

func configuredSampler() (sdktrace.Sampler, error) {
	ratio := func() (sdktrace.Sampler, error) {
		v, e := strconv.ParseFloat(os.Getenv("OTEL_TRACES_SAMPLER_ARG"), 64)
		if e != nil || math.IsNaN(v) || math.IsInf(v, 0) || v < 0 || v > 1 {
			return nil, errors.New("invalid OTEL_TRACES_SAMPLER_ARG")
		}
		return sdktrace.TraceIDRatioBased(v), nil
	}
	switch os.Getenv("OTEL_TRACES_SAMPLER") {
	case "", "parentbased_always_on":
		return sdktrace.ParentBased(sdktrace.AlwaysSample()), nil
	case "always_on":
		return sdktrace.AlwaysSample(), nil
	case "always_off":
		return sdktrace.NeverSample(), nil
	case "parentbased_always_off":
		return sdktrace.ParentBased(sdktrace.NeverSample()), nil
	case "traceidratio":
		return ratio()
	case "parentbased_traceidratio":
		s, e := ratio()
		if e != nil {
			return nil, e
		}
		return sdktrace.ParentBased(s), nil
	default:
		return nil, fmt.Errorf("unsupported OTEL_TRACES_SAMPLER")
	}
}
