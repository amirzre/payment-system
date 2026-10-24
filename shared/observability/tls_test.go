package observability

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type testCA struct {
	cert *x509.Certificate
	key  ed25519.PrivateKey
	pem  []byte
}

func newCA(t *testing.T, name string) testCA {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return testCA{cert, key, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

func leaf(t *testing.T, ca testCA, name string, usage x509.ExtKeyUsage) ([]byte, []byte) {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{usage},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, pub, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
}

func writeCert(t *testing.T, name string, data []byte) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, data, 0600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestMetricsMTLSRejectsMissingAndWrongIdentity(t *testing.T) {
	cleanEnv(t)
	serverCA := newCA(t, "metrics-servers")
	scraperCA := newCA(t, "metrics-scrapers")
	wrongCA := newCA(t, "application-ingest")
	cert, key := leaf(t, serverCA, "metrics", x509.ExtKeyUsageServerAuth)
	t.Setenv("METRICS_ADDR", "127.0.0.1:0")
	t.Setenv("METRICS_TLS_CERT_FILE", writeCert(t, "server.pem", cert))
	t.Setenv("METRICS_TLS_KEY_FILE", writeCert(t, "key.pem", key))
	t.Setenv("METRICS_CLIENT_CA_FILE", writeCert(t, "ca.pem", scraperCA.pem))
	runtime, err := Init(context.Background(), "payment-service")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = runtime.Shutdown(context.Background()) }()
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(serverCA.pem)
	for _, tc := range []struct {
		name string
		ca   *testCA
		ok   bool
	}{{"missing", nil, false}, {"wrong-role", &wrongCA, false}, {"valid-scraper", &scraperCA, true}} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots}
			if tc.ca != nil {
				cert, key := leaf(t, *tc.ca, tc.name, x509.ExtKeyUsageClientAuth)
				pair, err := tls.X509KeyPair(cert, key)
				if err != nil {
					t.Fatal(err)
				}
				cfg.Certificates = []tls.Certificate{pair}
			}
			transport := &http.Transport{TLSClientConfig: cfg}
			defer transport.CloseIdleConnections()
			client := &http.Client{Transport: transport, Timeout: time.Second}
			res, err := client.Get("https://" + runtime.listener.Addr().String() + "/metrics")
			if !tc.ok {
				if err == nil {
					_ = res.Body.Close()
					t.Fatal("unauthorized certificate accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = res.Body.Close() }()
			body, err := io.ReadAll(res.Body)
			if err != nil || !strings.Contains(string(body), "go_goroutines") {
				t.Fatalf("metrics missing runtime: %v", err)
			}
		})
	}
}

func TestBackendOutageDoesNotBlockRequestsAndShutdownIsBounded(t *testing.T) {
	cleanEnv(t)
	ca := newCA(t, "application-ingest")
	cert, key := leaf(t, ca, "app", x509.ExtKeyUsageClientAuth)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "https://127.0.0.1:1")
	t.Setenv("OTEL_EXPORTER_OTLP_CERTIFICATE", writeCert(t, "ca.pem", ca.pem))
	t.Setenv("OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE", writeCert(t, "client.pem", cert))
	t.Setenv("OTEL_EXPORTER_OTLP_CLIENT_KEY", writeCert(t, "client-key.pem", key))
	root, cancel := context.WithCancel(context.Background())
	runtime, err := Init(root, "payment-service")
	if err != nil {
		t.Fatal(err)
	}
	cancel() // application cancellation must not close telemetry before explicit drain.
	start := time.Now()
	for range 3000 {
		_, span := runtime.provider.Tracer("test").Start(context.Background(), "payment.create")
		span.End()
	}
	if time.Since(start) > time.Second {
		t.Fatal("bounded export queue blocked application")
	}
	ctx, stop := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer stop()
	start = time.Now()
	_ = runtime.Shutdown(ctx)
	if time.Since(start) > time.Second {
		t.Fatal("shutdown exceeded caller deadline")
	}
}

type traceRecorder struct {
	tracepb.UnimplementedTraceServiceServer
	requests chan *tracepb.ExportTraceServiceRequest
}

func (r *traceRecorder) Export(_ context.Context, req *tracepb.ExportTraceServiceRequest) (*tracepb.ExportTraceServiceResponse, error) {
	select {
	case r.requests <- req:
	default:
	}
	return &tracepb.ExportTraceServiceResponse{}, nil
}

func TestAuthenticatedOTLPExportSanitizesAutomaticAndCustomAttributes(t *testing.T) {
	cleanEnv(t)
	serverCA := newCA(t, "telemetry-servers")
	appCA := newCA(t, "application-ingest")
	serverCert, serverKey := leaf(t, serverCA, "alloy", x509.ExtKeyUsageServerAuth)
	pair, err := tls.X509KeyPair(serverCert, serverKey)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(appCA.pem)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{pair},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    roots,
	})))
	defer server.Stop()
	recorder := &traceRecorder{requests: make(chan *tracepb.ExportTraceServiceRequest, 4)}
	tracepb.RegisterTraceServiceServer(server, recorder)
	go func() { _ = server.Serve(listener) }()
	cert, key := leaf(t, appCA, "payment-service", x509.ExtKeyUsageClientAuth)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "https://"+listener.Addr().String())
	t.Setenv("OTEL_EXPORTER_OTLP_CERTIFICATE", writeCert(t, "ca.pem", serverCA.pem))
	t.Setenv("OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE", writeCert(t, "app.pem", cert))
	t.Setenv("OTEL_EXPORTER_OTLP_CLIENT_KEY", writeCert(t, "app.key", key))
	root, cancel := context.WithCancel(context.Background())
	runtime, err := Init(root, "payment-service")
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	_, span := runtime.provider.Tracer("test").Start(context.Background(), "payment.create")
	span.SetAttributes(
		attribute.String("url.full", "https://sentinel/password?token=sentinel"),
		attribute.String("db.statement", "sentinel"),
		attribute.String("http.request.header.authorization", "sentinel"),
	)
	span.SetStatus(otelcodes.Error, "sentinel")
	span.AddEvent("sentinel")
	span.End()
	ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	if err = runtime.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case req := <-recorder.requests:
		if strings.Contains(req.String(), "sentinel") {
			t.Fatal("OTLP exporter leaked sensitive telemetry")
		}
		if len(req.ResourceSpans) == 0 || len(req.ResourceSpans[0].ScopeSpans) == 0 {
			t.Fatal("no trace exported")
		}
	case <-time.After(time.Second):
		t.Fatal("authenticated exporter did not send traces")
	}
}
