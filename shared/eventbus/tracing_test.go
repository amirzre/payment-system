package eventbus

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/amirzre/payment-system/shared/metrics"
	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func testTracing(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	old := otel.GetTracerProvider()
	oldPropagator := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		otel.SetTracerProvider(old)
		otel.SetTextMapPropagator(oldPropagator)
		_ = provider.Shutdown(context.Background())
	})
	return recorder
}

func TestInsertOutboxPersistsOnlyTraceContext(t *testing.T) {
	db := testDatabase(t)
	testTracing(t)
	member, err := baggage.NewMember("secret", "must-not-propagate")
	if err != nil {
		t.Fatal(err)
	}
	bag, err := baggage.New(member)
	if err != nil {
		t.Fatal(err)
	}
	state, err := trace.ParseTraceState("vendor=opaque")
	if err != nil {
		t.Fatal(err)
	}
	ctx, span := otel.Tracer("eventbus-test").Start(baggage.ContextWithBaggage(context.Background(), bag), "request")
	defer span.End()
	sc := span.SpanContext().WithTraceState(state)
	ctx = trace.ContextWithSpanContext(ctx, sc)
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if err = InsertOutbox(ctx, tx, "test.event", "key", json.RawMessage(`{"amount":9007199254740993}`)); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var stored map[string]string
	var payload string
	if err = db.QueryRow(ctx, `SELECT trace_context,payload FROM outbox`).Scan(&stored, &payload); err != nil {
		t.Fatal(err)
	}
	want := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, want)
	if len(stored) != 2 || stored["traceparent"] != want["traceparent"] || stored["tracestate"] != "vendor=opaque" {
		t.Fatalf("stored context = %#v, want %#v", stored, want)
	}
	if payload != `{"amount":9007199254740993}` {
		t.Fatalf("payload changed: %s", payload)
	}
}

func TestInsertOutboxWithoutValidSpanStoresEmptyContext(t *testing.T) {
	db := testDatabase(t)
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if err = InsertOutbox(ctx, tx, "test.event", "legacy", map[string]bool{"ok": true}); err != nil {
		t.Fatal(err)
	}
	var stored map[string]string
	if err = tx.QueryRow(ctx, `SELECT trace_context FROM outbox`).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if len(stored) != 0 {
		t.Fatalf("invalid context persisted: %#v", stored)
	}
}

func TestOutboxWorkerRestoresTraceWithLiveContext(t *testing.T) {
	db := testDatabase(t)
	recorder := testTracing(t)
	requestCtx, requestSpan := otel.Tracer("eventbus-test").Start(context.Background(), "request")
	requestID := requestSpan.SpanContext()
	requestCtx, cancelRequest := context.WithCancel(requestCtx)
	tx, err := db.Begin(requestCtx)
	if err != nil {
		t.Fatal(err)
	}
	if err = InsertOutbox(requestCtx, tx, "test.event", "durable", map[string]bool{"ok": true}); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(requestCtx); err != nil {
		t.Fatal(err)
	}
	requestSpan.End()
	cancelRequest()
	type contextKey struct{}
	workerCtx := context.WithValue(context.Background(), contextKey{}, "worker-value")
	var delivered trace.SpanContext
	publishOutbox(workerCtx, db, publisherFunc(func(ctx context.Context, _, _ string, _ any) error {
		delivered = trace.SpanContextFromContext(ctx)
		if ctx.Err() != nil || ctx.Value(contextKey{}) != "worker-value" {
			t.Error("publication lost live worker context")
		}
		return nil
	}), testLogger())
	if !delivered.IsValid() || delivered.TraceID() != requestID.TraceID() || delivered.SpanID() == requestID.SpanID() {
		t.Fatalf("producer context = %v, request = %v", delivered, requestID)
	}
	spans := recorder.Ended()
	if len(spans) != 2 || spans[1].SpanKind() != trace.SpanKindProducer || spans[1].Parent().SpanID() != requestID.SpanID() {
		t.Fatalf("expected request -> producer, got %v", spans)
	}
}

func TestOutboxLegacyAndInvalidContextStartIndependentTraces(t *testing.T) {
	for _, stored := range []string{`{}`, `{"traceparent":"invalid","baggage":"secret=value"}`, `{"traceparent":42}`} {
		t.Run(stored, func(t *testing.T) {
			db := testDatabase(t)
			testTracing(t)
			seedOutbox(t, db, "legacy", `{"ok":true}`)
			if _, err := db.Exec(context.Background(), `UPDATE outbox SET trace_context=$1`, stored); err != nil {
				t.Fatal(err)
			}
			workerCtx, workerSpan := otel.Tracer("eventbus-test").Start(context.Background(), "worker")
			defer workerSpan.End()
			var got trace.SpanContext
			publishOutbox(workerCtx, db, publisherFunc(func(ctx context.Context, _, _ string, _ any) error {
				got = trace.SpanContextFromContext(ctx)
				return nil
			}), testLogger())
			if !got.IsValid() || got.TraceID() == workerSpan.SpanContext().TraceID() {
				t.Fatalf("legacy row inherited worker trace: %v", got)
			}
		})
	}
}

func TestNatsTracePropagationAndReplayDeduplication(t *testing.T) {
	db := testDatabase(t)
	bus, subject := testNats(t)
	registry := metrics.New("test-service")
	metrics.Activate(registry)
	t.Cleanup(func() { metrics.Deactivate(registry) })
	recorder := testTracing(t)
	member, err := baggage.NewMember("secret", "must-not-propagate")
	if err != nil {
		t.Fatal(err)
	}
	bag, err := baggage.New(member)
	if err != nil {
		t.Fatal(err)
	}
	requestCtx, requestSpan := otel.Tracer("eventbus-test").Start(baggage.ContextWithBaggage(context.Background(), bag), "request")
	requestID := requestSpan.SpanContext()
	tx, err := db.Begin(requestCtx)
	if err != nil {
		t.Fatal(err)
	}
	if err = InsertOutbox(requestCtx, tx, subject, "durable-key", json.RawMessage(`{"amount":9007199254740993}`)); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(requestCtx); err != nil {
		t.Fatal(err)
	}
	requestSpan.End()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	observed := make(chan trace.SpanContext, 4)
	handle := WithTransactionalIdempotency(db, "trace-consumer", func(ctx context.Context, tx pgx.Tx, key string, payload []byte) error {
		if string(payload) != `{"amount":9007199254740993}` || key != "durable-key" {
			return errors.New("event contract changed")
		}
		_, err := tx.Exec(ctx, `INSERT INTO effects(event_key,value) VALUES($1,1)`, key)
		return err
	})
	if err = bus.Subscribe(ctx, subject, "trace-consumer", func(ctx context.Context, key string, payload []byte) error {
		observed <- trace.SpanContextFromContext(ctx)
		return handle(ctx, key, payload)
	}); err != nil {
		t.Fatal(err)
	}
	publishOutbox(context.Background(), db, bus, testLogger())
	var consumer trace.SpanContext
	select {
	case consumer = <-observed:
	case <-time.After(5 * time.Second):
		t.Fatal("delivery timed out")
	}
	if !consumer.IsValid() || consumer.TraceID() != requestID.TraceID() {
		t.Fatalf("consumer lost request trace: %v", consumer)
	}
	// Replay after the JetStream duplicate window by sending the exact stored
	// event with no message ID. Durable database deduplication must still win.
	stored, err := bus.js.GetMsg(streamNameForSubject(subject), 1)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Header.Get("Baggage") != "" || stored.Header.Get("Traceparent") == "" {
		t.Fatalf("unexpected trace headers: %v", stored.Header)
	}
	replayHeader := stored.Header
	replayHeader.Del(nats.MsgIdHdr)
	if _, err = bus.js.PublishMsg(&nats.Msg{Subject: subject, Data: stored.Data, Header: replayHeader}); err != nil {
		t.Fatal(err)
	}
	select {
	case replay := <-observed:
		if replay.TraceID() != consumer.TraceID() || replay.SpanID() == consumer.SpanID() {
			t.Fatalf("replay context = %v", replay)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("replay timed out")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		info, err := bus.js.ConsumerInfo(streamNameForSubject(subject), "trace-consumer")
		if err != nil {
			t.Fatal(err)
		}
		if info.AckFloor.Stream == 2 {
			endedConsumers := 0
			for _, span := range recorder.Ended() {
				if span.SpanKind() == trace.SpanKindConsumer {
					endedConsumers++
				}
			}
			if endedConsumers == 2 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("replay not acknowledged")
		}
		time.Sleep(10 * time.Millisecond)
	}
	var effects int
	if err = db.QueryRow(ctx, `SELECT count(*) FROM effects`).Scan(&effects); err != nil {
		t.Fatal(err)
	}
	if effects != 1 {
		t.Fatalf("replay produced %d business effects", effects)
	}
	assertMetricValue(t, registry, "event_processing_total", "other", "duplicate", 1)
	assertMetricValue(t, registry, "event_processing_total", "other", "success", 1)
	var producers, consumers int
	var natsProducer trace.SpanContext
	var parents []trace.SpanContext
	for _, s := range recorder.Ended() {
		if s.SpanContext().TraceID() != requestID.TraceID() {
			t.Fatalf("span lost trace: %s", s.Name())
		}
		if s.Name() == "nats publish" {
			natsProducer = s.SpanContext()
		}
		if s.SpanKind() == trace.SpanKindProducer {
			producers++
		}
		if s.SpanKind() == trace.SpanKindConsumer {
			if s.Status().Code == codes.Error {
				t.Fatal("successful or duplicate processing marked as error")
			}
			parents = append(parents, s.Parent())
			consumers++
		}
		for _, a := range s.Attributes() {
			if strings.Contains(a.Value.AsString(), "durable-key") || strings.Contains(a.Value.AsString(), "9007199254740993") {
				t.Fatalf("sensitive attribute: %v", a)
			}
		}
	}
	for _, parent := range parents {
		if !parent.IsRemote() || parent.SpanID() != natsProducer.SpanID() {
			t.Fatalf("consumer parent = %v, producer = %v", parent, natsProducer)
		}
	}
	if producers != 2 || consumers != 2 {
		t.Fatalf("producer=%d consumer=%d", producers, consumers)
	}
}

func TestOutboxTraceContextMigrationsPreserveLegacyRows(t *testing.T) {
	for service, version := range map[string]string{"account": "000004", "ledger": "000004", "payment": "000005"} {
		t.Run(service, func(t *testing.T) {
			db := testDatabase(t)
			ctx := context.Background()
			if _, err := db.Exec(ctx, `ALTER TABLE outbox DROP COLUMN trace_context`); err != nil {
				t.Fatal(err)
			}
			seedOutbox(t, db, "pre-migration", `{"ok":true}`)
			base := "../../services/" + service + "/migrations/" + version + "_outbox_trace_context"
			up, err := os.ReadFile(base + ".up.sql")
			if err != nil {
				t.Fatal(err)
			}
			if _, err = db.Exec(ctx, string(up)); err != nil {
				t.Fatal(err)
			}
			var traceContext map[string]string
			if err = db.QueryRow(ctx, `SELECT trace_context FROM outbox`).Scan(&traceContext); err != nil {
				t.Fatal(err)
			}
			if len(traceContext) != 0 {
				t.Fatalf("legacy context=%v", traceContext)
			}
			seedOutbox(t, db, "post-migration", `{"ok":true}`)
			down, err := os.ReadFile(base + ".down.sql")
			if err != nil {
				t.Fatal(err)
			}
			if _, err = db.Exec(ctx, string(down)); err != nil {
				t.Fatal(err)
			}
			var count int
			if err = db.QueryRow(ctx, `SELECT count(*) FROM outbox`).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != 2 {
				t.Fatalf("migration lost rows: %d", count)
			}
		})
	}
}

func TestOutboxMetricsTrackFailuresBacklogAndRecovery(t *testing.T) {
	db := testDatabase(t)
	registry := metrics.New("test-service")
	metrics.Activate(registry)
	t.Cleanup(func() { metrics.Deactivate(registry) })
	ctx := context.Background()
	if _, err := db.Exec(
		ctx,
		`INSERT INTO outbox(subject,key,payload,created_at) VALUES('ledger.posted','metric-key','{}',now()-interval '1 minute')`,
	); err != nil {
		t.Fatal(err)
	}
	publishOutbox(
		ctx,
		db,
		publisherFunc(func(context.Context, string, string, any) error { return errors.New("offline") }),
		testLogger(),
	)
	assertMetricValue(t, registry, "outbox_pending", "ledger.posted", "", 1)
	assertMetricValue(t, registry, "outbox_publish_total", "ledger.posted", "error", 1)
	families, err := registry.Prometheus.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == "outbox_oldest_pending_age_seconds" {
			for _, metric := range family.Metric {
				for _, label := range metric.Label {
					if label.GetName() == "subject" && label.GetValue() == "ledger.posted" && metric.GetGauge().GetValue() < 59 {
						t.Fatalf("oldest pending age=%v", metric.GetGauge().GetValue())
					}
				}
			}
		}
	}
	publishOutbox(
		ctx,
		db,
		publisherFunc(func(context.Context, string, string, any) error { return nil }),
		testLogger(),
	)
	assertMetricValue(t, registry, "outbox_pending", "ledger.posted", "", 0)
	assertMetricValue(t, registry, "outbox_oldest_pending_age_seconds", "ledger.posted", "", 0)
	assertMetricValue(t, registry, "outbox_publish_total", "ledger.posted", "success", 1)
}

func assertMetricValue(t *testing.T, registry *metrics.Registry, name, subject, outcome string, want float64) {
	t.Helper()
	families, err := registry.Prometheus.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.Metric {
			var gotSubject, gotOutcome string
			for _, label := range metric.Label {
				switch label.GetName() {
				case "subject":
					gotSubject = label.GetValue()
				case "outcome":
					gotOutcome = label.GetValue()
				}
			}
			if gotSubject == subject && gotOutcome == outcome {
				got := metric.GetGauge().GetValue() + metric.GetCounter().GetValue()
				if got != want {
					t.Fatalf("%s{%s,%s}=%v want %v", name, subject, outcome, got, want)
				}
				return
			}
		}
	}
	t.Fatalf("missing metric %s{%s,%s}", name, subject, outcome)
}

// Delivery errors are correlated with their processing span without logging
// event keys, payloads, or arbitrary handler error strings.
func TestNatsInvalidHeadersStartFreshTraceAndSafeCorrelatedErrorLog(t *testing.T) {
	bus, subject := testNats(t)
	recorder := testTracing(t)
	logs := make(chan capturedEventLog, 4)
	bus.log = slog.New(eventLogCapture{logs: logs})
	workerCtx, workerSpan := otel.Tracer("eventbus-test").Start(context.Background(), "worker")
	defer workerSpan.End()
	ctx, cancel := context.WithCancel(workerCtx)
	defer cancel()
	delivered := make(chan trace.SpanContext, 4)
	if err := bus.Subscribe(ctx, subject, "invalid-context", func(ctx context.Context, key string, payload []byte) error {
		if baggage.FromContext(ctx).Len() != 0 {
			t.Error("extracted arbitrary baggage")
		}
		delivered <- trace.SpanContextFromContext(ctx)
		return errors.New("secret-handler-error")
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := bus.js.PublishMsg(&nats.Msg{
		Subject: subject,
		Data:    []byte(`{"secret-payload":true}`),
		Header: nats.Header{
			"Traceparent":   []string{"invalid"},
			"Baggage":       []string{"secret=value"},
			"X-Message-Key": []string{"secret-key"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	var consumer trace.SpanContext
	select {
	case consumer = <-delivered:
	case <-time.After(3 * time.Second):
		t.Fatal("delivery timed out")
	}
	if !consumer.IsValid() || consumer.TraceID() == workerSpan.SpanContext().TraceID() {
		t.Fatalf("invalid header inherited worker trace: %v", consumer)
	}
	select {
	case log := <-logs:
		if log.span.SpanID() != consumer.SpanID() {
			t.Fatalf("log context=%v consumer=%v", log.span, consumer)
		}
		if log.message != "handling event" {
			t.Fatalf("unexpected log: %s", log.message)
		}
		for _, attr := range log.attrs {
			if strings.Contains(attr.Value.String(), "secret") {
				t.Fatalf("leaked attribute: %v", attr)
			}
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no correlated error log")
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		for _, span := range recorder.Ended() {
			if span.SpanContext().SpanID() == consumer.SpanID() {
				if span.Parent().IsValid() {
					t.Fatalf("invalid context got parent: %v", span.Parent())
				}
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("consumer span never ended")
		}
		time.Sleep(time.Millisecond)
	}
}

type capturedEventLog struct {
	span    trace.SpanContext
	message string
	attrs   []slog.Attr
}
type eventLogCapture struct{ logs chan capturedEventLog }

func (h eventLogCapture) Enabled(context.Context, slog.Level) bool { return true }

func (h eventLogCapture) Handle(ctx context.Context, record slog.Record) error {
	log := capturedEventLog{span: trace.SpanContextFromContext(ctx), message: record.Message}
	record.Attrs(func(a slog.Attr) bool { log.attrs = append(log.attrs, a); return true })
	h.logs <- log
	return nil
}

func (h eventLogCapture) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h eventLogCapture) WithGroup(string) slog.Handler { return h }
