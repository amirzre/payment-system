package app

import (
	"context"
	"strings"
	"testing"

	"github.com/amirzre/payment-system/services/payment/internal/repository"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestPaymentOperationSpanDoesNotContainPaymentData(t *testing.T) {
	pool := paymentAppDB(t)
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() { otel.SetTracerProvider(previous); _ = provider.Shutdown(context.Background()) })
	service := NewPaymentService(
		repository.NewPaymentRepository(pool),
		accountClientFixture{},
		&ledgerClientFixture{committed: map[string]bool{}},
		nil,
	)
	secret := "private-idempotency-" + uuid.NewString()
	input := CreatePaymentInput{secret, uuid.New(), uuid.New(), 250, "USD"}
	if _, err := service.Create(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, span := range exporter.GetSpans() {
		if span.Name == "payment.create" {
			found = true
		}
		for _, attribute := range span.Attributes {
			value := attribute.Value.String()
			if strings.Contains(value, secret) || strings.Contains(value, input.SourceAccountID.String()) || strings.Contains(value, input.DestinationAccountID.String()) {
				t.Fatalf("payment data exposed in span attribute %s", attribute.Key)
			}
		}
	}
	if !found {
		t.Fatal("missing payment.create operation span")
	}
}
