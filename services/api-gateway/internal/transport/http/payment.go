package http

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	paymentv1 "github.com/amirzre/payment-system/gen/go/payment/v1"
)

// PaymentHandler proxies REST calls to payment-service via gRPC.
type PaymentHandler struct {
	client paymentv1.PaymentServiceClient
}

// NewPaymentHandler creates a new PaymentHandler.
func NewPaymentHandler(client paymentv1.PaymentServiceClient) *PaymentHandler {
	return &PaymentHandler{client: client}
}

// CreatePayment handles POST /v1/payments.
func (h *PaymentHandler) CreatePayment(w http.ResponseWriter, r *http.Request) {
	var req struct {
		IdempotencyKey       string `json:"idempotency_key"`
		SourceAccountID      string `json:"source_account_id"`
		DestinationAccountID string `json:"destination_account_id"`
		AmountMinor          int64  `json:"amount_minor"`
		Currency             string `json:"currency"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "invalid JSON body")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	resp, err := h.client.CreatePayment(ctx, &paymentv1.CreatePaymentRequest{
		IdempotencyKey:       req.IdempotencyKey,
		SourceAccountId:      req.SourceAccountID,
		DestinationAccountId: req.DestinationAccountID,
		AmountMinor:          req.AmountMinor,
		Currency:             req.Currency,
	})
	if err != nil {
		writeGRPCError(w, err)
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"id":                     resp.Payment.GetId(),
		"idempotency_key":        resp.Payment.GetIdempotencyKey(),
		"source_account_id":      resp.Payment.GetSourceAccountId(),
		"destination_account_id": resp.Payment.GetDestinationAccountId(),
		"amount_minor":           resp.Payment.GetAmountMinor(),
		"currency":               resp.Payment.GetCurrency(),
		"status":                 resp.Payment.GetStatus(),
		"created_at":             resp.Payment.GetCreatedAt(),
		"updated_at":             resp.Payment.GetUpdatedAt(),
	})
}

// GetPayment handles GET /v1/payments/{id}.
func (h *PaymentHandler) GetPayment(w http.ResponseWriter, r *http.Request) {
	id := extractPathParam(r.URL.Path, "/v1/payments/")
	if id == "" {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "payment id is required")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	resp, err := h.client.GetPayment(ctx, &paymentv1.GetPaymentRequest{Id: id})
	if err != nil {
		writeGRPCError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"id":                     resp.Payment.GetId(),
		"idempotency_key":        resp.Payment.GetIdempotencyKey(),
		"source_account_id":      resp.Payment.GetSourceAccountId(),
		"destination_account_id": resp.Payment.GetDestinationAccountId(),
		"amount_minor":           resp.Payment.GetAmountMinor(),
		"currency":               resp.Payment.GetCurrency(),
		"status":                 resp.Payment.GetStatus(),
		"created_at":             resp.Payment.GetCreatedAt(),
		"updated_at":             resp.Payment.GetUpdatedAt(),
	})
}
