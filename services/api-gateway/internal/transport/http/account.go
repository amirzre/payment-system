package http

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	accountv1 "github.com/amirzre/payment-system/gen/go/account/v1"
)

// AccountHandler proxies REST calls to account-service via gRPC.
type AccountHandler struct {
	client accountv1.AccountServiceClient
}

// NewAccountHandler creates a new AccountHandler.
func NewAccountHandler(client accountv1.AccountServiceClient) *AccountHandler {
	return &AccountHandler{client: client}
}

// CreateAccount handles POST /v1/accounts.
func (h *AccountHandler) CreateAccount(w http.ResponseWriter, r *http.Request) {
	var req struct {
		OwnerRef string `json:"owner_ref"`
		Currency string `json:"currency"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "invalid JSON body")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	resp, err := h.client.CreateAccount(ctx, &accountv1.CreateAccountRequest{
		OwnerRef: req.OwnerRef,
		Currency: req.Currency,
	})
	if err != nil {
		writeGRPCError(w, err)
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"id":         resp.Account.GetId(),
		"owner_ref":  resp.Account.GetOwnerRef(),
		"currency":   resp.Account.GetCurrency(),
		"status":     resp.Account.GetStatus(),
		"created_at": resp.Account.GetCreatedAt(),
	})
}

// GetAccount handles GET /v1/accounts/{id}.
func (h *AccountHandler) GetAccount(w http.ResponseWriter, r *http.Request) {
	id := extractPathParam(r.URL.Path, "/v1/accounts/")
	if id == "" {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "account id is required")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	resp, err := h.client.GetAccount(ctx, &accountv1.GetAccountRequest{Id: id})
	if err != nil {
		writeGRPCError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"id":         resp.Account.GetId(),
		"owner_ref":  resp.Account.GetOwnerRef(),
		"currency":   resp.Account.GetCurrency(),
		"status":     resp.Account.GetStatus(),
		"created_at": resp.Account.GetCreatedAt(),
	})
}
