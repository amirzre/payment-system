package http

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	authv1 "github.com/amirzre/payment-system/gen/go/auth/v1"
	"google.golang.org/grpc/metadata"
)

// AuthHandler proxies REST calls to auth-service via gRPC.
type AuthHandler struct {
	client authv1.AuthServiceClient
}

// NewAuthHandler creates a new AuthHandler.
func NewAuthHandler(client authv1.AuthServiceClient) *AuthHandler {
	return &AuthHandler{client: client}
}

// Register handles POST /v1/auth/register.
func (h *AuthHandler) Register(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "invalid JSON body")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	_, err := h.client.Register(ctx, &authv1.RegisterRequest{
		Email:    req.Email,
		Password: req.Password,
	})
	if err != nil {
		writeGRPCError(w, err)
		return
	}

	writeJSON(w, http.StatusCreated, map[string]string{
		"message": "user registered successfully",
	})
}

// Login handles POST /v1/auth/login.
func (h *AuthHandler) Login(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "invalid JSON body")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	resp, err := h.client.Login(ctx, &authv1.LoginRequest{
		Email:    req.Email,
		Password: req.Password,
	})
	if err != nil {
		writeGRPCError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"access_token":  resp.GetAccessToken(),
		"refresh_token": resp.GetRefreshToken(),
		"expires_in":    resp.GetExpiresIn(),
	})
}

// AuthMiddleware returns an HTTP middleware that verifies the Authorization header
// by calling auth-service.VerifyToken. 
func AuthMiddleware(authClient authv1.AuthServiceClient) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authHeader := r.Header.Get("Authorization")
			if authHeader == "" {
				writeError(w, http.StatusUnauthorized, "UNAUTHORIZED", "missing Authorization header")
				return
			}

			parts := strings.SplitN(authHeader, " ", 2)
			if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
				writeError(w, http.StatusUnauthorized, "UNAUTHORIZED", "invalid Authorization header format")
				return
			}

			token := parts[1]

			ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
			defer cancel()

			resp, err := authClient.VerifyToken(ctx, &authv1.VerifyTokenRequest{Token: token})
			if err != nil || !resp.GetValid() {
				writeError(w, http.StatusUnauthorized, "UNAUTHORIZED", "invalid or expired token")
				return
			}

			ctx = metadata.AppendToOutgoingContext(r.Context(),
				"x-user-id", resp.GetUserId(),
			)
			r = r.WithContext(ctx)

			next.ServeHTTP(w, r)
		})
	}
}
