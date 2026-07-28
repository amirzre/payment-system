package http

import (
	"encoding/json"
	"net/http"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// errorResponse is the standard error shape for all API responses.
type errorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// writeJSON writes a JSON response with the given status code.
func writeJSON(w http.ResponseWriter, statusCode int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(data)
}

// writeError writes a standard error JSON response.
func writeError(w http.ResponseWriter, httpStatus int, code, message string) {
	writeJSON(w, httpStatus, errorResponse{Code: code, Message: message})
}

// writeGRPCError maps a gRPC error to an HTTP status code and writes the standard error shape.
func writeGRPCError(w http.ResponseWriter, err error) {
	st, ok := status.FromError(err)
	if !ok {
		writeError(w, http.StatusInternalServerError, "INTERNAL", "internal server error")
		return
	}

	httpStatus := http.StatusInternalServerError
	switch st.Code() {
	case codes.NotFound:
		httpStatus = http.StatusNotFound
	case codes.InvalidArgument:
		httpStatus = http.StatusBadRequest
	case codes.AlreadyExists:
		httpStatus = http.StatusConflict
	case codes.FailedPrecondition:
		httpStatus = http.StatusPreconditionFailed
	case codes.Unavailable:
		httpStatus = http.StatusServiceUnavailable
	}

	code := strings.ReplaceAll(st.Code().String(), "_", "_")
	writeError(w, httpStatus, code, st.Message())
}

// extractPathParam extracts the last segment of a path after the given prefix.
func extractPathParam(path, prefix string) string {
	if !strings.HasPrefix(path, prefix) {
		return ""
	}
	return strings.TrimPrefix(path, prefix)
}
