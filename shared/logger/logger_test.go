package logger

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func TestNew_Production(t *testing.T) {
	logger := New("production", "test-service")
	if logger == nil {
		t.Fatal("expected non-nil logger")
	}

	var buf bytes.Buffer
	handler := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})
	logger = slog.New(handler).With(slog.String("service", "test-service"))

	logger.Info("test message", slog.String("key", "value"))

	output := buf.String()
	if !strings.Contains(output, `"msg":"test message"`) {
		t.Errorf("expected log to contain msg, got: %s", output)
	}
	if !strings.Contains(output, `"service":"test-service"`) {
		t.Errorf("expected log to contain service, got: %s", output)
	}

	var m map[string]any
	if err := json.Unmarshal([]byte(output), &m); err != nil {
		t.Errorf("expected valid JSON, got error: %v\noutput: %s", err, output)
	}
}

func TestNew_Development(t *testing.T) {
	logger := New("development", "test-service")

	if logger == nil {
		t.Fatal("expected non-nil logger")
	}

	logger.Debug("debug message")
}
