package logger

import (
	"log/slog"
	"os"
)

// New returns a *slog.Logger configured for the given environment.
func New(env, serviceName string) *slog.Logger {
	var handler slog.Handler

	opts := &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}

	if env == "production" {
		opts.Level = slog.LevelInfo
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}

	return slog.New(ContextHandler(handler)).With(slog.String("service", serviceName))
}
