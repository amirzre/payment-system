package redisclient

import (
	"strings"

	"github.com/redis/go-redis/v9"
)

// Options accepts Redis URLs (including credentials and DB) and legacy host:port addresses.
func Options(address string) (*redis.Options, error) {
	if strings.Contains(address, "://") {
		return redis.ParseURL(address)
	}
	return &redis.Options{Addr: address}, nil
}
