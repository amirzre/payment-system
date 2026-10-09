package redisclient

import "testing"

func TestOptions(t *testing.T) {
	for _, tt := range []struct {
		name, input, addr, username, password string
		db                                    int
		tls                                   bool
	}{
		{name: "authenticated Kubernetes URL", input: "redis://:redis@redis:6379/0", addr: "redis:6379", password: "redis"},
		{name: "database and user", input: "redis://cache:p%40ss@redis:6379/2", addr: "redis:6379", username: "cache", password: "p@ss", db: 2},
		{name: "TLS", input: "rediss://redis:6379/1", addr: "redis:6379", db: 1, tls: true},
		{name: "Compose address", input: "redis:6379", addr: "redis:6379"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			options, err := Options(tt.input)
			if err != nil {
				t.Fatal(err)
			}
			if options.Addr != tt.addr || options.Username != tt.username || options.Password != tt.password || options.DB != tt.db || (options.TLSConfig != nil) != tt.tls {
				t.Fatalf("unexpected options: addr=%q user=%q db=%d tls=%v", options.Addr, options.Username, options.DB, options.TLSConfig != nil)
			}
		})
	}
}

func TestOptionsRejectsInvalidURL(t *testing.T) {
	if _, err := Options("redis://redis:6379/not-a-database"); err == nil {
		t.Fatal("expected invalid Redis URL to be rejected")
	}
}
