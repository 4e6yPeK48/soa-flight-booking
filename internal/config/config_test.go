package config

import (
	"testing"
	"time"
)

func baseEnv(t *testing.T) {
	t.Helper()
	t.Setenv("DATABASE_URL", "postgres://example/db")
	t.Setenv("FLIGHT_API_KEY", "local-test-key")
	t.Setenv("REDIS_SENTINEL_ADDRS", "sentinel-1:26379,sentinel-2:26379")
}

func TestLoadDefaults(t *testing.T) {
	baseEnv(t)
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.RPC.Attempts != 3 || c.RPC.AttemptTimeout != 500*time.Millisecond || c.HTTPTimeout != 8*time.Second || c.OperationTimeout != 10*time.Second || len(c.RedisSentinelAddrs) != 2 {
		t.Fatalf("unexpected defaults: %+v", c)
	}
}

func TestLoadRejectsInvalidConfiguration(t *testing.T) {
	for _, tc := range []struct{ key, value string }{
		{"DATABASE_URL", ""}, {"FLIGHT_API_KEY", ""}, {"RPC_ATTEMPTS", "4"},
		{"RPC_ATTEMPTS", "bad"}, {"RPC_TIMEOUT", "0s"}, {"HTTP_TIMEOUT", "-1s"},
		{"CB_FAILURE_THRESHOLD", "0"}, {"CB_WINDOW", "bad"}, {"OPERATION_TIMEOUT", "1s"},
	} {
		t.Run(tc.key+tc.value, func(t *testing.T) {
			baseEnv(t)
			t.Setenv(tc.key, tc.value)
			if _, err := Load(); err == nil {
				t.Fatal("accepted invalid configuration")
			}
		})
	}
}
