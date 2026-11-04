package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/mchupryna/soa-flight-booking/internal/transport"
)

type Config struct {
	DatabaseURL, APIKey, GRPCAddr, HTTPAddr, HealthAddr, FlightAddr, RedisMasterName string
	RedisSentinelAddrs                                                               []string
	RPC                                                                              transport.Config
	HTTPTimeout, OperationTimeout, RecoveryInterval                                  time.Duration
}

func Load() (Config, error) {
	c := Config{DatabaseURL: os.Getenv("DATABASE_URL"), APIKey: os.Getenv("FLIGHT_API_KEY"), GRPCAddr: env("GRPC_ADDR", ":50051"), HTTPAddr: env("HTTP_ADDR", ":8080"), HealthAddr: env("HEALTH_ADDR", ":9090"), FlightAddr: env("FLIGHT_ADDR", "flight:50051"), RedisMasterName: env("REDIS_MASTER_NAME", "flights"), RPC: transport.DefaultConfig(), HTTPTimeout: 8 * time.Second, OperationTimeout: 10 * time.Second, RecoveryInterval: time.Second}
	if c.DatabaseURL == "" || c.APIKey == "" {
		return Config{}, errors.New("DATABASE_URL and FLIGHT_API_KEY are required")
	}
	for _, addr := range strings.Split(os.Getenv("REDIS_SENTINEL_ADDRS"), ",") {
		if addr = strings.TrimSpace(addr); addr != "" {
			c.RedisSentinelAddrs = append(c.RedisSentinelAddrs, addr)
		}
	}
	c.RPC.APIKey = c.APIKey
	for name, dst := range map[string]*time.Duration{"RPC_BACKOFF": &c.RPC.Backoff, "RPC_ATTEMPT_TIMEOUT": &c.RPC.AttemptTimeout, "RPC_TIMEOUT": &c.RPC.RPCTimeout, "CB_WINDOW": &c.RPC.Window, "CB_OPEN_TIMEOUT": &c.RPC.OpenTimeout, "HTTP_TIMEOUT": &c.HTTPTimeout, "OPERATION_TIMEOUT": &c.OperationTimeout, "RECOVERY_INTERVAL": &c.RecoveryInterval} {
		if value := os.Getenv(name); value != "" {
			parsed, err := time.ParseDuration(value)
			if err != nil || parsed <= 0 {
				return Config{}, fmt.Errorf("%s must be a positive duration", name)
			}
			*dst = parsed
		}
	}
	for name, dst := range map[string]*int{"RPC_ATTEMPTS": &c.RPC.Attempts, "CB_FAILURE_THRESHOLD": &c.RPC.FailureThreshold} {
		if value := os.Getenv(name); value != "" {
			parsed, err := strconv.Atoi(value)
			if err != nil {
				return Config{}, fmt.Errorf("%s must be an integer", name)
			}
			*dst = parsed
		}
	}
	if err := c.RPC.Validate(); err != nil {
		return Config{}, err
	}
	if c.OperationTimeout <= c.HTTPTimeout {
		return Config{}, errors.New("OPERATION_TIMEOUT must exceed HTTP_TIMEOUT")
	}
	return c, nil
}

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
