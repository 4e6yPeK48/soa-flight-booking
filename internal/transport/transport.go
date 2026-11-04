// Package transport implements authentication and bounded, circuit-protected gRPC calls.
package transport

import (
	"context"
	"crypto/subtle"
	"errors"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type Config struct {
	APIKey           string
	Attempts         int
	Backoff          time.Duration
	AttemptTimeout   time.Duration
	RPCTimeout       time.Duration
	FailureThreshold int
	Window           time.Duration
	OpenTimeout      time.Duration
}

func DefaultConfig() Config {
	return Config{Attempts: 3, Backoff: 100 * time.Millisecond, AttemptTimeout: 500 * time.Millisecond, RPCTimeout: 2 * time.Second, FailureThreshold: 5, Window: 60 * time.Second, OpenTimeout: 15 * time.Second}
}

func (c Config) Validate() error {
	if c.APIKey == "" {
		return errors.New("gRPC API key must not be empty")
	}
	if c.Attempts < 1 || c.Attempts > 3 {
		return errors.New("gRPC attempts must be between 1 and 3")
	}
	if c.Backoff <= 0 || c.AttemptTimeout <= 0 || c.RPCTimeout <= 0 || c.FailureThreshold < 1 || c.Window <= 0 || c.OpenTimeout <= 0 {
		return errors.New("gRPC timeouts, backoff, failure threshold and window must be positive")
	}
	return nil
}

// NewClientInterceptor shares one breaker per target across methods. Call Validate at startup.
func NewClientInterceptor(cfg Config, logger *slog.Logger) grpc.UnaryClientInterceptor {
	if logger == nil {
		logger = slog.Default()
	}
	var mu sync.Mutex
	breakers := make(map[string]*breaker)
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if err := ctx.Err(); err != nil {
			return status.FromContextError(err).Err()
		}
		ctx, cancel := context.WithTimeout(ctx, cfg.RPCTimeout)
		defer cancel()
		target := cc.Target()
		mu.Lock()
		b := breakers[target]
		if b == nil {
			b = &breaker{cfg: cfg, logger: logger, target: target, state: "CLOSED"}
			breakers[target] = b
		}
		mu.Unlock()
		generation, probe, err := b.admit()
		if err != nil {
			return err
		}
		attempts := min(3, max(1, cfg.Attempts))
		if probe {
			attempts = 1
		}
		md, _ := metadata.FromOutgoingContext(ctx)
		md = md.Copy()
		md.Set("x-api-key", cfg.APIKey)
		ctx = metadata.NewOutgoingContext(ctx, md)
		for attempt := 0; attempt < attempts; attempt++ {
			if err = ctx.Err(); err != nil {
				err = status.FromContextError(err).Err()
				break
			}
			if attempt > 0 {
				logger.Info("retrying gRPC call", "method", method, "attempt", attempt+1)
			}
			attemptCtx, attemptCancel := context.WithTimeout(ctx, cfg.AttemptTimeout)
			err = invoker(attemptCtx, method, req, reply, cc, opts...)
			if contextErr := attemptCtx.Err(); contextErr != nil {
				err = status.FromContextError(contextErr).Err()
			}
			attemptCancel()
			if !infrastructure(err) || attempt+1 == attempts {
				break
			}
			timer := time.NewTimer(cfg.Backoff * time.Duration(1<<attempt))
			select {
			case <-ctx.Done():
				timer.Stop()
				err = status.FromContextError(ctx.Err()).Err()
			case <-timer.C:
				continue
			}
			break
		}
		b.complete(generation, err, status.Code(err) == codes.Canceled && ctx.Err() == context.Canceled)
		return err
	}
}

func ServerAuth(apiKey string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		keys := md.Get("x-api-key")
		if len(keys) != 1 || apiKey == "" || subtle.ConstantTimeCompare([]byte(keys[0]), []byte(apiKey)) != 1 {
			return nil, status.Error(codes.Unauthenticated, "invalid service credentials")
		}
		return handler(ctx, req)
	}
}

func infrastructure(err error) bool {
	code := status.Code(err)
	return code == codes.Unavailable || code == codes.DeadlineExceeded
}

type breaker struct {
	mu          sync.Mutex
	cfg         Config
	logger      *slog.Logger
	target      string
	state       string
	generation  uint64
	failures    int
	windowStart time.Time
	openedAt    time.Time
}

func (b *breaker) transition(next string) {
	previous := b.state
	b.state = next
	b.generation++
	b.logger.Info("gRPC circuit breaker transition", "target", b.target, "from", previous, "to", next)
}

func (b *breaker) admit() (uint64, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == "OPEN" {
		if time.Since(b.openedAt) < b.cfg.OpenTimeout {
			return 0, false, status.Error(codes.Unavailable, "flight service circuit breaker is open")
		}
		b.transition("HALF_OPEN")
		return b.generation, true, nil
	}
	if b.state == "HALF_OPEN" {
		return 0, false, status.Error(codes.Unavailable, "flight service circuit breaker probe is in progress")
	}
	return b.generation, false, nil
}

func (b *breaker) complete(generation uint64, err error, locallyCanceled bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.generation != generation {
		return
	}
	if b.state == "HALF_OPEN" {
		if infrastructure(err) || locallyCanceled {
			b.openedAt = time.Now()
			b.transition("OPEN")
		} else {
			b.failures = 0
			b.windowStart = time.Time{}
			b.transition("CLOSED")
		}
		return
	}
	// Local cancellation provides no evidence about service connectivity.
	if locallyCanceled {
		return
	}
	// Success and business responses prove connectivity and reset consecutive failures.
	if !infrastructure(err) {
		b.failures = 0
		b.windowStart = time.Time{}
		return
	}
	now := time.Now()
	if b.windowStart.IsZero() || now.Sub(b.windowStart) >= b.cfg.Window {
		b.failures = 0
		b.windowStart = now
	}
	b.failures++
	if b.failures >= b.cfg.FailureThreshold {
		b.openedAt = now
		b.transition("OPEN")
	}
}
