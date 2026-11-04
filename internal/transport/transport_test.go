package transport

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func testClient(t *testing.T) (*grpc.ClientConn, *slog.Logger) {
	t.Helper()
	cc, err := grpc.NewClient("passthrough:///flight", grpc.WithInsecure())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	return cc, slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestRetryMetadataAndBudget(t *testing.T) {
	cc, logger := testClient(t)
	synctest.Test(t, func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.APIKey = "secret"
		interceptor := NewClientInterceptor(cfg, logger)
		started := time.Now()
		var elapsed []time.Duration
		invoker := func(ctx context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
			elapsed = append(elapsed, time.Since(started))
			md, _ := metadata.FromOutgoingContext(ctx)
			if got := md.Get("x-api-key"); !slices.Equal(got, []string{"secret"}) {
				t.Errorf("auth = %v", got)
			}
			if got := md.Get("trace-id"); !slices.Equal(got, []string{"trace"}) {
				t.Errorf("trace = %v", got)
			}
			deadline, ok := ctx.Deadline()
			if !ok || deadline.Sub(time.Now()) != 500*time.Millisecond {
				t.Errorf("attempt deadline %v", deadline)
			}
			return status.Error(codes.Unavailable, "down")
		}
		ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("x-api-key", "old", "trace-id", "trace"))
		err := interceptor(ctx, "/flight/GetFlight", nil, nil, cc, invoker)
		if status.Code(err) != codes.Unavailable {
			t.Fatal(err)
		}
		if !slices.Equal(elapsed, []time.Duration{0, 100 * time.Millisecond, 300 * time.Millisecond}) {
			t.Fatalf("attempt timing = %v", elapsed)
		}
	})
}

func TestRetryOnlyInfrastructureErrors(t *testing.T) {
	cc, logger := testClient(t)
	for _, code := range []codes.Code{codes.InvalidArgument, codes.NotFound, codes.ResourceExhausted, codes.Unauthenticated, codes.FailedPrecondition, codes.Canceled, codes.Internal, codes.Unavailable, codes.DeadlineExceeded, codes.OK} {
		t.Run(code.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg := DefaultConfig()
				cfg.APIKey = "key"
				calls := 0
				err := NewClientInterceptor(cfg, logger)(context.Background(), "/flight/GetFlight", nil, nil, cc, func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
					calls++
					return status.Error(code, "result")
				})
				want := 1
				if code == codes.Unavailable || code == codes.DeadlineExceeded {
					want = 3
				}
				if calls != want || status.Code(err) != code {
					t.Fatalf("calls=%d err=%v", calls, err)
				}
			})
		})
	}
}

func TestDeadlinesAndCancellation(t *testing.T) {
	cc, logger := testClient(t)
	t.Run("attempt and overall deadline", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.APIKey = "key"
			cfg.RPCTimeout = 900 * time.Millisecond
			calls := 0
			started := time.Now()
			err := NewClientInterceptor(cfg, logger)(context.Background(), "/flight/GetFlight", nil, nil, cc, func(ctx context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
				calls++
				<-ctx.Done()
				return ctx.Err()
			})
			if status.Code(err) != codes.DeadlineExceeded || calls != 2 || time.Since(started) != 900*time.Millisecond {
				t.Fatalf("calls=%d elapsed=%v err=%v", calls, time.Since(started), err)
			}
		})
	})
	t.Run("parent earlier deadline", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.APIKey = "key"
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			calls := 0
			started := time.Now()
			err := NewClientInterceptor(cfg, logger)(ctx, "/flight/GetFlight", nil, nil, cc, func(ctx context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
				calls++
				<-ctx.Done()
				return ctx.Err()
			})
			if status.Code(err) != codes.DeadlineExceeded || calls != 1 || time.Since(started) != 50*time.Millisecond {
				t.Fatalf("calls=%d elapsed=%v err=%v", calls, time.Since(started), err)
			}
		})
	})
	t.Run("cancellation interrupts backoff", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.APIKey = "key"
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			timer := time.AfterFunc(50*time.Millisecond, cancel)
			defer timer.Stop()
			calls := 0
			started := time.Now()
			err := NewClientInterceptor(cfg, logger)(ctx, "/flight/GetFlight", nil, nil, cc, func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
				calls++
				return status.Error(codes.Unavailable, "down")
			})
			if status.Code(err) != codes.Canceled || calls != 1 || time.Since(started) != 50*time.Millisecond {
				t.Fatalf("calls=%d elapsed=%v err=%v", calls, time.Since(started), err)
			}
		})
	})
	t.Run("already cancelled makes no attempt", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.APIKey = "key"
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := NewClientInterceptor(cfg, logger)(ctx, "/flight/GetFlight", nil, nil, cc, func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
			t.Fatal("unexpected attempt")
			return nil
		})
		if status.Code(err) != codes.Canceled {
			t.Fatal(err)
		}
	})
}

func TestBreakerCountsLogicalCallsAndResetsWindow(t *testing.T) {
	cc, logger := testClient(t)
	synctest.Test(t, func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.APIKey = "key"
		interceptor := NewClientInterceptor(cfg, logger)
		calls := 0
		invoke := func(code codes.Code) error {
			return interceptor(context.Background(), "/flight/GetFlight", nil, nil, cc, func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
				calls++
				return status.Error(code, "result")
			})
		}
		for range 4 {
			_ = invoke(codes.Unavailable)
		}
		time.Sleep(60 * time.Second)
		_ = invoke(codes.Unavailable)
		if calls != 15 {
			t.Fatalf("window did not reset: %d attempts", calls)
		}
		for range 4 {
			_ = invoke(codes.Unavailable)
		}
		before := calls
		err := invoke(codes.OK)
		if status.Code(err) != codes.Unavailable || calls != before {
			t.Fatalf("open breaker called network: calls=%d err=%v", calls, err)
		}
	})
}

func TestBreakerBusinessResponseResetsFailures(t *testing.T) {
	cc, logger := testClient(t)
	synctest.Test(t, func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.APIKey = "key"
		interceptor := NewClientInterceptor(cfg, logger)
		invoke := func(code codes.Code) error {
			return interceptor(context.Background(), "/flight/GetFlight", nil, nil, cc, func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
				return status.Error(code, "result")
			})
		}
		for range 4 {
			_ = invoke(codes.Unavailable)
		}
		if status.Code(invoke(codes.NotFound)) != codes.NotFound {
			t.Fatal("business error blocked")
		}
		for range 4 {
			_ = invoke(codes.Unavailable)
		}
		if err := invoke(codes.OK); err != nil {
			t.Fatal(err)
		}
	})
}

func TestHalfOpenSingleWireProbeAndRecovery(t *testing.T) {
	cc, logger := testClient(t)
	synctest.Test(t, func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.APIKey = "key"
		cfg.FailureThreshold = 1
		interceptor := NewClientInterceptor(cfg, logger)
		var calls atomic.Int32
		down := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
			calls.Add(1)
			return status.Error(codes.Unavailable, "down")
		}
		invoke := func(fn grpc.UnaryInvoker) error {
			return interceptor(context.Background(), "/flight/GetFlight", nil, nil, cc, fn)
		}
		_ = invoke(down)
		time.Sleep(15 * time.Second)
		entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
		go func() {
			done <- invoke(func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
				calls.Add(1)
				close(entered)
				<-release
				return status.Error(codes.Unavailable, "probe failed")
			})
		}()
		<-entered
		for range 10 {
			if status.Code(invoke(down)) != codes.Unavailable {
				t.Error("concurrent probe accepted")
			}
		}
		if calls.Load() != 4 {
			t.Errorf("concurrent wire calls=%d", calls.Load())
		}
		close(release)
		if status.Code(<-done) != codes.Unavailable {
			t.Fatal("probe failure lost")
		}
		if calls.Load() != 4 {
			t.Fatalf("probe retried: calls=%d", calls.Load())
		}
		_ = invoke(down)
		if calls.Load() != 4 {
			t.Fatal("failed probe did not reopen")
		}
		time.Sleep(15 * time.Second)
		err := invoke(func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
			calls.Add(1)
			return status.Error(codes.NotFound, "business response")
		})
		if status.Code(err) != codes.NotFound {
			t.Fatal(err)
		}
		if err := invoke(func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
			calls.Add(1)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if calls.Load() != 6 {
			t.Fatalf("recovery calls=%d", calls.Load())
		}
	})
}

func TestStaleCompletionCannotChangeNewGeneration(t *testing.T) {
	cc, logger := testClient(t)
	synctest.Test(t, func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.APIKey = "key"
		cfg.FailureThreshold = 1
		cfg.RPCTimeout = time.Minute
		cfg.AttemptTimeout = time.Minute
		interceptor := NewClientInterceptor(cfg, logger)
		invoke := func(fn grpc.UnaryInvoker) error {
			return interceptor(context.Background(), "/flight/GetFlight", nil, nil, cc, fn)
		}
		started, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
		go func() {
			done <- invoke(func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
				close(started)
				<-release
				return nil
			})
		}()
		<-started
		down := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
			return status.Error(codes.Unavailable, "down")
		}
		_ = invoke(down)
		close(release)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		calls := 0
		err := invoke(func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
			calls++
			return nil
		})
		if status.Code(err) != codes.Unavailable || calls != 0 {
			t.Fatalf("stale success reopened breaker: calls=%d err=%v", calls, err)
		}
	})
}

func TestServerAuth(t *testing.T) {
	for _, tc := range []struct {
		name   string
		values []string
		valid  bool
	}{
		{"missing", nil, false}, {"wrong", []string{"wrong"}, false}, {"empty", []string{""}, false}, {"multiple", []string{"key", "key"}, false}, {"valid", []string{"key"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := metadata.NewIncomingContext(context.Background(), metadata.MD{"x-api-key": tc.values})
			called := false
			got, err := ServerAuth("key")(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/flight/GetFlight"}, func(context.Context, any) (any, error) { called = true; return "ok", nil })
			if tc.valid {
				if err != nil || got != "ok" || !called {
					t.Fatalf("got=%v err=%v called=%v", got, err, called)
				}
			} else if status.Code(err) != codes.Unauthenticated || called {
				t.Fatalf("got=%v err=%v called=%v", got, err, called)
			}
		})
	}
}

func TestConfigValidation(t *testing.T) {
	cfg := DefaultConfig()
	cfg.APIKey = "key"
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		change func(*Config)
	}{
		{"empty key", func(c *Config) { c.APIKey = "" }}, {"too many attempts", func(c *Config) { c.Attempts = 4 }}, {"no attempts", func(c *Config) { c.Attempts = 0 }},
		{"zero backoff", func(c *Config) { c.Backoff = 0 }}, {"zero attempt timeout", func(c *Config) { c.AttemptTimeout = 0 }}, {"zero RPC timeout", func(c *Config) { c.RPCTimeout = 0 }},
		{"zero failures", func(c *Config) { c.FailureThreshold = 0 }}, {"zero window", func(c *Config) { c.Window = 0 }}, {"zero open timeout", func(c *Config) { c.OpenTimeout = 0 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := cfg
			tc.change(&bad)
			if bad.Validate() == nil {
				t.Fatal("invalid config accepted")
			}
		})
	}
}

func TestTransientFailureRecoversWithoutExtraAttempt(t *testing.T) {
	cc, logger := testClient(t)
	synctest.Test(t, func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.APIKey = "key"
		calls := 0
		started := time.Now()
		err := NewClientInterceptor(cfg, logger)(context.Background(), "/flight/GetFlight", nil, nil, cc, func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
			calls++
			if calls == 1 {
				return status.Error(codes.DeadlineExceeded, "temporary timeout")
			}
			return nil
		})
		if err != nil || calls != 2 || time.Since(started) != 100*time.Millisecond {
			t.Fatalf("calls=%d elapsed=%v err=%v", calls, time.Since(started), err)
		}
	})
}

func TestBreakersAreIsolatedByTargetAndSharedByMethod(t *testing.T) {
	cc, logger := testClient(t)
	other, err := grpc.NewClient("passthrough:///other", grpc.WithInsecure())
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	synctest.Test(t, func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.APIKey = "key"
		cfg.FailureThreshold = 1
		interceptor := NewClientInterceptor(cfg, logger)
		err := interceptor(context.Background(), "/flight/GetFlight", nil, nil, cc, func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
			return status.Error(codes.Unavailable, "down")
		})
		if status.Code(err) != codes.Unavailable {
			t.Fatal(err)
		}
		calls := 0
		success := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
			calls++
			return nil
		}
		if err := interceptor(context.Background(), "/flight/ReserveSeats", nil, nil, cc, success); status.Code(err) != codes.Unavailable {
			t.Fatalf("other method bypassed breaker: %v", err)
		}
		if err := interceptor(context.Background(), "/flight/GetFlight", nil, nil, other, success); err != nil {
			t.Fatal(err)
		}
		if calls != 1 {
			t.Fatalf("wire calls=%d", calls)
		}
	})
}

func TestStaleFailureCannotReopenRecoveredBreaker(t *testing.T) {
	cc, logger := testClient(t)
	synctest.Test(t, func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.APIKey = "key"
		cfg.FailureThreshold = 1
		cfg.RPCTimeout = time.Minute
		cfg.AttemptTimeout = time.Minute
		cfg.Attempts = 1
		interceptor := NewClientInterceptor(cfg, logger)
		invoke := func(fn grpc.UnaryInvoker) error {
			return interceptor(context.Background(), "/flight/GetFlight", nil, nil, cc, fn)
		}
		started, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
		down := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
			return status.Error(codes.Unavailable, "down")
		}
		go func() {
			done <- invoke(func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
				close(started)
				<-release
				return status.Error(codes.Unavailable, "old failure")
			})
		}()
		<-started
		_ = invoke(down)
		time.Sleep(15 * time.Second)
		success := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error { return nil }
		if err := invoke(success); err != nil {
			t.Error(err)
		}
		close(release)
		if status.Code(<-done) != codes.Unavailable {
			t.Error("old failure lost")
		}
		if err := invoke(success); err != nil {
			t.Fatalf("stale failure reopened recovered breaker: %v", err)
		}
	})
}

func TestLogsExposeTransitionsWithoutCredentials(t *testing.T) {
	cc, _ := testClient(t)
	synctest.Test(t, func(t *testing.T) {
		var logs bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&logs, nil))
		cfg := DefaultConfig()
		cfg.APIKey = "secret-key-not-for-logs"
		cfg.FailureThreshold = 1
		interceptor := NewClientInterceptor(cfg, logger)
		invoke := func(fn grpc.UnaryInvoker) error {
			return interceptor(context.Background(), "/flight/GetFlight", nil, nil, cc, fn)
		}
		_ = invoke(func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
			return status.Error(codes.Unavailable, "secret-key-not-for-logs")
		})
		time.Sleep(15 * time.Second)
		_ = invoke(func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error { return nil })
		text := logs.String()
		for _, want := range []string{"retrying gRPC call", "from=CLOSED to=OPEN", "from=OPEN to=HALF_OPEN", "from=HALF_OPEN to=CLOSED"} {
			if !strings.Contains(text, want) {
				t.Errorf("missing %q in logs: %s", want, text)
			}
		}
		if strings.Contains(text, cfg.APIKey) {
			t.Fatal("credentials leaked to logs")
		}
	})
}

func TestAttemptLimitCannotExceedThree(t *testing.T) {
	cc, logger := testClient(t)
	synctest.Test(t, func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.APIKey = "key"
		cfg.Attempts = 10
		calls := 0
		_ = NewClientInterceptor(cfg, logger)(context.Background(), "/flight/GetFlight", nil, nil, cc, func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
			calls++
			return status.Error(codes.Unavailable, "down")
		})
		if calls != 3 {
			t.Fatalf("wire calls=%d, want maximum 3", calls)
		}
	})
}

func TestCancelledHalfOpenProbeReopensBreaker(t *testing.T) {
	cc, logger := testClient(t)
	synctest.Test(t, func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.APIKey = "key"
		cfg.FailureThreshold = 1
		cfg.Attempts = 1
		interceptor := NewClientInterceptor(cfg, logger)
		calls := 0
		_ = interceptor(context.Background(), "/flight/GetFlight", nil, nil, cc, func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
			calls++
			return status.Error(codes.Unavailable, "down")
		})
		time.Sleep(15 * time.Second)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := interceptor(ctx, "/flight/GetFlight", nil, nil, cc, func(ctx context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
			calls++
			cancel()
			<-ctx.Done()
			return ctx.Err()
		})
		if status.Code(err) != codes.Canceled {
			t.Fatalf("probe returned %v, want Canceled", err)
		}
		err = interceptor(context.Background(), "/flight/GetFlight", nil, nil, cc, func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
			calls++
			return nil
		})
		if status.Code(err) != codes.Unavailable || calls != 2 {
			t.Fatalf("cancelled probe closed breaker: calls=%d err=%v", calls, err)
		}
	})
}

func TestClosedLocalCancellationPreservesInfrastructureStreak(t *testing.T) {
	cc, logger := testClient(t)
	for _, duringBackoff := range []bool{false, true} {
		name := "active attempt"
		if duringBackoff {
			name = "retry backoff"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg := DefaultConfig()
				cfg.APIKey = "key"
				cfg.FailureThreshold = 2
				interceptor := NewClientInterceptor(cfg, logger)
				calls := 0
				down := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
					calls++
					return status.Error(codes.Unavailable, "down")
				}
				_ = interceptor(context.Background(), "/flight/GetFlight", nil, nil, cc, down)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				timer := time.AfterFunc(50*time.Millisecond, cancel)
				defer timer.Stop()
				err := interceptor(ctx, "/flight/GetFlight", nil, nil, cc, func(ctx context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
					calls++
					if duringBackoff {
						return status.Error(codes.Unavailable, "down")
					}
					<-ctx.Done()
					return ctx.Err()
				})
				if status.Code(err) != codes.Canceled {
					t.Fatalf("cancelled RPC returned %v", err)
				}
				_ = interceptor(context.Background(), "/flight/GetFlight", nil, nil, cc, down)
				err = interceptor(context.Background(), "/flight/GetFlight", nil, nil, cc, func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
					calls++
					return nil
				})
				if status.Code(err) != codes.Unavailable || calls != 7 {
					t.Fatalf("local cancellation reset failure streak: calls=%d err=%v", calls, err)
				}
			})
		})
	}
}
