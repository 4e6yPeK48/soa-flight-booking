package main

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	flightv1 "github.com/mchupryna/soa-flight-booking/gen/flight/v1"
	"github.com/mchupryna/soa-flight-booking/internal/config"
	"github.com/mchupryna/soa-flight-booking/internal/flight"
	"github.com/mchupryna/soa-flight-booking/internal/transport"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("flight stopped", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if len(cfg.RedisSentinelAddrs) == 0 {
		return errors.New("REDIS_SENTINEL_ADDRS is required")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	db, err := sql.Open("pgx", cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(5)
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	err = db.PingContext(pingCtx)
	cancel()
	if err != nil {
		return err
	}
	cache := redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName: cfg.RedisMasterName, SentinelAddrs: cfg.RedisSentinelAddrs,
		ContextTimeoutEnabled: true, MaxRetries: -1, DialerRetries: 1,
		DialTimeout: 50 * time.Millisecond, ReadTimeout: 50 * time.Millisecond,
		WriteTimeout: 50 * time.Millisecond, PoolTimeout: 50 * time.Millisecond,
	})
	defer cache.Close()
	server := grpc.NewServer(grpc.UnaryInterceptor(transport.ServerAuth(cfg.APIKey)))
	flightv1.RegisterFlightServiceServer(server, flight.New(db, cache, logger))
	listener, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return err
	}
	defer listener.Close()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		c, done := context.WithTimeout(r.Context(), time.Second)
		defer done()
		if db.PingContext(c) != nil {
			http.Error(w, "database unavailable", 503)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	health := &http.Server{Addr: cfg.HealthAddr, Handler: mux, ReadHeaderTimeout: 3 * time.Second}
	errs := make(chan error, 2)
	go func() { errs <- server.Serve(listener) }()
	go func() { errs <- health.ListenAndServe() }()
	logger.Info("flight started", "grpc", cfg.GRPCAddr)
	select {
	case <-ctx.Done():
	case err = <-errs:
	}
	shutdown, done := context.WithTimeout(context.Background(), 5*time.Second)
	defer done()
	_ = health.Shutdown(shutdown)
	stopped := make(chan struct{})
	go func() { server.GracefulStop(); close(stopped) }()
	select {
	case <-stopped:
	case <-shutdown.Done():
		server.Stop()
	}
	if errors.Is(err, http.ErrServerClosed) || errors.Is(err, grpc.ErrServerStopped) {
		return nil
	}
	return err
}
