package main

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	flightv1 "github.com/mchupryna/soa-flight-booking/gen/flight/v1"
	"github.com/mchupryna/soa-flight-booking/internal/booking"
	"github.com/mchupryna/soa-flight-booking/internal/config"
	"github.com/mchupryna/soa-flight-booking/internal/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("booking stopped", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
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
	conn, err := grpc.NewClient(cfg.FlightAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry(), grpc.WithUnaryInterceptor(transport.NewClientInterceptor(cfg.RPC, logger)))
	if err != nil {
		return err
	}
	defer conn.Close()
	app := booking.New(db, flightv1.NewFlightServiceClient(conn), logger, booking.Config{RequestTimeout: cfg.HTTPTimeout, OperationTimeout: cfg.OperationTimeout, RecoveryInterval: cfg.RecoveryInterval})
	go app.RunRecovery(ctx)
	server := &http.Server{Addr: cfg.HTTPAddr, Handler: app.Handler(), ReadHeaderTimeout: 3 * time.Second, ReadTimeout: cfg.HTTPTimeout, IdleTimeout: 60 * time.Second}
	errs := make(chan error, 1)
	go func() { errs <- server.ListenAndServe() }()
	logger.Info("booking started", "http", cfg.HTTPAddr)
	select {
	case <-ctx.Done():
	case err = <-errs:
	}
	shutdown, done := context.WithTimeout(context.Background(), 5*time.Second)
	defer done()
	_ = server.Shutdown(shutdown)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
