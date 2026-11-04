package flight

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"github.com/jackc/pgx/v5/pgconn"
	"net"
	"testing"

	flightv1 "github.com/mchupryna/soa-flight-booking/gen/flight/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestInvalidRequests(t *testing.T) {
	s := New(nil, nil, nil)
	ctx := context.Background()
	id := "e4d09a52-85c9-458a-b82f-1712b5401516"
	tests := []struct {
		name string
		call func() error
	}{
		{"get malformed UUID", func() error { _, e := s.GetFlight(ctx, &flightv1.GetFlightRequest{FlightId: "bad"}); return e }},
		{"get noncanonical UUID", func() error {
			_, e := s.GetFlight(ctx, &flightv1.GetFlightRequest{FlightId: "e4d09a5285c9458ab82f1712b5401516"})
			return e
		}},
		{"get uppercase UUID", func() error {
			_, e := s.GetFlight(ctx, &flightv1.GetFlightRequest{FlightId: "E4D09A52-85C9-458A-B82F-1712B5401516"})
			return e
		}},
		{"lowercase IATA", func() error {
			_, e := s.SearchFlights(ctx, &flightv1.SearchFlightsRequest{Origin: "svo", Destination: "LED"})
			return e
		}},
		{"same airports", func() error {
			_, e := s.SearchFlights(ctx, &flightv1.SearchFlightsRequest{Origin: "SVO", Destination: "SVO"})
			return e
		}},
		{"impossible date", func() error {
			_, e := s.SearchFlights(ctx, &flightv1.SearchFlightsRequest{Origin: "SVO", Destination: "LED", Date: "2026-02-30"})
			return e
		}},
		{"year zero", func() error {
			_, e := s.SearchFlights(ctx, &flightv1.SearchFlightsRequest{Origin: "SVO", Destination: "LED", Date: "0000-01-01"})
			return e
		}},
		{"nonpadded date", func() error {
			_, e := s.SearchFlights(ctx, &flightv1.SearchFlightsRequest{Origin: "SVO", Destination: "LED", Date: "2026-2-03"})
			return e
		}},
		{"reserve zero", func() error {
			_, e := s.ReserveSeats(ctx, &flightv1.ReserveSeatsRequest{FlightId: id, BookingId: id})
			return e
		}},
		{"reserve invalid booking", func() error {
			_, e := s.ReserveSeats(ctx, &flightv1.ReserveSeatsRequest{FlightId: id, BookingId: "bad", SeatCount: 1})
			return e
		}},
		{"release invalid booking", func() error {
			_, e := s.ReleaseReservation(ctx, &flightv1.ReleaseReservationRequest{BookingId: "bad"})
			return e
		}},
		{"empty update", func() error { _, e := s.UpdateFlight(ctx, &flightv1.UpdateFlightRequest{FlightId: id}); return e }},
		{"zero price", func() error {
			p := int64(0)
			_, e := s.UpdateFlight(ctx, &flightv1.UpdateFlightRequest{FlightId: id, Price: &p})
			return e
		}},
		{"unknown status", func() error {
			v := flightv1.FlightStatus(123)
			_, e := s.UpdateFlight(ctx, &flightv1.UpdateFlightRequest{FlightId: id, Status: &v})
			return e
		}},
		{"unspecified status", func() error {
			v := flightv1.FlightStatus_FLIGHT_STATUS_UNSPECIFIED
			_, e := s.UpdateFlight(ctx, &flightv1.UpdateFlightRequest{FlightId: id, Status: &v})
			return e
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if e := tt.call(); status.Code(e) != codes.InvalidArgument {
				t.Fatalf("code=%v err=%v", status.Code(e), e)
			}
		})
	}
}

func TestStorageErrorMapping(t *testing.T) {
	s := New(nil, nil, nil)
	tests := []struct {
		name string
		err  error
		code codes.Code
	}{
		{"lost connection", driver.ErrBadConn, codes.Unavailable},
		{"closed connection", sql.ErrConnDone, codes.Unavailable},
		{"network failure", &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("offline")}, codes.Unavailable},
		{"postgres shutdown", &pgconn.PgError{Code: "57P01"}, codes.Unavailable},
		{"connection failure", &pgconn.PgError{Code: "08006"}, codes.Unavailable},
		{"constraint failure", &pgconn.PgError{Code: "23514"}, codes.Internal},
		{"unexpected failure", errors.New("broken query"), codes.Internal},
		{"canceled", context.Canceled, codes.Canceled},
		{"deadline", context.DeadlineExceeded, codes.DeadlineExceeded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) { requireCode(t, s.storageError(context.Background(), tt.err), tt.code) })
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	requireCode(t, s.storageError(ctx, errors.New("query canceled")), codes.Canceled)
}
