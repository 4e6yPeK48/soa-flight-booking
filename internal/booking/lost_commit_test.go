package booking

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	flightv1 "github.com/mchupryna/soa-flight-booking/gen/flight/v1"
	"github.com/mchupryna/soa-flight-booking/internal/api"
	"github.com/mchupryna/soa-flight-booking/internal/testdb"
	"google.golang.org/grpc"
)

type journalCheckedClient struct {
	flightv1.FlightServiceClient
	beforeGet func()
}

func (c *journalCheckedClient) GetFlight(ctx context.Context, r *flightv1.GetFlightRequest, opts ...grpc.CallOption) (*flightv1.Flight, error) {
	c.beforeGet()
	return c.FlightServiceClient.GetFlight(ctx, r, opts...)
}

func TestPostgresCreateLostCommitAcknowledgment(t *testing.T) {
	s, f, db, key, fid := fixture(t)
	faultDB, fault := testdb.OpenCommitFault(t, os.Getenv("BOOKING_TEST_DATABASE_URL"))
	s.db = faultDB
	s.flights = &journalCheckedClient{FlightServiceClient: f, beforeGet: func() {
		// The first remote call must already have a durable operation to recover.
		assertState(t, db, key, "PENDING", 0)
	}}
	// beginCreate also commits; arm only after the remote reservation succeeds.
	f.afterReserve = fault.Arm
	w := request(s, "POST", "/bookings", payload(fid), key)
	var body api.Error
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if w.Code != 503 || body.ErrorCode != "UNKNOWN_OUTCOME" || !fault.Triggered() {
		t.Fatalf("response=%d %s fault triggered=%v", w.Code, w.Body, fault.Triggered())
	}
	assertState(t, db, key, "DONE", 1)
	var bookingID, bookingStatus string
	if err := db.QueryRow(`SELECT b.id,b.status FROM bookings b JOIN booking_operations o ON o.booking_id=b.id WHERE o.idempotency_key=$1`, key).Scan(&bookingID, &bookingStatus); err != nil {
		t.Fatal(err)
	}
	if bookingStatus != "CONFIRMED" || body.BookingId == nil || *body.BookingId != bookingID {
		t.Fatalf("persisted booking=%s/%s response=%+v", bookingID, bookingStatus, body)
	}
	if f.releases != 0 || !f.reserved[bookingID] {
		t.Fatalf("ambiguous commit compensated committed booking: releases=%d reserved=%v", f.releases, f.reserved)
	}
	replay := mustBooking(t, request(s, "POST", "/bookings", payload(fid), key), 201)
	if replay.Id != bookingID || replay.Status != "CONFIRMED" || f.gets != 1 || f.reserves != 1 {
		t.Fatalf("replay=%+v get=%d reserve=%d", replay, f.gets, f.reserves)
	}
	if _, err := db.Exec(`UPDATE booking_operations SET expires_at=now()-interval '1 second' WHERE idempotency_key=$1`, key); err != nil {
		t.Fatal(err)
	}
	s.recoverOnce(context.Background())
	assertState(t, db, key, "DONE", 1)
	if f.releases != 0 || !f.reserved[bookingID] {
		t.Fatalf("recovery compensated committed booking: releases=%d reserved=%v", f.releases, f.reserved)
	}
}
