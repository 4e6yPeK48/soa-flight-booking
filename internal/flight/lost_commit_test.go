package flight

import (
	"context"
	"os"
	"testing"

	flightv1 "github.com/mchupryna/soa-flight-booking/gen/flight/v1"
	"github.com/mchupryna/soa-flight-booking/internal/testdb"
	"google.golang.org/grpc/codes"
)

func TestPostgresReserveLostCommitAcknowledgment(t *testing.T) {
	db := flightDB(t)
	id, bid := createTestFlight(t, db, 3), bookingID(t, db)
	faultDB, fault := testdb.OpenCommitFault(t, os.Getenv("FLIGHT_TEST_DATABASE_URL"))
	s := New(faultDB, nil, nil)
	ctx := context.Background()
	var before int64
	if err := db.QueryRow(`SELECT revision FROM cache_revision WHERE singleton`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	assertCommitted := func(wantSeats int, wantStatus string, wantRevision int64) {
		t.Helper()
		var seats, count, reservedSeats int
		var state string
		var revision int64
		err := db.QueryRow(`SELECT f.available_seats,r.status,r.seat_count,(SELECT count(*) FROM seat_reservations WHERE booking_id=$2),c.revision FROM flights f JOIN seat_reservations r ON r.flight_id=f.id CROSS JOIN cache_revision c WHERE f.id=$1 AND r.booking_id=$2 AND c.singleton`, id, bid).Scan(&seats, &state, &reservedSeats, &count, &revision)
		if err != nil {
			t.Fatal(err)
		}
		if seats != wantSeats || state != wantStatus || reservedSeats != 2 || count != 1 || revision != wantRevision {
			t.Fatalf("seats=%d reservation=%s/%d count=%d revision=%d; want seats=%d status=%s revision=%d", seats, state, reservedSeats, count, revision, wantSeats, wantStatus, wantRevision)
		}
	}
	r := &flightv1.ReserveSeatsRequest{FlightId: id, BookingId: bid, SeatCount: 2}
	fault.Arm()
	_, err := s.ReserveSeats(ctx, r)
	requireCode(t, err, codes.Unavailable)
	if !fault.Triggered() {
		t.Fatal("fault did not run after a successful PostgreSQL COMMIT")
	}
	assertCommitted(1, "ACTIVE", before+1)
	replay, err := s.ReserveSeats(ctx, r)
	requireCode(t, err, codes.OK)
	if replay.BookingId != bid || replay.FlightId != id || replay.SeatCount != 2 || replay.Status != flightv1.ReservationStatus_ACTIVE {
		t.Fatalf("replay=%+v", replay)
	}
	assertCommitted(1, "ACTIVE", before+1)
	for range 2 {
		_, err := s.ReleaseReservation(ctx, &flightv1.ReleaseReservationRequest{BookingId: bid})
		requireCode(t, err, codes.OK)
		assertCommitted(3, "RELEASED", before+2)
	}
}
