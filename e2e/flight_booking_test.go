//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	flightv1 "github.com/mchupryna/soa-flight-booking/gen/flight/v1"
	"github.com/mchupryna/soa-flight-booking/internal/api"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestFlightBooking(t *testing.T) {
	for _, test := range []struct {
		name string
		fn   func(*scenario)
	}{
		{"REST lifecycle and user isolation", lifecycle},
		{"REST strict JSON and input validation", validation},
		{"All five gRPC methods require authentication", authentication},
		{"Search uses UTC dates and scheduled status", searchUTC},
		{"gRPC validation and not-found errors", grpcValidation},
		{"Reservation and release are idempotent", reservationIdempotency},
		{"Last seat has exactly one concurrent winner", lastSeat},
		{"HTTP idempotency replay and payload conflict", httpIdempotency},
		{"Failed upstream creates no booking or seat loss", upstreamBusinessFailures},
		{"Price snapshot and cache invalidation", priceAndCache},
		// Fault scenarios are deliberately sequential and last: they stop this project's containers.
		{"Redis outage falls back to PostgreSQL", redisOutage},
		{"Sentinel promotes replica and cache recovers", sentinelFailover},
		{"Flight outage opens and recovers circuit", flightOutage},
		{"Booking crash after reserve is compensated", crashAfterReserve},
		{"Booking crash after release finishes cancellation", crashAfterRelease},
	} {
		t.Run(test.name, func(t *testing.T) { runCase(t, test.name, test.fn) })
	}
}
func lifecycle(s *scenario) {
	fid := s.newFlight(10, 12500)
	s.request("GET", "/health", nil, "", 200)
	var flights []api.Flight
	s.must(json.Unmarshal(s.request("GET", "/flights?origin=QAA&destination=QBB&date=2037-04-02", nil, "", 200), &flights))
	s.equal(len(flights), 1)
	s.equal(flights[0].Id, fid)
	var f api.Flight
	s.must(json.Unmarshal(s.request("GET", "/flights/"+fid, nil, "", 200), &f))
	s.equal(f.AvailableSeats, 10)
	b := s.create(fid, 2, "")
	s.equal(b.Status, "CONFIRMED")
	s.equal(b.TotalPrice, 25000)
	s.equal(s.booking(b.Id).Id, b.Id)
	s.equal(s.seats(fid), 8)
	var list []api.Booking
	s.must(json.Unmarshal(s.request("GET", "/bookings?user_id=7182349001", nil, "", 200), &list))
	found := false
	for _, item := range list {
		s.equal(item.UserId, 7182349001)
		if item.Id == b.Id {
			found = true
		}
	}
	s.equal(found, true)
	s.must(json.Unmarshal(s.request("GET", "/bookings?user_id=7182349002", nil, "", 200), &list))
	for _, item := range list {
		if item.Id == b.Id {
			s.t.Fatal("another user received this booking")
		}
	}
	var cancelled api.Booking
	s.must(json.Unmarshal(s.request("POST", "/bookings/"+b.Id+"/cancel", nil, "", 200), &cancelled))
	s.equal(cancelled.Status, "CANCELLED")
	s.equal(s.booking(b.Id).Status, "CANCELLED")
	s.equal(s.seats(fid), 10)
	s.request("POST", "/bookings/"+b.Id+"/cancel", nil, "", 200)
	s.equal(s.seats(fid), 10)
	s.request("GET", "/flights/"+uuid.NewString(), nil, "", 404)
	s.request("GET", "/bookings/"+uuid.NewString(), nil, "", 404)
	s.request("POST", "/bookings/"+uuid.NewString()+"/cancel", nil, "", 404)
}
func validation(s *scenario) {
	fid := s.newFlight(4, 100)
	for _, test := range []struct {
		name   string
		change func(map[string]any)
	}{
		{"invalid UUID", func(m map[string]any) { m["flight_id"] = "bad" }},
		{"empty name", func(m map[string]any) { m["passenger_name"] = " " }},
		{"long name", func(m map[string]any) { m["passenger_name"] = strings.Repeat("x", 201) }},
		{"invalid email", func(m map[string]any) { m["passenger_email"] = "bad" }},
		{"long email", func(m map[string]any) { m["passenger_email"] = strings.Repeat("x", 250) + "@example.test" }},
		{"zero seats", func(m map[string]any) { m["seat_count"] = 0 }},
		{"negative seats", func(m map[string]any) { m["seat_count"] = -1 }},
		{"int32 overflow", func(m map[string]any) { m["seat_count"] = int64(math.MaxInt32) + 1 }},
		{"fractional seats", func(m map[string]any) { m["seat_count"] = 1.5 }},
		{"invalid user", func(m map[string]any) { m["user_id"] = 0 }},
		{"unknown property", func(m map[string]any) { m["unknown"] = true }},
		{"missing required property", func(m map[string]any) { delete(m, "seat_count") }},
	} {
		s.attach("Validation case", map[string]any{"case": test.name})
		body := requestBody(fid, 1)
		test.change(body)
		s.request("POST", "/bookings", body, "", 400)
	}
	for _, body := range []string{"{", "null", "[]", `{"user_id":1} {}`, `{"user_id":1,"user_id":2}`} {
		s.raw("POST", "/bookings", []byte(body), "", 400)
	}
	s.request("POST", "/bookings", requestBody(fid, 1), "invalid-key", 400)
	for _, path := range []string{"/flights?origin=QAA", "/flights?origin=qaa&destination=QBB", "/flights?origin=QAA&destination=QAA", "/flights?origin=QAA&destination=QBB&date=2037-02-30", "/flights/invalid", "/bookings/invalid", "/bookings?user_id=-1", "/bookings?user_id=9223372036854775808"} {
		s.request("GET", path, nil, "", 400)
	}
	s.slowPartialBody()
	s.equal(s.countBookings(fid), 0)
	s.equal(s.seats(fid), 4)
	expensive := s.newFlight(2, math.MaxInt64)
	s.request("POST", "/bookings", requestBody(expensive, 2), uuid.NewString(), 502)
	s.equal(s.countBookings(expensive), 0)
	s.equal(s.seats(expensive), 2)
}
func authentication(s *scenario) {
	fid := s.newFlight(4, 100)
	bid := uuid.NewString()
	s.remember(bid)
	operations := []struct {
		name string
		call func(context.Context) error
	}{
		{"SearchFlights", func(ctx context.Context) error {
			_, e := s.flight.SearchFlights(ctx, &flightv1.SearchFlightsRequest{Origin: "QAA", Destination: "QBB"})
			return e
		}},
		{"GetFlight", func(ctx context.Context) error {
			_, e := s.flight.GetFlight(ctx, &flightv1.GetFlightRequest{FlightId: fid})
			return e
		}},
		{"ReserveSeats", func(ctx context.Context) error {
			_, e := s.flight.ReserveSeats(ctx, &flightv1.ReserveSeatsRequest{FlightId: fid, BookingId: bid, SeatCount: 1})
			return e
		}},
		{"ReleaseReservation", func(ctx context.Context) error {
			_, e := s.flight.ReleaseReservation(ctx, &flightv1.ReleaseReservationRequest{BookingId: bid})
			return e
		}},
		{"UpdateFlight", func(ctx context.Context) error {
			p := int64(101)
			_, e := s.flight.UpdateFlight(ctx, &flightv1.UpdateFlightRequest{FlightId: fid, Price: &p})
			return e
		}},
	}
	for _, op := range operations {
		for _, key := range []string{"", "wrong-key"} {
			ctx := s.t.Context()
			if key != "" {
				ctx = metadata.AppendToOutgoingContext(ctx, "x-api-key", key)
			}
			err := op.call(ctx)
			s.attach("gRPC authentication", map[string]any{"method": op.name, "credential": map[bool]string{true: "missing", false: "invalid"}[key == ""], "code": status.Code(err).String()})
			s.equal(status.Code(err), codes.Unauthenticated)
		}
	}
	for _, op := range operations {
		s.must(op.call(s.ctx))
	}
	s.equal(s.seats(fid), 4)
}
func searchUTC(s *scenario) {
	// The local 02 April timestamp belongs to 01 April in UTC.
	fid := s.fixture(3, 100, "SCHEDULED", time.Date(2037, 4, 2, 1, 30, 0, 0, time.FixedZone("UTC+3", 3*3600)))
	s.fixture(3, 100, "CANCELLED", time.Date(2037, 4, 1, 12, 0, 0, 0, time.UTC))
	s.fixture(3, 100, "DEPARTED", time.Date(2037, 4, 1, 13, 0, 0, 0, time.UTC))
	s.fixture(3, 100, "COMPLETED", time.Date(2037, 4, 1, 14, 0, 0, 0, time.UTC))
	for _, test := range []struct {
		day   string
		count int
	}{{"2037-04-01", 1}, {"2037-04-02", 0}, {"", 1}} {
		got, err := s.flight.SearchFlights(s.ctx, &flightv1.SearchFlightsRequest{Origin: "QAA", Destination: "QBB", Date: test.day})
		s.must(err)
		s.equal(len(got.Flights), test.count)
		if test.count > 0 {
			s.equal(got.Flights[0].Id, fid)
			s.equal(got.Flights[0].Status, flightv1.FlightStatus_SCHEDULED)
		}
	}
	_, err := s.flight.SearchFlights(s.ctx, &flightv1.SearchFlightsRequest{Origin: "QAA", Destination: "QBB", Date: "2037-02-30"})
	s.equal(status.Code(err), codes.InvalidArgument)
}
func grpcValidation(s *scenario) {
	fid := s.newFlight(2, 100)
	bid := uuid.NewString()
	s.remember(bid)
	cases := []struct {
		name string
		call func() error
		want codes.Code
	}{
		{"GetFlight invalid UUID", func() error {
			_, e := s.flight.GetFlight(s.ctx, &flightv1.GetFlightRequest{FlightId: "invalid"})
			return e
		}, codes.InvalidArgument},
		{"GetFlight absent", func() error {
			_, e := s.flight.GetFlight(s.ctx, &flightv1.GetFlightRequest{FlightId: uuid.NewString()})
			return e
		}, codes.NotFound},
		{"Search invalid IATA", func() error {
			_, e := s.flight.SearchFlights(s.ctx, &flightv1.SearchFlightsRequest{Origin: "bad", Destination: "QBB"})
			return e
		}, codes.InvalidArgument},
		{"ReserveSeats zero seats", func() error {
			_, e := s.flight.ReserveSeats(s.ctx, &flightv1.ReserveSeatsRequest{FlightId: fid, BookingId: bid, SeatCount: 0})
			return e
		}, codes.InvalidArgument},
		{"ReserveSeats absent flight", func() error {
			_, e := s.flight.ReserveSeats(s.ctx, &flightv1.ReserveSeatsRequest{FlightId: uuid.NewString(), BookingId: bid, SeatCount: 1})
			return e
		}, codes.NotFound},
		{"ReleaseReservation invalid UUID", func() error {
			_, e := s.flight.ReleaseReservation(s.ctx, &flightv1.ReleaseReservationRequest{BookingId: "invalid"})
			return e
		}, codes.InvalidArgument},
		{"UpdateFlight absent mutation", func() error {
			_, e := s.flight.UpdateFlight(s.ctx, &flightv1.UpdateFlightRequest{FlightId: fid})
			return e
		}, codes.InvalidArgument},
		{"UpdateFlight invalid price", func() error {
			p := int64(0)
			_, e := s.flight.UpdateFlight(s.ctx, &flightv1.UpdateFlightRequest{FlightId: fid, Price: &p})
			return e
		}, codes.InvalidArgument},
		{"UpdateFlight invalid status", func() error {
			state := flightv1.FlightStatus_FLIGHT_STATUS_UNSPECIFIED
			_, e := s.flight.UpdateFlight(s.ctx, &flightv1.UpdateFlightRequest{FlightId: fid, Status: &state})
			return e
		}, codes.InvalidArgument},
		{"UpdateFlight absent flight", func() error {
			p := int64(200)
			_, e := s.flight.UpdateFlight(s.ctx, &flightv1.UpdateFlightRequest{FlightId: uuid.NewString(), Price: &p})
			return e
		}, codes.NotFound},
	}
	for _, test := range cases {
		code := status.Code(test.call())
		s.attach("gRPC contract error", map[string]any{"case": test.name, "code": code.String()})
		s.equal(code, test.want)
	}
	s.equal(s.seats(fid), 2)
}
func reservationIdempotency(s *scenario) {
	fid := s.newFlight(3, 100)
	bid := uuid.NewString()
	s.remember(bid)
	req := &flightv1.ReserveSeatsRequest{FlightId: fid, BookingId: bid, SeatCount: 2}
	for i := 0; i < 2; i++ {
		r, e := s.flight.ReserveSeats(s.ctx, req)
		s.must(e)
		s.equal(r.Status, flightv1.ReservationStatus_ACTIVE)
		s.equal(s.seats(fid), 1)
	}
	other := s.newFlight(3, 100)
	_, changedErr := s.flight.ReserveSeats(s.ctx, &flightv1.ReserveSeatsRequest{FlightId: other, BookingId: bid, SeatCount: 2})
	s.equal(status.Code(changedErr), codes.FailedPrecondition)
	s.equal(s.seats(other), 3)
	req.SeatCount = 1
	_, err := s.flight.ReserveSeats(s.ctx, req)
	s.equal(status.Code(err), codes.FailedPrecondition)
	s.equal(s.seats(fid), 1)
	for i := 0; i < 2; i++ {
		r, e := s.flight.ReleaseReservation(s.ctx, &flightv1.ReleaseReservationRequest{BookingId: bid})
		s.must(e)
		s.equal(r.Status, flightv1.ReservationStatus_RELEASED)
		s.equal(s.seats(fid), 3)
	}
	req.SeatCount = 2
	_, err = s.flight.ReserveSeats(s.ctx, req)
	s.equal(status.Code(err), codes.FailedPrecondition)
	absent := uuid.NewString()
	s.remember(absent)
	_, err = s.flight.ReleaseReservation(s.ctx, &flightv1.ReleaseReservationRequest{BookingId: absent})
	s.must(err)
	_, err = s.flight.ReserveSeats(s.ctx, &flightv1.ReserveSeatsRequest{FlightId: fid, BookingId: absent, SeatCount: 1})
	s.equal(status.Code(err), codes.FailedPrecondition)
	s.equal(s.seats(fid), 3)
	var n int
	s.must(s.db.QueryRow(`SELECT count(*) FROM seat_reservations WHERE booking_id=$1`, bid).Scan(&n))
	s.equal(n, 1)
}
func lastSeat(s *scenario) {
	fid := s.newFlight(1, 100)
	const callers = 12
	var wg sync.WaitGroup
	start := make(chan struct{})
	results := make(chan codes.Code, callers)
	for i := 0; i < callers; i++ {
		bid := uuid.NewString()
		s.remember(bid)
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
			defer cancel()
			_, err := s.flight.ReserveSeats(ctx, &flightv1.ReserveSeatsRequest{FlightId: fid, BookingId: bid, SeatCount: 1})
			results <- status.Code(err)
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	wins := 0
	for code := range results {
		if code == codes.OK {
			wins++
		} else {
			s.equal(code, codes.ResourceExhausted)
		}
	}
	s.equal(wins, 1)
	s.equal(s.seats(fid), 0)
	var active int
	s.must(s.db.QueryRow(`SELECT count(*) FROM seat_reservations WHERE flight_id=$1 AND status='ACTIVE'`, fid).Scan(&active))
	s.equal(active, 1)
	s.attach("Concurrent reservation verdict", map[string]any{"callers": callers, "successful_reservations": wins, "active_db_rows": active})
}
func httpIdempotency(s *scenario) {
	fid := s.newFlight(8, 100)
	key := uuid.NewString()
	one := s.create(fid, 2, key)
	two := s.create(fid, 2, key)
	s.equal(one.Id, two.Id)
	s.equal(s.seats(fid), 6)
	s.equal(s.countBookings(fid), 1)
	s.request("POST", "/bookings", requestBody(fid, 1), key, 409)
	s.equal(s.seats(fid), 6)
	s.equal(s.countBookings(fid), 1)
	three := s.create(fid, 1, "")
	four := s.create(fid, 1, "")
	if three.Id == four.Id {
		s.t.Fatal("requests without Idempotency-Key were collapsed")
	}
	s.equal(s.countBookings(fid), 3)
	s.equal(s.seats(fid), 4)
}
func upstreamBusinessFailures(s *scenario) {
	fid := s.newFlight(1, 100)
	for _, test := range []struct {
		fid         string
		seats, code int
	}{{uuid.NewString(), 1, 404}, {fid, 2, 409}} {
		key := uuid.NewString()
		s.request("POST", "/bookings", requestBody(test.fid, test.seats), key, test.code)
		var state, bid string
		var n int
		s.must(s.bookings.QueryRow(`SELECT state,booking_id::text,(SELECT count(*) FROM bookings WHERE id=o.booking_id) FROM booking_operations o WHERE idempotency_key=$1`, key).Scan(&state, &bid, &n))
		s.remember(bid)
		s.equal(state, "FAILED")
		s.equal(n, 0)
		s.equal(s.seats(fid), 1)
		s.attach("Failed booking SQL", map[string]any{"booking_id": bid, "journal_state": state, "booking_rows": n})
	}
	unscheduled := s.fixture(2, 100, "DEPARTED", time.Date(2037, 4, 2, 13, 0, 0, 0, time.UTC))
	bid := uuid.NewString()
	s.remember(bid)
	_, err := s.flight.ReserveSeats(s.ctx, &flightv1.ReserveSeatsRequest{FlightId: unscheduled, BookingId: bid, SeatCount: 1})
	s.equal(status.Code(err), codes.FailedPrecondition)
	s.equal(s.seats(unscheduled), 2)
	s.request("POST", "/bookings", requestBody(unscheduled, 1), uuid.NewString(), 409)
	s.equal(s.countBookings(unscheduled), 0)
	s.equal(s.seats(unscheduled), 2)
}
func priceAndCache(s *scenario) {
	fid := s.newFlight(10, 12345)
	since := time.Now().UTC().Format(time.RFC3339Nano)
	r := s.redis()
	before := s.revision()
	get := func() *flightv1.Flight {
		f, e := s.flight.GetFlight(s.ctx, &flightv1.GetFlightRequest{FlightId: fid})
		s.must(e)
		return f
	}
	search := func() *flightv1.SearchFlightsResponse {
		f, e := s.flight.SearchFlights(s.ctx, &flightv1.SearchFlightsRequest{Origin: "QAA", Destination: "QBB", Date: "2037-04-02"})
		s.must(e)
		return f
	}
	get()
	search()
	get()
	search()
	for _, key := range []string{"flight:" + before + ":" + fid, "search:" + before + ":QAA:QBB:2037-04-02"} {
		ttl, e := r.TTL(s.t.Context(), key).Result()
		s.must(e)
		if ttl < 290*time.Second || ttl > 300*time.Second {
			s.t.Fatalf("cache TTL=%s, want finite 300 seconds", ttl)
		}
		s.attach("Redis key TTL", map[string]any{"key": key, "ttl_seconds": ttl.Seconds()})
	}
	b := s.create(fid, 2, uuid.NewString())
	s.equal(b.TotalPrice, 24690)
	s.equal(get().AvailableSeats, 8)
	s.equal(search().Flights[0].AvailableSeats, 8)
	price := int64(20000)
	_, err := s.flight.UpdateFlight(s.ctx, &flightv1.UpdateFlightRequest{FlightId: fid, Price: &price})
	s.must(err)
	s.equal(get().Price, price)
	s.equal(search().Flights[0].Price, price)
	s.equal(s.booking(b.Id).TotalPrice, 24690)
	if before == s.revision() {
		s.t.Fatal("cache revision did not change after mutations")
	}
	s.equal(get().AvailableSeats, 8)
	s.request("POST", "/bookings/"+b.Id+"/cancel", nil, "", 200)
	s.equal(get().AvailableSeats, 10)
	s.equal(search().Flights[0].AvailableSeats, 10)
	cancelled := flightv1.FlightStatus_CANCELLED
	_, err = s.flight.UpdateFlight(s.ctx, &flightv1.UpdateFlightRequest{FlightId: fid, Status: &cancelled})
	s.must(err)
	s.equal(get().Status, cancelled)
	s.equal(len(search().Flights), 0)
	s.equal(s.booking(b.Id).TotalPrice, 24690)
	bid := uuid.NewString()
	s.remember(bid)
	_, err = s.flight.ReserveSeats(s.ctx, &flightv1.ReserveSeatsRequest{FlightId: fid, BookingId: bid, SeatCount: 1})
	s.equal(status.Code(err), codes.FailedPrecondition)
	afterKey := "flight:" + s.revision() + ":" + fid
	s.equal(r.Exists(s.t.Context(), afterKey).Val(), 1)
	logs, err := s.docker("logs", "--since", since, s.container("flight"))
	s.must(err)
	for _, fragment := range []string{"cache hit", "cache miss", "flight:" + before + ":" + fid} {
		if !strings.Contains(logs, fragment) {
			s.t.Fatalf("cache logs missing %s", fragment)
		}
	}
	s.a.Attachment("Cache hits and misses", []byte(s.redact(logs)), "text/plain")
}
