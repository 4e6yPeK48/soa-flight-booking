package flight

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	flightv1 "github.com/mchupryna/soa-flight-booking/gen/flight/v1"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func flightDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("FLIGHT_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("FLIGHT_TEST_DATABASE_URL is not set")
	}
	db, e := sql.Open("pgx", dsn)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { db.Close() })
	if e = db.PingContext(context.Background()); e != nil {
		t.Fatal(e)
	}
	return db
}

func createTestFlight(t *testing.T, db *sql.DB, seats int) string {
	t.Helper()
	id := uuid.NewString()
	var airlineID int64
	for {
		code := strings.ToUpper(uuid.NewString()[:3])
		e := db.QueryRow(`INSERT INTO airlines(code,name) VALUES($1,'Integration test') ON CONFLICT(code) DO NOTHING RETURNING id`, code).Scan(&airlineID)
		if e == sql.ErrNoRows {
			continue
		}
		if e != nil {
			t.Fatal(e)
		}
		break
	}
	t.Cleanup(func() {
		if _, e := db.Exec(`DELETE FROM seat_reservations WHERE flight_id=$1`, id); e != nil {
			t.Error(e)
		}
		if _, e := db.Exec(`DELETE FROM flights WHERE id=$1`, id); e != nil {
			t.Error(e)
		}
		if _, e := db.Exec(`DELETE FROM airlines WHERE id=$1`, airlineID); e != nil {
			t.Error(e)
		}

	})
	_, e := db.Exec(`INSERT INTO flights(id,airline_id,flight_number,origin,destination,departure_time,arrival_time,total_seats,available_seats,price,status) VALUES($1,$2,$3,'SVO','LED','2040-06-03 10:00:00+00','2040-06-03 12:00:00+00',$4,$4,10000,'SCHEDULED')`, id, airlineID, "TEST-"+id, seats)
	if e != nil {
		t.Fatal(e)
	}

	return id
}
func bookingID(t *testing.T, db *sql.DB) string {
	t.Helper()
	id := uuid.NewString()
	t.Cleanup(func() {
		if _, e := db.Exec(`DELETE FROM reservation_cancellations WHERE booking_id=$1`, id); e != nil {
			t.Error(e)
		}
	})
	return id
}
func available(t *testing.T, db *sql.DB, id string) int {
	t.Helper()
	var n int
	if e := db.QueryRow(`SELECT available_seats FROM flights WHERE id=$1`, id).Scan(&n); e != nil {
		t.Fatal(e)
	}
	return n
}
func requireCode(t *testing.T, e error, want codes.Code) {
	t.Helper()
	if status.Code(e) != want {
		t.Fatalf("code=%v want=%v error=%v", status.Code(e), want, e)
	}
}

func TestPostgresReservationLifecycle(t *testing.T) {
	db := flightDB(t)
	s := New(db, nil, nil)
	ctx := context.Background()
	t.Run("last seat has one winner", func(t *testing.T) {
		id := createTestFlight(t, db, 1)
		start := make(chan struct{})
		results := make(chan error, 2)
		for range 2 {
			bid := bookingID(t, db)
			go func() {
				<-start
				_, e := s.ReserveSeats(ctx, &flightv1.ReserveSeatsRequest{FlightId: id, BookingId: bid, SeatCount: 1})
				results <- e
			}()
		}
		close(start)
		var ok, full int
		for range 2 {
			e := <-results
			switch status.Code(e) {
			case codes.OK:
				ok++
			case codes.ResourceExhausted:
				full++
			default:
				t.Fatalf("unexpected reserve: %v", e)
			}
		}
		if ok != 1 || full != 1 || available(t, db, id) != 0 {
			t.Fatalf("wins=%d full=%d seats=%d", ok, full, available(t, db, id))
		}
	})
	t.Run("concurrent duplicate and changed replay", func(t *testing.T) {
		id := createTestFlight(t, db, 3)
		bid := bookingID(t, db)
		r := &flightv1.ReserveSeatsRequest{FlightId: id, BookingId: bid, SeatCount: 2}
		var wg sync.WaitGroup
		errs := make(chan error, 8)
		for range 8 {
			wg.Go(func() { _, e := s.ReserveSeats(ctx, r); errs <- e })
		}
		wg.Wait()
		close(errs)
		for e := range errs {
			requireCode(t, e, codes.OK)
		}
		if n := available(t, db, id); n != 1 {
			t.Fatalf("seats=%d", n)
		}
		_, e := s.ReserveSeats(ctx, &flightv1.ReserveSeatsRequest{FlightId: id, BookingId: bid, SeatCount: 1})
		requireCode(t, e, codes.FailedPrecondition)
		for range 2 {
			_, e = s.ReleaseReservation(ctx, &flightv1.ReleaseReservationRequest{BookingId: bid})
			requireCode(t, e, codes.OK)
		}
		if n := available(t, db, id); n != 3 {
			t.Fatalf("released seats=%d", n)
		}
		_, e = s.ReserveSeats(ctx, r)
		requireCode(t, e, codes.FailedPrecondition)
	})
	t.Run("release before reserve blocks late operation", func(t *testing.T) {
		id := createTestFlight(t, db, 1)
		bid := bookingID(t, db)
		for range 2 {
			_, e := s.ReleaseReservation(ctx, &flightv1.ReleaseReservationRequest{BookingId: bid})
			requireCode(t, e, codes.OK)
		}
		_, e := s.ReserveSeats(ctx, &flightv1.ReserveSeatsRequest{FlightId: id, BookingId: bid, SeatCount: 1})
		requireCode(t, e, codes.FailedPrecondition)
		if n := available(t, db, id); n != 1 {
			t.Fatalf("seats=%d", n)
		}
	})
	t.Run("missing flight and price status updates", func(t *testing.T) {
		_, e := s.GetFlight(ctx, &flightv1.GetFlightRequest{FlightId: uuid.NewString()})
		requireCode(t, e, codes.NotFound)
		id := createTestFlight(t, db, 2)
		bid := bookingID(t, db)
		_, e = s.ReserveSeats(ctx, &flightv1.ReserveSeatsRequest{FlightId: id, BookingId: bid, SeatCount: 1})
		requireCode(t, e, codes.OK)
		price := int64(9999999999)
		state := flightv1.FlightStatus_CANCELLED
		f, e := s.UpdateFlight(ctx, &flightv1.UpdateFlightRequest{FlightId: id, Price: &price, Status: &state})
		requireCode(t, e, codes.OK)
		if f.Price != price || f.Status != state || f.AvailableSeats != 1 || f.DepartureTime.AsTime().UTC().Format(time.RFC3339) != "2040-06-03T10:00:00Z" {
			t.Fatalf("updated flight=%v", f)
		}
		_, e = s.ReserveSeats(ctx, &flightv1.ReserveSeatsRequest{FlightId: id, BookingId: bookingID(t, db), SeatCount: 1})
		requireCode(t, e, codes.FailedPrecondition)
		_, e = s.ReserveSeats(ctx, &flightv1.ReserveSeatsRequest{FlightId: id, BookingId: bid, SeatCount: 1})
		requireCode(t, e, codes.OK)
		list, e := s.SearchFlights(ctx, &flightv1.SearchFlightsRequest{Origin: "SVO", Destination: "LED", Date: "2040-06-03"})
		requireCode(t, e, codes.OK)
		for _, f := range list.Flights {
			if f.Id == id {
				t.Fatal("cancelled flight in search")
			}
		}
	})
	t.Run("UTC date filtering and stable search order", func(t *testing.T) {
		a := createTestFlight(t, db, 1)
		b := createTestFlight(t, db, 1)
		outside := createTestFlight(t, db, 1)
		if _, e := db.Exec(`UPDATE flights SET departure_time='2040-06-04 00:00:00+00',arrival_time='2040-06-04 02:00:00+00' WHERE id=$1`, outside); e != nil {
			t.Fatal(e)
		}
		list, e := s.SearchFlights(ctx, &flightv1.SearchFlightsRequest{Origin: "SVO", Destination: "LED", Date: "2040-06-03"})
		requireCode(t, e, codes.OK)
		var own []string
		for _, f := range list.Flights {
			if f.Id == outside {
				t.Fatal("next UTC day included")
			}
			if f.Id == a || f.Id == b {
				own = append(own, f.Id)
			}
		}
		first, second := a, b
		if first > second {
			first, second = second, first
		}
		if len(own) != 2 || own[0] != first || own[1] != second {
			t.Fatalf("order=%v want=%v", own, []string{first, second})
		}
		list, e = s.SearchFlights(ctx, &flightv1.SearchFlightsRequest{Origin: "SVO", Destination: "LED"})
		requireCode(t, e, codes.OK)
		found := false
		for _, f := range list.Flights {
			if f.Id == outside {
				found = true
			}
		}
		if !found {
			t.Fatal("optional date restricted search")
		}
	})
	t.Run("Redis errors fall back to PostgreSQL", func(t *testing.T) {
		id := createTestFlight(t, db, 1)
		unavailable := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
		unavailable.Close()
		fallback := New(db, unavailable, nil)
		f, e := fallback.GetFlight(ctx, &flightv1.GetFlightRequest{FlightId: id})
		requireCode(t, e, codes.OK)
		if f.Id != id || f.Price != 10000 {
			t.Fatalf("fallback flight=%v", f)
		}
	})

	t.Run("reservation write failure rolls back seats and revision", func(t *testing.T) {
		id := createTestFlight(t, db, 2)
		bid := bookingID(t, db)
		constraint := "test_reject_" + strings.ReplaceAll(bid, "-", "")
		if _, e := db.Exec(fmt.Sprintf(`ALTER TABLE seat_reservations ADD CONSTRAINT %s CHECK (booking_id <> '%s'::uuid) NOT VALID`, constraint, bid)); e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() {
			if _, e := db.Exec(`ALTER TABLE seat_reservations DROP CONSTRAINT ` + constraint); e != nil {
				t.Error(e)
			}
		})
		var before, after int64
		if e := db.QueryRow(`SELECT revision FROM cache_revision`).Scan(&before); e != nil {
			t.Fatal(e)
		}
		_, e := s.ReserveSeats(ctx, &flightv1.ReserveSeatsRequest{FlightId: id, BookingId: bid, SeatCount: 1})
		requireCode(t, e, codes.Internal)
		if n := available(t, db, id); n != 2 {
			t.Fatalf("rollback seats=%d", n)
		}
		if e := db.QueryRow(`SELECT revision FROM cache_revision`).Scan(&after); e != nil {
			t.Fatal(e)
		}
		if after != before {
			t.Fatalf("revision committed on failure: %d -> %d", before, after)
		}
	})
}

func TestPostgresRedisRevisionCache(t *testing.T) {
	db := flightDB(t)
	raw := os.Getenv("FLIGHT_TEST_REDIS_URL")
	if raw == "" {
		t.Skip("FLIGHT_TEST_REDIS_URL is not set")
	}
	opts, e := redis.ParseURL(raw)
	if e != nil {
		t.Fatal(e)
	}
	cache := redis.NewClient(opts)
	t.Cleanup(func() { cache.Close() })
	ctx := context.Background()
	if e := cache.Ping(ctx).Err(); e != nil {
		t.Fatal(e)
	}
	s := New(db, cache, nil)
	id := createTestFlight(t, db, 2)
	f, e := s.GetFlight(ctx, &flightv1.GetFlightRequest{FlightId: id})
	requireCode(t, e, codes.OK)
	var namespace string
	var rev int64
	if e := db.QueryRow(`SELECT namespace::text, revision FROM cache_revision`).Scan(&namespace, &rev); e != nil {
		t.Fatal(e)
	}
	key := fmt.Sprintf("flight:%s:%d:%s", namespace, rev, id)
	t.Cleanup(func() { cache.Del(ctx, key) })
	ttl, e := cache.TTL(ctx, key).Result()
	if e != nil {
		t.Fatal(e)
	}
	if ttl <= 0 || ttl > 300*time.Second {
		t.Fatalf("ttl=%s", ttl)
	}
	// Changing the cached payload proves the next read is a cache hit.
	cached, e := cache.Get(ctx, key).Result()
	if e != nil {
		t.Fatal(e)
	}
	if e := cache.Set(ctx, key, strings.Replace(cached, `"price":"10000"`, `"price":"12345"`, 1), 300*time.Second).Err(); e != nil {
		t.Fatal(e)
	}
	f, e = s.GetFlight(ctx, &flightv1.GetFlightRequest{FlightId: id})
	requireCode(t, e, codes.OK)
	if f.Price != 12345 {
		t.Fatalf("cache hit price=%d", f.Price)
	}
	price := int64(20000)
	_, e = s.UpdateFlight(ctx, &flightv1.UpdateFlightRequest{FlightId: id, Price: &price})
	requireCode(t, e, codes.OK)
	f, e = s.GetFlight(ctx, &flightv1.GetFlightRequest{FlightId: id})
	requireCode(t, e, codes.OK)
	if f.Price != 20000 {
		t.Fatalf("stale price=%d", f.Price)
	}
	// Both search payloads and flight payloads move to a new revision on seat changes.
	searchRequest := &flightv1.SearchFlightsRequest{Origin: "SVO", Destination: "LED", Date: "2040-06-03"}
	seatsInSearch := func() int32 {
		t.Helper()
		list, e := s.SearchFlights(ctx, searchRequest)
		requireCode(t, e, codes.OK)
		for _, entry := range list.Flights {
			if entry.Id == id {
				return entry.AvailableSeats
			}
		}
		t.Fatal("test flight missing in search")
		return -1
	}
	if n := seatsInSearch(); n != 2 {
		t.Fatalf("search seats=%d", n)
	}
	if e := db.QueryRow(`SELECT revision FROM cache_revision`).Scan(&rev); e != nil {
		t.Fatal(e)
	}
	searchKey := fmt.Sprintf("search:%s:%d:SVO:LED:2040-06-03", namespace, rev)
	ttl, e = cache.TTL(ctx, searchKey).Result()
	if e != nil || ttl <= 0 || ttl > 300*time.Second {
		t.Fatalf("search ttl=%s err=%v", ttl, e)
	}
	// A malformed cache record must not leak partially decoded flights into the DB fallback.
	if e := cache.Set(ctx, searchKey, `{"flights":[{"id":"stale"}],"unknown":true}`, 300*time.Second).Err(); e != nil {
		t.Fatal(e)
	}
	list, e := s.SearchFlights(ctx, searchRequest)
	requireCode(t, e, codes.OK)
	for _, entry := range list.Flights {
		if entry.Id == "stale" {
			t.Fatal("partially decoded cache data leaked into fallback")
		}
	}
	bid := bookingID(t, db)
	_, e = s.ReserveSeats(ctx, &flightv1.ReserveSeatsRequest{FlightId: id, BookingId: bid, SeatCount: 1})
	requireCode(t, e, codes.OK)
	if n := seatsInSearch(); n != 1 {
		t.Fatalf("stale reserve search seats=%d", n)
	}
	_, e = s.ReleaseReservation(ctx, &flightv1.ReleaseReservationRequest{BookingId: bid})
	requireCode(t, e, codes.OK)
	if n := seatsInSearch(); n != 2 {
		t.Fatalf("stale release search seats=%d", n)
	}

}

func TestPostgresCacheOutageFitsRPCDeadline(t *testing.T) {
	db := flightDB(t)
	id := createTestFlight(t, db, 2)
	// A closed local port models stopped Redis/Sentinel without DNS or external timing.
	stopped, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	stoppedAddress := stopped.Addr().String()
	stopped.Close()
	stalled, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	var connections []net.Conn
	var workers sync.WaitGroup
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		for {
			connection, e := stalled.Accept()
			if e != nil {
				return
			}
			connections = append(connections, connection)
			workers.Go(func() { defer connection.Close(); _, _ = io.Copy(io.Discard, connection) })
		}
	}()
	t.Cleanup(func() {
		stalled.Close()
		<-acceptDone
		for _, connection := range connections {
			connection.Close()
		}
		workers.Wait()
	})
	tests := []struct {
		name  string
		cache *redis.Client
	}{
		{"stopped sentinel", redis.NewFailoverClient(&redis.FailoverOptions{MasterName: "flights", SentinelAddrs: []string{stoppedAddress}, ContextTimeoutEnabled: true, DialTimeout: 200 * time.Millisecond, ReadTimeout: 200 * time.Millisecond, WriteTimeout: 200 * time.Millisecond, MaxRetries: -1})},
		{"stalled sentinel", redis.NewFailoverClient(&redis.FailoverOptions{MasterName: "flights", SentinelAddrs: []string{stalled.Addr().String()}, ContextTimeoutEnabled: true, DialTimeout: 200 * time.Millisecond, ReadTimeout: 200 * time.Millisecond, WriteTimeout: 200 * time.Millisecond, MaxRetries: -1})},
		{"stalled redis", redis.NewClient(&redis.Options{Addr: stalled.Addr().String(), ContextTimeoutEnabled: true, DialTimeout: 200 * time.Millisecond, ReadTimeout: 200 * time.Millisecond, WriteTimeout: 200 * time.Millisecond, MaxRetries: -1})},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer tt.cache.Close()
			server := New(db, tt.cache, nil)
			for _, method := range []string{"get", "search"} {
				t.Run(method, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
					defer cancel()
					started := time.Now()
					if method == "get" {
						f, e := server.GetFlight(ctx, &flightv1.GetFlightRequest{FlightId: id})
						requireCode(t, e, codes.OK)
						if f.Id != id {
							t.Fatal("wrong fallback flight")
						}
					} else {
						list, e := server.SearchFlights(ctx, &flightv1.SearchFlightsRequest{Origin: "SVO", Destination: "LED", Date: "2040-06-03"})
						requireCode(t, e, codes.OK)
						found := false
						for _, f := range list.Flights {
							if f.Id == id {
								found = true
							}
						}
						if !found {
							t.Fatal("fallback flight missing")
						}
					}
					elapsed := time.Since(started)
					t.Logf("PostgreSQL fallback with unavailable cache: %s", elapsed)
					if elapsed >= 250*time.Millisecond {
						t.Fatalf("optional cache consumed %s of a 500ms RPC", elapsed)
					}
				})
			}
		})
	}
}
