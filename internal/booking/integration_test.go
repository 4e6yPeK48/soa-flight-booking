package booking

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	flightv1 "github.com/mchupryna/soa-flight-booking/gen/flight/v1"
	"github.com/mchupryna/soa-flight-booking/internal/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The fake controls the remote boundary; all journal and booking writes use PostgreSQL.
type flightFake struct {
	flightv1.FlightServiceClient
	mu                             sync.Mutex
	getErr, reserveErr, releaseErr error
	gets, reserves, releases       int
	reserved                       map[string]bool
	afterGet, afterReserve         func()
	price                          int64
}

func (f *flightFake) GetFlight(ctx context.Context, r *flightv1.GetFlightRequest, _ ...grpc.CallOption) (*flightv1.Flight, error) {
	f.mu.Lock()
	f.gets++
	flight, err := &flightv1.Flight{Id: r.FlightId, Price: f.price, Status: flightv1.FlightStatus_SCHEDULED}, f.getErr
	f.mu.Unlock()
	if f.afterGet != nil {
		f.afterGet()
	}
	return flight, err
}
func (f *flightFake) ReserveSeats(ctx context.Context, r *flightv1.ReserveSeatsRequest, _ ...grpc.CallOption) (*flightv1.SeatReservation, error) {
	f.mu.Lock()
	f.reserves++
	if f.reserveErr != nil {
		err := f.reserveErr
		f.mu.Unlock()
		if f.afterReserve != nil {
			f.afterReserve()
		}
		return nil, err
	}
	f.reserved[r.BookingId] = true
	f.mu.Unlock()
	if f.afterReserve != nil {
		f.afterReserve()
	}
	return &flightv1.SeatReservation{BookingId: r.BookingId, Status: flightv1.ReservationStatus_ACTIVE}, nil
}
func (f *flightFake) ReleaseReservation(ctx context.Context, r *flightv1.ReleaseReservationRequest, _ ...grpc.CallOption) (*flightv1.ReleaseReservationResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.releases++
	if f.releaseErr != nil {
		return nil, f.releaseErr
	}
	delete(f.reserved, r.BookingId)
	return &flightv1.ReleaseReservationResponse{BookingId: r.BookingId, Status: flightv1.ReservationStatus_RELEASED}, nil
}
func bookingDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("BOOKING_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("BOOKING_TEST_DATABASE_URL is not set")
	}
	db, e := sql.Open("pgx", dsn)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { db.Close() })
	if e = db.Ping(); e != nil {
		t.Fatal(e)
	}
	return db
}
func fixture(t *testing.T) (*Server, *flightFake, *sql.DB, string, string) {
	t.Helper()
	db := bookingDB(t)
	f := &flightFake{price: 12345, reserved: map[string]bool{}}
	s := New(db, f, slog.New(slog.NewTextHandler(io.Discard, nil)), DefaultConfig())
	key, fid := uuid.NewString(), uuid.NewString()
	t.Cleanup(func() {
		var bid string
		_ = db.QueryRow(`SELECT booking_id FROM booking_operations WHERE idempotency_key=$1`, key).Scan(&bid)
		if bid != "" {
			_, _ = db.Exec(`DELETE FROM booking_operations WHERE booking_id=$1`, bid)
			_, _ = db.Exec(`DELETE FROM bookings WHERE id=$1`, bid)
		}
	})
	return s, f, db, key, fid
}
func payload(fid string) string {
	return `{"user_id":9837461,"flight_id":"` + fid + `","passenger_name":" Alice ","passenger_email":"alice@example.org","seat_count":2}`
}
func request(s *Server, method, path, body, key string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if key != "" {
		r.Header.Set("Idempotency-Key", key)
	}
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	return w
}
func mustBooking(t *testing.T, w *httptest.ResponseRecorder, code int) api.Booking {
	t.Helper()
	if w.Code != code {
		t.Fatalf("status=%d body=%s want=%d", w.Code, w.Body, code)
	}
	var b api.Booking
	if e := json.Unmarshal(w.Body.Bytes(), &b); e != nil {
		t.Fatal(e)
	}
	return b
}
func assertState(t *testing.T, db *sql.DB, key, want string, books int) {
	t.Helper()
	var state string
	var n int
	if e := db.QueryRow(`SELECT state,(SELECT count(*) FROM bookings b WHERE b.id=o.booking_id) FROM booking_operations o WHERE idempotency_key=$1`, key).Scan(&state, &n); e != nil {
		t.Fatal(e)
	}
	if state != want || n != books {
		t.Fatalf("state=%s bookings=%d want %s/%d", state, n, want, books)
	}
}
func TestPostgresCreateAndReplay(t *testing.T) {
	s, f, db, key, fid := fixture(t)
	b := mustBooking(t, request(s, "POST", "/bookings", payload(fid), key), 201)
	if b.TotalPrice != 24690 || b.Status != "CONFIRMED" || b.PassengerName != "Alice" {
		t.Fatalf("booking=%+v", b)
	}
	b2 := mustBooking(t, request(s, "POST", "/bookings", payload(fid), key), 201)
	if b.Id != b2.Id || f.gets != 1 || f.reserves != 1 {
		t.Fatalf("replay=%+v get=%d reserve=%d", b2, f.gets, f.reserves)
	}
	assertState(t, db, key, "DONE", 1)
	w := request(s, "POST", "/bookings", strings.Replace(payload(fid), `"seat_count":2`, `"seat_count":1`, 1), key)
	if w.Code != 409 {
		t.Fatalf("changed request=%d %s", w.Code, w.Body)
	}
	got := mustBooking(t, request(s, "GET", "/bookings/"+b.Id, "", ""), 200)
	if got.Id != b.Id {
		t.Fatal(got)
	}
	w = request(s, "GET", "/bookings?user_id=9837461", "", "")
	if w.Code != 200 {
		t.Fatal(w.Body)
	}
}
func TestPostgresFailedUpstreamNeverCreatesBooking(t *testing.T) {
	for _, stage := range []string{"get", "reserve"} {
		t.Run(stage, func(t *testing.T) {
			s, f, db, key, fid := fixture(t)
			if stage == "get" {
				f.getErr = status.Error(codes.NotFound, "flight missing")
			} else {
				f.reserveErr = status.Error(codes.ResourceExhausted, "no seats")
			}
			w := request(s, "POST", "/bookings", payload(fid), key)
			want := 404
			if stage == "reserve" {
				want = 409
			}
			if w.Code != want {
				t.Fatalf("%d %s", w.Code, w.Body)
			}
			assertState(t, db, key, "FAILED", 0)
			again := request(s, "POST", "/bookings", payload(fid), key)
			if again.Code != want || again.Body.String() != w.Body.String() {
				t.Fatalf("replay=%d %s original=%s", again.Code, again.Body, w.Body)
			}
			if f.gets != 1 {
				t.Fatalf("replay called Get %d times", f.gets)
			}
		})
	}
}
func TestPostgresPendingRecoveryAndNoDuplicateRPC(t *testing.T) {
	s, f, db, key, fid := fixture(t)
	entered, proceed := make(chan struct{}), make(chan struct{})
	f.afterReserve = func() { close(entered); <-proceed }
	done := make(chan *httptest.ResponseRecorder)
	go func() { done <- request(s, "POST", "/bookings", payload(fid), key) }()
	<-entered
	w := request(s, "POST", "/bookings", payload(fid), key)
	if w.Code != 503 {
		t.Fatalf("duplicate=%d %s", w.Code, w.Body)
	}
	_, e := db.Exec(`UPDATE booking_operations SET expires_at=now()-interval '1 second' WHERE idempotency_key=$1`, key)
	if e != nil {
		t.Fatal(e)
	}
	s.recoverOnce(context.Background())
	close(proceed)
	w = <-done
	assertInterruptedReplay(t, s, fid, key, w)
	s.recoverOnce(context.Background())
	assertState(t, db, key, "FAILED", 0)
	if len(f.reserved) != 0 {
		t.Fatal("late response leaked reserved seats")
	}
}
func TestPostgresCrashAfterReserveRecovery(t *testing.T) {
	s, f, db, key, fid := fixture(t)
	bid, oid := uuid.NewString(), uuid.NewString()
	body := payload(fid)
	_, e := db.Exec(`INSERT INTO booking_operations(id,booking_id,kind,state,idempotency_key,request,expires_at) VALUES($1,$2,'CREATE','PENDING',$3,$4,now()-interval '1 second')`, oid, bid, key, body)
	if e != nil {
		t.Fatal(e)
	}
	f.reserved[bid] = true
	s.recoverOnce(context.Background())
	assertState(t, db, key, "FAILED", 0)
	if f.reserved[bid] {
		t.Fatal("reservation leaked")
	}
}
func TestPostgresCancelRecovery(t *testing.T) {
	s, f, db, key, fid := fixture(t)
	b := mustBooking(t, request(s, "POST", "/bookings", payload(fid), key), 201)
	f.releaseErr = status.Error(codes.Unavailable, "offline")
	w := request(s, "POST", "/bookings/"+b.Id+"/cancel", "", "")
	if w.Code != 503 {
		t.Fatalf("cancel=%d %s", w.Code, w.Body)
	}
	got := mustBooking(t, request(s, "GET", "/bookings/"+b.Id, "", ""), 200)
	if got.Status != "CONFIRMED" {
		t.Fatal(got)
	}
	f.releaseErr = nil
	s.recoverOnce(context.Background())
	got = mustBooking(t, request(s, "GET", "/bookings/"+b.Id, "", ""), 200)
	if got.Status != "CANCELLED" || f.reserved[b.Id] {
		t.Fatalf("cancelled=%+v reservation=%v", got, f.reserved)
	}
	mustBooking(t, request(s, "POST", "/bookings/"+b.Id+"/cancel", "", ""), 200)
	var state string
	if e := db.QueryRow(`SELECT state FROM booking_operations WHERE booking_id=$1 AND kind='CANCEL'`, b.Id).Scan(&state); e != nil || state != "DONE" {
		t.Fatalf("state=%s error=%v", state, e)
	}
}
func TestPostgresOptionalKeyAndEmptyArrays(t *testing.T) {
	s, _, db, _, fid := fixture(t)
	w := request(s, "POST", "/bookings", payload(fid), "")
	b := mustBooking(t, w, 201)
	key := w.Header().Get("Idempotency-Key")
	if _, e := uuid.Parse(key); e != nil {
		t.Fatalf("generated key=%s", key)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM booking_operations WHERE booking_id=$1`, b.Id)
		_, _ = db.Exec(`DELETE FROM bookings WHERE id=$1`, b.Id)
	})
	w = request(s, "GET", "/bookings?user_id=9223372036854775807", "", "")
	if w.Code != http.StatusOK || strings.TrimSpace(w.Body.String()) != "[]" {
		t.Fatalf("empty list=%d %s", w.Code, w.Body)
	}
}

func TestPostgresFailedCleanupRemainsDurableAndReplaysOriginalError(t *testing.T) {
	s, f, db, key, fid := fixture(t)
	f.reserveErr = status.Error(codes.ResourceExhausted, "no seats")
	f.releaseErr = status.Error(codes.Unavailable, "offline")
	w := request(s, "POST", "/bookings", payload(fid), key)
	if w.Code != 409 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body)
	}
	assertState(t, db, key, "COMPENSATING", 0)
	pending := request(s, "POST", "/bookings", payload(fid), key)
	if pending.Code != 503 {
		t.Fatalf("pending=%d %s", pending.Code, pending.Body)
	}
	f.releaseErr = nil
	s.recoverOnce(context.Background())
	assertState(t, db, key, "FAILED", 0)
	replay := request(s, "POST", "/bookings", payload(fid), key)
	if replay.Code != 409 || replay.Body.String() != w.Body.String() {
		t.Fatalf("replay=%d %s original=%s", replay.Code, replay.Body, w.Body)
	}
}

func TestPostgresRecoveryNeverReleasesCommittedBooking(t *testing.T) {
	s, f, db, key, fid := fixture(t)
	b := mustBooking(t, request(s, "POST", "/bookings", payload(fid), key), 201)
	if _, e := db.Exec(`UPDATE booking_operations SET expires_at=now()-interval '1 second' WHERE idempotency_key=$1`, key); e != nil {
		t.Fatal(e)
	}
	s.recoverOnce(context.Background())
	assertState(t, db, key, "DONE", 1)
	if !f.reserved[b.Id] || f.releases != 0 {
		t.Fatalf("committed booking lost reservation; releases=%d", f.releases)
	}
}

func assertInterruptedReplay(t *testing.T, s *Server, fid, key string, original *httptest.ResponseRecorder) {
	t.Helper()
	var body api.Error
	if e := json.Unmarshal(original.Body.Bytes(), &body); e != nil {
		t.Fatal(e)
	}
	if original.Code != 503 || body.ErrorCode != "INTERRUPTED" {
		t.Fatalf("original=%d %s want=503 INTERRUPTED", original.Code, original.Body)
	}
	replay := request(s, "POST", "/bookings", payload(fid), key)
	if replay.Code != original.Code || replay.Body.String() != original.Body.String() {
		t.Fatalf("replay=%d %s original=%d %s", replay.Code, replay.Body, original.Code, original.Body)
	}
}

func TestPostgresRecoveryWinsBeforeDelayedRPCError(t *testing.T) {
	for _, stage := range []string{"get", "reserve"} {
		t.Run(stage, func(t *testing.T) {
			s, f, db, key, fid := fixture(t)
			entered, proceed := make(chan struct{}), make(chan struct{})
			pause := func() { close(entered); <-proceed }
			if stage == "get" {
				f.getErr = status.Error(codes.NotFound, "flight missing")
				f.afterGet = pause
			} else {
				f.reserveErr = status.Error(codes.ResourceExhausted, "no seats")
				f.afterReserve = pause
			}
			done := make(chan *httptest.ResponseRecorder)
			go func() { done <- request(s, "POST", "/bookings", payload(fid), key) }()
			<-entered
			if _, e := db.Exec(`UPDATE booking_operations SET expires_at=now()-interval '1 second' WHERE idempotency_key=$1`, key); e != nil {
				t.Fatal(e)
			}
			s.recoverOnce(context.Background())
			assertState(t, db, key, "FAILED", 0)
			close(proceed)
			original := <-done
			assertInterruptedReplay(t, s, fid, key, original)
			assertState(t, db, key, "FAILED", 0)
			if f.releases != 1 {
				t.Fatalf("late handler repeated cleanup: releases=%d", f.releases)
			}
		})
	}
}
