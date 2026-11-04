package booking

import (
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mchupryna/soa-flight-booking/internal/api"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNormalizeRequest(t *testing.T) {
	valid := api.CreateBookingRequest{UserId: 1, FlightId: "d62ffafd-4808-40c5-b1a9-16cfade7a3ac", PassengerName: " Alice ", PassengerEmail: "alice@example.org", SeatCount: 2}
	got, e := normalize(valid)
	if e != nil || got.PassengerName != "Alice" {
		t.Fatalf("got=%+v error=%v", got, e)
	}
	for _, tc := range []struct {
		name   string
		change func(*api.CreateBookingRequest)
	}{
		{"zero user", func(r *api.CreateBookingRequest) { r.UserId = 0 }},
		{"invalid UUID", func(r *api.CreateBookingRequest) { r.FlightId = "bad" }},
		{"zero seats", func(r *api.CreateBookingRequest) { r.SeatCount = 0 }},
		{"blank name", func(r *api.CreateBookingRequest) { r.PassengerName = " \t" }},
		{"NUL name", func(r *api.CreateBookingRequest) { r.PassengerName = "A\x00B" }},
		{"long name", func(r *api.CreateBookingRequest) { r.PassengerName = strings.Repeat("я", 201) }},
		{"email display name", func(r *api.CreateBookingRequest) { r.PassengerEmail = "Alice <alice@example.org>" }},
		{"bad email", func(r *api.CreateBookingRequest) { r.PassengerEmail = "no@" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := valid
			tc.change(&r)
			if _, e := normalize(r); e == nil {
				t.Fatal("invalid request accepted")
			}
		})
	}
}
func TestCheckedPrice(t *testing.T) {
	for _, tc := range []struct {
		price int64
		seats int32
		want  int64
		bad   bool
	}{{1001, 3, 3003, false}, {math.MaxInt64, 1, math.MaxInt64, false}, {math.MaxInt64, 2, 0, true}, {0, 2, 0, true}, {-1, 2, 0, true}, {100, 0, 0, true}} {
		got, e := totalPrice(tc.price, tc.seats)
		if (e != nil) != tc.bad || got != tc.want {
			t.Fatalf("%+v got=%d error=%v", tc, got, e)
		}
	}
}
func TestGRPCErrorMapping(t *testing.T) {
	for _, tc := range []struct {
		code codes.Code
		http int
	}{{codes.InvalidArgument, 400}, {codes.NotFound, 404}, {codes.ResourceExhausted, 409}, {codes.FailedPrecondition, 409}, {codes.Unavailable, 503}, {codes.DeadlineExceeded, 504}, {codes.Unauthenticated, 502}, {codes.Internal, 502}} {
		got := rpcFailure(status.Error(tc.code, "upstream"))
		if got.status != tc.http {
			t.Fatalf("%v mapped to %d", tc.code, got.status)
		}
	}
	if got := rpcFailure(errors.New("bad")); got.status != 502 {
		t.Fatal(got)
	}
}
func TestInvalidRESTRequestsDoNotReachDependencies(t *testing.T) {
	h := New(nil, nil, nil, DefaultConfig()).Handler()
	for _, tc := range []struct{ method, path, body string }{
		{"POST", "/bookings", `{"user_id":1,"flight_id":"bad","passenger_name":"A","passenger_email":"a@b.com","seat_count":1}`},
		{"POST", "/bookings", `{"user_id":1,"flight_id":"d62ffafd-4808-40c5-b1a9-16cfade7a3ac","passenger_name":"A\u0000B","passenger_email":"a@b.com","seat_count":1}`},
		{"POST", "/bookings", `{"unknown":1}`}, {"POST", "/bookings", `{} {}`}, {"POST", "/bookings", strings.Repeat("x", (1<<20)+1)},
		{"GET", "/bookings", ""}, {"GET", "/bookings?user_id=-1", ""}, {"GET", "/bookings/no", ""}, {"GET", "/flights/no", ""}, {"GET", "/flights?origin=SVO", ""}, {"GET", "/flights?origin=SVO&destination=LED&date=2026-02-30", ""},
	} {
		t.Run(tc.path+tc.body[:min(20, len(tc.body))], func(t *testing.T) {
			r := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", w.Code, w.Body)
			}
		})
	}
}
