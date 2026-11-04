package booking

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/mail"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	flightv1 "github.com/mchupryna/soa-flight-booking/gen/flight/v1"
	"github.com/mchupryna/soa-flight-booking/internal/api"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Config struct {
	RequestTimeout   time.Duration
	OperationTimeout time.Duration
	RecoveryInterval time.Duration
}

func DefaultConfig() Config { return Config{8 * time.Second, 10 * time.Second, time.Second} }

type Server struct {
	db      *sql.DB
	flights flightv1.FlightServiceClient
	logger  *slog.Logger
	cfg     Config
}

func New(db *sql.DB, flights flightv1.FlightServiceClient, logger *slog.Logger, cfg Config) *Server {
	d := DefaultConfig()
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = d.RequestTimeout
	}
	if cfg.OperationTimeout <= 0 {
		cfg.OperationTimeout = d.OperationTimeout
	}
	if cfg.RecoveryInterval <= 0 {
		cfg.RecoveryInterval = d.RecoveryInterval
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{db, flights, logger, cfg}
}

type failure struct {
	status int
	body   api.Error
}

func fail(code int, name, message string) *failure {
	return &failure{code, api.Error{ErrorCode: name, Message: message}}
}
func rpcFailure(err error) *failure {
	code := status.Code(err)
	httpCode := http.StatusBadGateway
	switch code {
	case codes.InvalidArgument:
		httpCode = 400
	case codes.NotFound:
		httpCode = 404
	case codes.ResourceExhausted, codes.FailedPrecondition:
		httpCode = 409
	case codes.Unavailable:
		httpCode = 503
	case codes.DeadlineExceeded:
		httpCode = 504
	}
	return fail(httpCode, code.String(), status.Convert(err).Message())
}
func storageFailure() *failure {
	return fail(503, "STORAGE_UNAVAILABLE", "booking storage is unavailable")
}
func progressFailure() *failure {
	return fail(503, "OPERATION_IN_PROGRESS", "operation is pending; retry with the same Idempotency-Key")
}
func unknownFailure() *failure {
	return fail(503, "UNKNOWN_OUTCOME", "commit outcome is unknown; retry with the same Idempotency-Key")
}
func jsonResponse(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
func writeFailure(w http.ResponseWriter, e *failure, op *operation) {
	if op != nil {
		e.body.OperationId = &op.id
		e.body.BookingId = &op.bookingID
	}
	jsonResponse(w, e.status, e.body)
}
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", home)
	mux.HandleFunc("GET /openapi.json", specification)
	mux.Handle("GET /swagger", http.RedirectHandler("/swagger/", http.StatusPermanentRedirect))
	swagger := http.FileServerFS(publicFiles)
	for _, path := range []string{"/swagger/{$}", "/swagger/swagger-ui.css", "/swagger/swagger-ui-bundle.js"} {
		mux.Handle("GET "+path, swagger)
	}
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		if e := s.db.PingContext(r.Context()); e != nil {
			writeFailure(w, storageFailure(), nil)
			return
		}
		jsonResponse(w, 200, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /flights", s.searchFlights)
	mux.HandleFunc("GET /flights/{id}", s.getFlight)
	mux.HandleFunc("POST /bookings", s.createBooking)
	mux.HandleFunc("GET /bookings", s.listBookings)
	mux.HandleFunc("GET /bookings/{id}", s.getBooking)
	mux.HandleFunc("POST /bookings/{id}/cancel", s.cancelBooking)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), s.cfg.RequestTimeout)
		defer cancel()
		mux.ServeHTTP(w, r.WithContext(ctx))
	})
}
func validUUID(v string) bool { _, e := uuid.Parse(v); return e == nil }
func normalize(r api.CreateBookingRequest) (api.CreateBookingRequest, error) {
	r.PassengerName = strings.TrimSpace(r.PassengerName)
	if r.UserId <= 0 || !validUUID(r.FlightId) || r.SeatCount <= 0 || r.PassengerName == "" || strings.ContainsRune(r.PassengerName, 0) || utf8.RuneCountInString(r.PassengerName) > 200 {
		return r, errors.New("user_id, flight_id, passenger_name or seat_count is invalid")
	}
	r.FlightId = uuid.MustParse(r.FlightId).String()
	email := string(r.PassengerEmail)
	a, e := mail.ParseAddress(email)
	if e != nil || a.Name != "" || a.Address != email || len(email) > 254 {
		return r, errors.New("passenger_email must be a plain email address of at most 254 bytes")
	}
	return r, nil
}
func totalPrice(price int64, seats int32) (int64, error) {
	if price <= 0 || seats <= 0 || price > math.MaxInt64/int64(seats) {
		return 0, errors.New("total price is outside the supported range")
	}
	return price * int64(seats), nil
}
func badRequest(w http.ResponseWriter, message string) {
	writeFailure(w, fail(400, "INVALID_ARGUMENT", message), nil)
}
func idFromPath(w http.ResponseWriter, r *http.Request) (string, bool) {
	id := r.PathValue("id")
	if !validUUID(id) {
		badRequest(w, "id must be a UUID")
		return "", false
	}
	return uuid.MustParse(id).String(), true
}
func flightDTO(f *flightv1.Flight) api.Flight {
	return api.Flight{Id: f.Id, Airline: f.Airline, FlightNumber: f.FlightNumber, Origin: f.Origin, Destination: f.Destination, DepartureTime: f.DepartureTime.AsTime(), ArrivalTime: f.ArrivalTime.AsTime(), TotalSeats: f.TotalSeats, AvailableSeats: f.AvailableSeats, Price: f.Price, Status: api.FlightStatus(f.Status.String())}
}
func iata(v string) bool {
	if len(v) != 3 {
		return false
	}
	for _, c := range v {
		if c < 'A' || c > 'Z' {
			return false
		}
	}
	return true
}
func (s *Server) searchFlights(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	origin, destination, date := q.Get("origin"), q.Get("destination"), q.Get("date")
	if !iata(origin) || !iata(destination) {
		badRequest(w, "origin and destination must be three uppercase IATA letters")
		return
	}
	if q.Has("date") {
		if _, e := time.Parse("2006-01-02", date); e != nil {
			badRequest(w, "date must be YYYY-MM-DD")
			return
		}
	}
	out, e := s.flights.SearchFlights(r.Context(), &flightv1.SearchFlightsRequest{Origin: origin, Destination: destination, Date: date})
	if e != nil {
		writeFailure(w, rpcFailure(e), nil)
		return
	}
	flights := make([]api.Flight, 0, len(out.Flights))
	for _, f := range out.Flights {
		flights = append(flights, flightDTO(f))
	}
	jsonResponse(w, 200, flights)
}
func (s *Server) getFlight(w http.ResponseWriter, r *http.Request) {
	id, ok := idFromPath(w, r)
	if !ok {
		return
	}
	f, e := s.flights.GetFlight(r.Context(), &flightv1.GetFlightRequest{FlightId: id})
	if e != nil {
		writeFailure(w, rpcFailure(e), nil)
		return
	}
	jsonResponse(w, 200, flightDTO(f))
}

const bookingColumns = `id,user_id,flight_id,passenger_name,passenger_email,seat_count,total_price,status,created_at`

type scanner interface{ Scan(...any) error }

func scanBooking(row scanner) (api.Booking, error) {
	var b api.Booking
	e := row.Scan(&b.Id, &b.UserId, &b.FlightId, &b.PassengerName, &b.PassengerEmail, &b.SeatCount, &b.TotalPrice, &b.Status, &b.CreatedAt)
	return b, e
}
func (s *Server) readBooking(ctx context.Context, id string) (api.Booking, error) {
	return scanBooking(s.db.QueryRowContext(ctx, `SELECT `+bookingColumns+` FROM bookings WHERE id=$1`, id))
}
func (s *Server) getBooking(w http.ResponseWriter, r *http.Request) {
	id, ok := idFromPath(w, r)
	if !ok {
		return
	}
	b, e := s.readBooking(r.Context(), id)
	if errors.Is(e, sql.ErrNoRows) {
		writeFailure(w, fail(404, "NOT_FOUND", "booking not found"), nil)
		return
	}
	if e != nil {
		writeFailure(w, storageFailure(), nil)
		return
	}
	jsonResponse(w, 200, b)
}
func (s *Server) listBookings(w http.ResponseWriter, r *http.Request) {
	user, e := strconv.ParseInt(r.URL.Query().Get("user_id"), 10, 64)
	if e != nil || user <= 0 {
		badRequest(w, "user_id must be a positive int64")
		return
	}
	rows, e := s.db.QueryContext(r.Context(), `SELECT `+bookingColumns+` FROM bookings WHERE user_id=$1 ORDER BY created_at,id`, user)
	if e != nil {
		writeFailure(w, storageFailure(), nil)
		return
	}
	defer rows.Close()
	result := make([]api.Booking, 0)
	for rows.Next() {
		b, e := scanBooking(rows)
		if e != nil {
			writeFailure(w, storageFailure(), nil)
			return
		}
		result = append(result, b)
	}
	if e := rows.Err(); e != nil {
		writeFailure(w, storageFailure(), nil)
		return
	}
	jsonResponse(w, 200, result)
}

type operation struct {
	id, bookingID, kind, state string
	request                    []byte
	errorStatus                sql.NullInt64
	errorCode, errorMessage    sql.NullString
	expired                    bool
}

const operationColumns = `id,booking_id,kind,state,request,error_status,error_code,error_message,expires_at<=clock_timestamp()`

func scanOperation(row scanner) (operation, error) {
	var o operation
	e := row.Scan(&o.id, &o.bookingID, &o.kind, &o.state, &o.request, &o.errorStatus, &o.errorCode, &o.errorMessage, &o.expired)
	return o, e
}
func (o operation) originalFailure() *failure {
	if o.errorStatus.Valid {
		return fail(int(o.errorStatus.Int64), o.errorCode.String, o.errorMessage.String)
	}
	return fail(503, "INTERRUPTED", "operation was interrupted before completion")
}
func (s *Server) beginCreate(ctx context.Context, key string, request []byte) (operation, bool, error) {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return operation{}, false, e
	}
	defer tx.Rollback()
	o, e := scanOperation(tx.QueryRowContext(ctx, `INSERT INTO booking_operations(id,booking_id,kind,state,idempotency_key,request,expires_at) VALUES($1,$2,'CREATE','PENDING',$3,$4,clock_timestamp()+$5*interval '1 second') ON CONFLICT(idempotency_key) DO NOTHING RETURNING `+operationColumns, uuid.NewString(), uuid.NewString(), key, request, s.cfg.OperationTimeout.Seconds()))
	fresh := true
	if errors.Is(e, sql.ErrNoRows) {
		fresh = false
		o, e = scanOperation(tx.QueryRowContext(ctx, `SELECT `+operationColumns+` FROM booking_operations WHERE idempotency_key=$1`, key))
	}
	if e != nil {
		return o, false, e
	}
	if e = tx.Commit(); e != nil {
		return o, false, e
	}
	return o, fresh, nil
}
func sameRequest(a, b []byte) bool {
	var x, y api.CreateBookingRequest
	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil {
		return false
	}
	return x == y
}
func (s *Server) replyOperation(w http.ResponseWriter, r *http.Request, o operation, success int) {
	switch o.state {
	case "DONE":
		b, e := s.readBooking(r.Context(), o.bookingID)
		if e != nil {
			writeFailure(w, storageFailure(), &o)
			return
		}
		jsonResponse(w, success, b)
	case "FAILED":
		writeFailure(w, o.originalFailure(), &o)
	default:
		writeFailure(w, progressFailure(), &o)
	}
}
func (s *Server) createBooking(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req api.CreateBookingRequest
	if e := dec.Decode(&req); e != nil {
		badRequest(w, "invalid booking JSON body")
		return
	}
	if e := dec.Decode(new(any)); e != io.EOF {
		badRequest(w, "body must contain exactly one JSON object")
		return
	}
	req, e := normalize(req)
	if e != nil {
		badRequest(w, e.Error())
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		key = uuid.NewString()
	} else if !validUUID(key) {
		badRequest(w, "Idempotency-Key must be a UUID")
		return
	} else {
		key = uuid.MustParse(key).String()
	}
	w.Header().Set("Idempotency-Key", key)
	body, _ := json.Marshal(req)
	o, fresh, e := s.beginCreate(r.Context(), key, body)
	if e != nil {
		if o.id != "" {
			writeFailure(w, storageFailure(), &o)
		} else {
			writeFailure(w, storageFailure(), nil)
		}
		return
	}
	if !sameRequest(o.request, body) {
		writeFailure(w, fail(409, "IDEMPOTENCY_CONFLICT", "Idempotency-Key was used for a different request"), &o)
		return
	}
	if !fresh {
		s.replyOperation(w, r, o, 201)
		return
	}
	flight, e := s.flights.GetFlight(r.Context(), &flightv1.GetFlightRequest{FlightId: req.FlightId})
	if e != nil {
		s.replyCreateFailure(w, r, o, rpcFailure(e))
		return
	}
	price, e := totalPrice(flight.Price, req.SeatCount)
	if e != nil {
		f := fail(502, "INVALID_UPSTREAM_PRICE", e.Error())
		s.replyCreateFailure(w, r, o, f)
		return
	}
	_, e = s.flights.ReserveSeats(r.Context(), &flightv1.ReserveSeatsRequest{FlightId: req.FlightId, SeatCount: req.SeatCount, BookingId: o.bookingID})
	if e != nil {
		s.replyCreateFailure(w, r, o, rpcFailure(e))
		return
	}
	b, f, unknown := s.finishCreate(r.Context(), o, req, price)
	if f != nil {
		if unknown {
			writeFailure(w, f, &o)
		} else {
			s.replyCreateFailure(w, r, o, f)
		}
		return
	}
	jsonResponse(w, 201, b)
}

// No SQL transaction survives a remote call. Expiry and finalization serialize on the journal row.
func (s *Server) finishCreate(ctx context.Context, o operation, req api.CreateBookingRequest, price int64) (api.Booking, *failure, bool) {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return api.Booking{}, storageFailure(), false
	}
	defer tx.Rollback()
	current, e := scanOperation(tx.QueryRowContext(ctx, `SELECT `+operationColumns+` FROM booking_operations WHERE id=$1 FOR UPDATE`, o.id))
	if e != nil {
		return api.Booking{}, storageFailure(), false
	}
	if current.state != "PENDING" || current.expired {
		return api.Booking{}, progressFailure(), false
	}
	b, e := scanBooking(tx.QueryRowContext(ctx, `INSERT INTO bookings(id,user_id,flight_id,passenger_name,passenger_email,seat_count,total_price,status) VALUES($1,$2,$3,$4,$5,$6,$7,'CONFIRMED') RETURNING `+bookingColumns, o.bookingID, req.UserId, req.FlightId, req.PassengerName, req.PassengerEmail, req.SeatCount, price))
	if e != nil {
		return b, storageFailure(), false
	}
	if _, e = tx.ExecContext(ctx, `UPDATE booking_operations SET state='DONE' WHERE id=$1`, o.id); e != nil {
		return b, storageFailure(), false
	}
	if e = tx.Commit(); e != nil {
		return api.Booking{}, unknownFailure(), true
	}
	return b, nil, false
}
func (s *Server) replyCreateFailure(w http.ResponseWriter, r *http.Request, o operation, f *failure) {
	current, e := s.abortCreate(o, f)
	if e != nil {
		writeFailure(w, storageFailure(), &o)
		return
	}
	if current.state == "DONE" {
		s.replyOperation(w, r, current, 201)
		return
	}
	writeFailure(w, current.originalFailure(), &current)
}
func (s *Server) abortCreate(o operation, f *failure) (operation, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.RequestTimeout)
	defer cancel()
	current, e := scanOperation(s.db.QueryRowContext(ctx, `UPDATE booking_operations SET state='COMPENSATING',error_status=$2,error_code=$3,error_message=$4 WHERE id=$1 AND state='PENDING' RETURNING `+operationColumns, o.id, f.status, f.body.ErrorCode, f.body.Message))
	if errors.Is(e, sql.ErrNoRows) {
		return scanOperation(s.db.QueryRowContext(ctx, `SELECT `+operationColumns+` FROM booking_operations WHERE id=$1`, o.id))
	}
	if e != nil {
		s.logger.Error("cannot persist booking compensation", "operation_id", o.id, "error", e)
		return o, e
	}
	s.recoverOperation(ctx, current)
	return current, nil
}
func (s *Server) cancelBooking(w http.ResponseWriter, r *http.Request) {
	id, ok := idFromPath(w, r)
	if !ok {
		return
	}
	o, b, fresh, e := s.beginCancel(r.Context(), id)
	if errors.Is(e, sql.ErrNoRows) {
		writeFailure(w, fail(404, "NOT_FOUND", "booking not found"), nil)
		return
	}
	if e != nil {
		writeFailure(w, storageFailure(), nil)
		return
	}
	if b.Status == api.BookingStatusCANCELLED {
		jsonResponse(w, 200, b)
		return
	}
	if !fresh {
		s.replyOperation(w, r, o, 200)
		return
	}
	_, e = s.flights.ReleaseReservation(r.Context(), &flightv1.ReleaseReservationRequest{BookingId: id})
	if e != nil {
		writeFailure(w, rpcFailure(e), &o)
		return
	}
	b, e = s.finishCancel(r.Context(), o)
	if e != nil {
		writeFailure(w, unknownFailure(), &o)
		return
	}
	jsonResponse(w, 200, b)
}
func (s *Server) beginCancel(ctx context.Context, id string) (operation, api.Booking, bool, error) {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return operation{}, api.Booking{}, false, e
	}
	defer tx.Rollback()
	b, e := scanBooking(tx.QueryRowContext(ctx, `SELECT `+bookingColumns+` FROM bookings WHERE id=$1 FOR UPDATE`, id))
	if e != nil {
		return operation{}, b, false, e
	}
	if b.Status == api.BookingStatusCANCELLED {
		return operation{}, b, false, nil
	}
	o, e := scanOperation(tx.QueryRowContext(ctx, `INSERT INTO booking_operations(id,booking_id,kind,state,expires_at) VALUES($1,$2,'CANCEL','COMPENSATING',clock_timestamp()) ON CONFLICT(booking_id,kind) DO NOTHING RETURNING `+operationColumns, uuid.NewString(), id))
	fresh := true
	if errors.Is(e, sql.ErrNoRows) {
		fresh = false
		o, e = scanOperation(tx.QueryRowContext(ctx, `SELECT `+operationColumns+` FROM booking_operations WHERE booking_id=$1 AND kind='CANCEL'`, id))
	}
	if e != nil {
		return o, b, false, e
	}
	if e = tx.Commit(); e != nil {
		return o, b, false, e
	}
	return o, b, fresh, nil
}
func (s *Server) finishCancel(ctx context.Context, o operation) (api.Booking, error) {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return api.Booking{}, e
	}
	defer tx.Rollback()
	if _, e = tx.ExecContext(ctx, `SELECT id FROM booking_operations WHERE id=$1 FOR UPDATE`, o.id); e != nil {
		return api.Booking{}, e
	}
	b, e := scanBooking(tx.QueryRowContext(ctx, `UPDATE bookings SET status='CANCELLED' WHERE id=$1 RETURNING `+bookingColumns, o.bookingID))
	if e != nil {
		return b, e
	}
	if _, e = tx.ExecContext(ctx, `UPDATE booking_operations SET state='DONE' WHERE id=$1`, o.id); e != nil {
		return b, e
	}
	return b, tx.Commit()
}
func (s *Server) RunRecovery(ctx context.Context) {
	s.recoverOnce(ctx)
	ticker := time.NewTicker(s.cfg.RecoveryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.recoverOnce(ctx)
		}
	}
}
func (s *Server) recoverOnce(ctx context.Context) {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		s.logger.Error("recovery storage unavailable", "error", e)
		return
	}
	defer tx.Rollback()
	rows, e := tx.QueryContext(ctx, `SELECT `+operationColumns+` FROM booking_operations WHERE state='COMPENSATING' OR (state='PENDING' AND expires_at<=clock_timestamp()) ORDER BY expires_at LIMIT 100 FOR UPDATE SKIP LOCKED`)
	if e != nil {
		s.logger.Error("recovery scan failed", "error", e)
		return
	}
	operations := make([]operation, 0)
	for rows.Next() {
		o, e := scanOperation(rows)
		if e != nil {
			rows.Close()
			s.logger.Error("recovery scan failed", "error", e)
			return
		}
		operations = append(operations, o)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		s.logger.Error("recovery scan failed", "error", e)
		return
	}
	for i := range operations {
		o := &operations[i]
		if o.state == "PENDING" {
			if _, e = tx.ExecContext(ctx, `UPDATE booking_operations SET state='COMPENSATING',error_status=COALESCE(error_status,503),error_code=COALESCE(error_code,'INTERRUPTED'),error_message=COALESCE(error_message,'operation was interrupted before completion') WHERE id=$1`, o.id); e != nil {
				s.logger.Error("recovery claim failed", "error", e)
				return
			}
			o.state = "COMPENSATING"
		}
	}
	if e = tx.Commit(); e != nil {
		s.logger.Error("recovery claim commit failed", "error", e)
		return
	}
	for _, o := range operations {
		if ctx.Err() != nil {
			return
		}
		rpcCtx, cancel := context.WithTimeout(ctx, s.cfg.RequestTimeout)
		s.recoverOperation(rpcCtx, o)
		cancel()
	}
}
func (s *Server) recoverOperation(ctx context.Context, o operation) {
	_, e := s.flights.ReleaseReservation(ctx, &flightv1.ReleaseReservationRequest{BookingId: o.bookingID})
	if e != nil {
		s.logger.Warn("booking cleanup will retry", "operation_id", o.id, "error", e)
		return
	}
	if o.kind == "CANCEL" {
		_, e = s.finishCancel(ctx, o)
	} else {
		_, e = s.db.ExecContext(ctx, `UPDATE booking_operations SET state='FAILED' WHERE id=$1 AND state='COMPENSATING'`, o.id)
	}
	if e != nil {
		s.logger.Error("booking cleanup persistence failed", "operation_id", o.id, "error", e)
	}
}
