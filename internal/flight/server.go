package flight

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	flightv1 "github.com/mchupryna/soa-flight-booking/gen/flight/v1"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const cacheTTL = 300 * time.Second
const cacheTimeout = 50 * time.Millisecond
const flightColumns = `f.id::text, a.name, f.flight_number, f.origin, f.destination, f.departure_time, f.arrival_time, f.total_seats, f.available_seats, f.price, f.status`
const flightFrom = ` FROM flights f JOIN airlines a ON a.id=f.airline_id`

var iata = regexp.MustCompile(`^[A-Z]{3}$`)

type Server struct {
	flightv1.UnimplementedFlightServiceServer
	db     *sql.DB
	cache  *redis.Client
	logger *slog.Logger
}

func New(db *sql.DB, cache *redis.Client, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{db: db, cache: cache, logger: logger}
}

func validUUID(id string) bool     { u, e := uuid.Parse(id); return e == nil && u.String() == id }
func invalid(message string) error { return status.Error(codes.InvalidArgument, message) }

// Convert storage failures at the boundary so Booking can safely retry outages.
func (s *Server) storageError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return status.FromContextError(ctx.Err()).Err()
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	if errors.Is(err, sql.ErrNoRows) {
		return status.Error(codes.NotFound, "flight not found")
	}
	var pgErr *pgconn.PgError
	var netErr net.Error
	unavailable := errors.Is(err, driver.ErrBadConn) || errors.Is(err, sql.ErrConnDone) || errors.As(err, &netErr)
	if errors.As(err, &pgErr) {
		unavailable = strings.HasPrefix(pgErr.Code, "08") || strings.HasPrefix(pgErr.Code, "53") || pgErr.Code == "57P01" || pgErr.Code == "57P02" || pgErr.Code == "57P03"
	}
	s.logger.ErrorContext(ctx, "flight storage error", "error", err)
	if unavailable {
		return status.Error(codes.Unavailable, "flight storage unavailable")
	}
	return status.Error(codes.Internal, "flight storage failure")
}

type scanner interface{ Scan(...any) error }

func scanFlight(row scanner) (*flightv1.Flight, error) {
	f := new(flightv1.Flight)
	var departure, arrival time.Time
	var state string
	if e := row.Scan(&f.Id, &f.Airline, &f.FlightNumber, &f.Origin, &f.Destination, &departure, &arrival, &f.TotalSeats, &f.AvailableSeats, &f.Price, &state); e != nil {
		return nil, e
	}
	f.DepartureTime = timestamppb.New(departure)
	f.ArrivalTime = timestamppb.New(arrival)
	f.Status = flightv1.FlightStatus(flightv1.FlightStatus_value[state])
	if e := f.DepartureTime.CheckValid(); e != nil {
		return nil, e
	}
	if e := f.ArrivalTime.CheckValid(); e != nil {
		return nil, e
	}
	return f, nil
}

// Namespace survives restarts; the revision commits atomically with every mutation.
// Old fills cannot reach keys read after a committed revision, even if Redis is down.
func (s *Server) cacheKey(ctx context.Context, suffix string) (string, error) {
	if s.cache == nil {
		return "", nil
	}
	var namespace string
	var revision int64
	e := s.db.QueryRowContext(ctx, `SELECT namespace::text, revision FROM cache_revision WHERE singleton`).Scan(&namespace, &revision)
	if e != nil {
		return "", e
	}
	kind, parameters, _ := strings.Cut(suffix, ":")
	return fmt.Sprintf("%s:%s:%d:%s", kind, namespace, revision, parameters), nil
}
func (s *Server) cacheGet(ctx context.Context, key string, dst proto.Message) bool {
	if key == "" {
		return false
	}
	cacheCtx, cancel := context.WithTimeout(ctx, cacheTimeout)
	defer cancel()
	data, e := s.cache.Get(cacheCtx, key).Bytes()
	if e == nil {
		e = protojson.Unmarshal(data, dst)
		if e == nil {
			s.logger.InfoContext(ctx, "cache hit", "key", key)
			return true
		}
	}
	if e != nil && !errors.Is(e, redis.Nil) {
		s.logger.WarnContext(ctx, "cache read failed", "key", key, "error", e)
	}
	proto.Reset(dst)
	s.logger.InfoContext(ctx, "cache miss", "key", key)
	return false
}
func (s *Server) cachePut(ctx context.Context, key string, value proto.Message) {
	if key == "" {
		return
	}
	data, e := protojson.Marshal(value)
	if e == nil {
		cacheCtx, cancel := context.WithTimeout(ctx, cacheTimeout)
		defer cancel()
		e = s.cache.Set(cacheCtx, key, data, cacheTTL).Err()
	}
	if e != nil {
		s.logger.WarnContext(ctx, "cache write failed", "key", key, "error", e)
	}
}

func (s *Server) GetFlight(ctx context.Context, r *flightv1.GetFlightRequest) (*flightv1.Flight, error) {
	if !validUUID(r.GetFlightId()) {
		return nil, invalid("flight_id must be a canonical UUID")
	}
	key, e := s.cacheKey(ctx, "flight:"+r.FlightId)
	if e != nil {
		return nil, s.storageError(ctx, e)
	}
	f := new(flightv1.Flight)
	if s.cacheGet(ctx, key, f) {
		return f, nil
	}
	f, e = scanFlight(s.db.QueryRowContext(ctx, `SELECT `+flightColumns+flightFrom+` WHERE f.id=$1`, r.FlightId))
	if e != nil {
		return nil, s.storageError(ctx, e)
	}
	s.cachePut(ctx, key, f)
	return f, nil
}

func (s *Server) SearchFlights(ctx context.Context, r *flightv1.SearchFlightsRequest) (*flightv1.SearchFlightsResponse, error) {
	if !iata.MatchString(r.GetOrigin()) || !iata.MatchString(r.GetDestination()) || r.GetOrigin() == r.GetDestination() {
		return nil, invalid("distinct origin and destination IATA codes are required")
	}
	var day time.Time
	if r.Date != "" {
		var e error
		day, e = time.Parse("2006-01-02", r.Date)
		if e != nil || day.Year() < 1 || day.Format("2006-01-02") != r.Date {
			return nil, invalid("date must be a real YYYY-MM-DD")
		}
	}
	key, e := s.cacheKey(ctx, "search:"+r.Origin+":"+r.Destination+":"+r.Date)
	if e != nil {
		return nil, s.storageError(ctx, e)
	}
	result := &flightv1.SearchFlightsResponse{Flights: []*flightv1.Flight{}}
	if s.cacheGet(ctx, key, result) {
		return result, nil
	}
	query := `SELECT ` + flightColumns + flightFrom + ` WHERE f.origin=$1 AND f.destination=$2 AND f.status='SCHEDULED'`
	args := []any{r.Origin, r.Destination}
	if r.Date != "" {
		query += ` AND f.departure_time >= $3 AND f.departure_time < $4`
		args = append(args, day, day.AddDate(0, 0, 1))
	}
	rows, e := s.db.QueryContext(ctx, query+` ORDER BY f.departure_time,f.id`, args...)
	if e != nil {
		return nil, s.storageError(ctx, e)
	}
	defer rows.Close()
	for rows.Next() {
		f, e := scanFlight(rows)
		if e != nil {
			return nil, s.storageError(ctx, e)
		}
		result.Flights = append(result.Flights, f)
	}
	if e := rows.Err(); e != nil {
		return nil, s.storageError(ctx, e)
	}
	s.cachePut(ctx, key, result)
	return result, nil
}

func lockBooking(ctx context.Context, tx *sql.Tx, id string) error {
	_, e := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, id)
	return e
}
func advanceRevision(ctx context.Context, tx *sql.Tx) error {
	_, e := tx.ExecContext(ctx, `UPDATE cache_revision SET revision=revision+1 WHERE singleton`)
	return e
}
func reservation(ctx context.Context, tx *sql.Tx, id string) (*flightv1.SeatReservation, error) {
	r := &flightv1.SeatReservation{BookingId: id}
	var state string
	e := tx.QueryRowContext(ctx, `SELECT flight_id::text,seat_count,status FROM seat_reservations WHERE booking_id=$1`, id).Scan(&r.FlightId, &r.SeatCount, &state)
	if e != nil {
		return nil, e
	}
	r.Status = flightv1.ReservationStatus(flightv1.ReservationStatus_value[state])
	return r, nil
}

func (s *Server) ReserveSeats(ctx context.Context, r *flightv1.ReserveSeatsRequest) (*flightv1.SeatReservation, error) {
	if !validUUID(r.GetFlightId()) || !validUUID(r.GetBookingId()) || r.GetSeatCount() <= 0 {
		return nil, invalid("canonical flight_id, booking_id and positive seat_count required")
	}
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return nil, s.storageError(ctx, e)
	}
	defer tx.Rollback()
	if e = lockBooking(ctx, tx, r.BookingId); e != nil {
		return nil, s.storageError(ctx, e)
	}
	var cancelled bool
	if e = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM reservation_cancellations WHERE booking_id=$1)`, r.BookingId).Scan(&cancelled); e != nil {
		return nil, s.storageError(ctx, e)
	}
	if cancelled {
		return nil, status.Error(codes.FailedPrecondition, "booking has been released")
	}
	existing, e := reservation(ctx, tx, r.BookingId)
	if e == nil {
		if existing.Status != flightv1.ReservationStatus_ACTIVE || existing.FlightId != r.FlightId || existing.SeatCount != r.SeatCount {
			return nil, status.Error(codes.FailedPrecondition, "booking_id already used with different parameters or released")
		}
		if e = tx.Commit(); e != nil {
			return nil, s.storageError(ctx, e)
		}
		return existing, nil
	}
	if !errors.Is(e, sql.ErrNoRows) {
		return nil, s.storageError(ctx, e)
	}
	var seats int32
	var state string
	if e = tx.QueryRowContext(ctx, `SELECT available_seats,status FROM flights WHERE id=$1 FOR UPDATE`, r.FlightId).Scan(&seats, &state); e != nil {
		return nil, s.storageError(ctx, e)
	}
	if state != "SCHEDULED" {
		return nil, status.Error(codes.FailedPrecondition, "flight is not scheduled")
	}
	if seats < r.SeatCount {
		return nil, status.Error(codes.ResourceExhausted, "insufficient seats")
	}
	if _, e = tx.ExecContext(ctx, `UPDATE flights SET available_seats=available_seats-$2 WHERE id=$1`, r.FlightId, r.SeatCount); e != nil {
		return nil, s.storageError(ctx, e)
	}
	if _, e = tx.ExecContext(ctx, `INSERT INTO seat_reservations(booking_id,flight_id,seat_count,status) VALUES($1,$2,$3,'ACTIVE')`, r.BookingId, r.FlightId, r.SeatCount); e != nil {
		return nil, s.storageError(ctx, e)
	}
	if e = advanceRevision(ctx, tx); e != nil {
		return nil, s.storageError(ctx, e)
	}
	if e = tx.Commit(); e != nil {
		return nil, s.storageError(ctx, e)
	}
	return &flightv1.SeatReservation{BookingId: r.BookingId, FlightId: r.FlightId, SeatCount: r.SeatCount, Status: flightv1.ReservationStatus_ACTIVE}, nil
}

func (s *Server) ReleaseReservation(ctx context.Context, r *flightv1.ReleaseReservationRequest) (*flightv1.ReleaseReservationResponse, error) {
	if !validUUID(r.GetBookingId()) {
		return nil, invalid("booking_id must be a canonical UUID")
	}
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return nil, s.storageError(ctx, e)
	}
	defer tx.Rollback()
	if e = lockBooking(ctx, tx, r.BookingId); e != nil {
		return nil, s.storageError(ctx, e)
	}
	if _, e = tx.ExecContext(ctx, `INSERT INTO reservation_cancellations(booking_id) VALUES($1) ON CONFLICT DO NOTHING`, r.BookingId); e != nil {
		return nil, s.storageError(ctx, e)
	}
	existing, e := reservation(ctx, tx, r.BookingId)
	if e != nil && !errors.Is(e, sql.ErrNoRows) {
		return nil, s.storageError(ctx, e)
	}
	if e == nil && existing.Status == flightv1.ReservationStatus_ACTIVE {
		var id string
		if e = tx.QueryRowContext(ctx, `SELECT id::text FROM flights WHERE id=$1 FOR UPDATE`, existing.FlightId).Scan(&id); e != nil {
			return nil, s.storageError(ctx, e)
		}
		if _, e = tx.ExecContext(ctx, `UPDATE flights SET available_seats=available_seats+$2 WHERE id=$1`, existing.FlightId, existing.SeatCount); e != nil {
			return nil, s.storageError(ctx, e)
		}
		if _, e = tx.ExecContext(ctx, `UPDATE seat_reservations SET status='RELEASED' WHERE booking_id=$1`, r.BookingId); e != nil {
			return nil, s.storageError(ctx, e)
		}
		if e = advanceRevision(ctx, tx); e != nil {
			return nil, s.storageError(ctx, e)
		}
	}
	if e = tx.Commit(); e != nil {
		return nil, s.storageError(ctx, e)
	}
	return &flightv1.ReleaseReservationResponse{BookingId: r.BookingId, Status: flightv1.ReservationStatus_RELEASED}, nil
}

func (s *Server) UpdateFlight(ctx context.Context, r *flightv1.UpdateFlightRequest) (*flightv1.Flight, error) {
	if !validUUID(r.GetFlightId()) {
		return nil, invalid("flight_id must be a canonical UUID")
	}
	if r.Price == nil && r.Status == nil {
		return nil, invalid("price or status is required")
	}
	if r.Price != nil && *r.Price <= 0 {
		return nil, invalid("price must be positive")
	}
	if r.Status != nil && (*r.Status < flightv1.FlightStatus_SCHEDULED || *r.Status > flightv1.FlightStatus_COMPLETED) {
		return nil, invalid("valid status is required")
	}
	var price, state any
	if r.Price != nil {
		price = *r.Price
	}
	if r.Status != nil {
		state = r.Status.String()
	}
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return nil, s.storageError(ctx, e)
	}
	defer tx.Rollback()
	result, e := tx.ExecContext(ctx, `UPDATE flights SET price=COALESCE($2,price),status=COALESCE($3,status) WHERE id=$1`, r.FlightId, price, state)
	if e != nil {
		return nil, s.storageError(ctx, e)
	}
	count, e := result.RowsAffected()
	if e != nil {
		return nil, s.storageError(ctx, e)
	}
	if count == 0 {
		return nil, status.Error(codes.NotFound, "flight not found")
	}
	if e = advanceRevision(ctx, tx); e != nil {
		return nil, s.storageError(ctx, e)
	}
	f, e := scanFlight(tx.QueryRowContext(ctx, `SELECT `+flightColumns+flightFrom+` WHERE f.id=$1`, r.FlightId))
	if e != nil {
		return nil, s.storageError(ctx, e)
	}
	if e = tx.Commit(); e != nil {
		return nil, s.storageError(ctx, e)
	}
	return f, nil
}
