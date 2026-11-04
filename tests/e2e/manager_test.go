//go:build e2e

package e2e

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	allure "github.com/allure-framework/allure-go/commons/gotest"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	flightv1 "github.com/mchupryna/soa-flight-booking/gen/flight/v1"
	"github.com/mchupryna/soa-flight-booking/internal/api"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type scenario struct {
	t            *testing.T
	a            *allure.Context
	db, bookings *sql.DB
	flight       flightv1.FlightServiceClient
	http         *http.Client
	base         string
	ctx          context.Context
	flights, ids []string
}

var runDir string

func TestMain(m *testing.M) {
	base := os.Getenv("E2E_ARTIFACTS_DIR")
	if base == "" {
		base = "../../artifacts/e2e"
	}
	id := strings.ToUpper(strings.ReplaceAll(uuid.NewString(), "-", "")[:10])
	runDir = filepath.Join(base, id)
	if err := os.MkdirAll(filepath.Join(runDir, "allure-results"), 0755); err != nil {
		fmt.Fprintln(os.Stderr, "create E2E artifacts:", err)
		os.Exit(1)
	}
	if err := os.WriteFile(filepath.Join(base, "latest-run"), []byte(id+"\n"), 0644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := os.Setenv("ALLURE_RESULTS_DIR", filepath.Join(runDir, "allure-results")); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

func runCase(t *testing.T, name string, fn func(*scenario)) {
	t.Helper()
	allure.Test(t, name, func(a *allure.Context) {
		s := &scenario{t: a.T(), a: a, http: &http.Client{Timeout: 15 * time.Second}, base: strings.TrimRight(required(a.T(), "E2E_BASE_URL"), "/")}
		s.ctx = metadata.AppendToOutgoingContext(s.t.Context(), "x-api-key", required(s.t, "FLIGHT_API_KEY"))
		s.db = s.openDB("E2E_FLIGHT_DATABASE_URL")
		s.bookings = s.openDB("E2E_BOOKING_DATABASE_URL")
		conn, err := grpc.NewClient(required(s.t, "E2E_FLIGHT_ADDR"), grpc.WithTransportCredentials(insecure.NewCredentials()))
		s.must(err)
		s.t.Cleanup(func() { conn.Close() })
		s.flight = flightv1.NewFlightServiceClient(conn)
		s.t.Cleanup(s.cleanup)
		defer func() {
			if s.t.Failed() {
				s.logs("flight")
				s.logs("booking")
			}
		}()
		fn(s)
	}, allure.WithSuite("Flight Booking E2E"), allure.WithFeature(name), allure.WithTestCaseID(name))
}
func required(t *testing.T, name string) string {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		t.Fatalf("required E2E environment %s is missing", name)
	}
	return v
}
func (s *scenario) must(err error) {
	s.t.Helper()
	if err != nil {
		s.t.Fatal(s.redact(err.Error()))
	}
}
func (s *scenario) equal(got, want any) {
	s.t.Helper()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		s.t.Fatalf("got %v, want %v", got, want)
	}
}
func (s *scenario) openDB(env string) *sql.DB {
	s.t.Helper()
	db, err := sql.Open("pgx", required(s.t, env))
	s.must(err)
	s.t.Cleanup(func() { db.Close() })
	ctx, cancel := context.WithTimeout(s.t.Context(), 5*time.Second)
	defer cancel()
	s.must(db.PingContext(ctx))
	return db
}
func (s *scenario) attach(name string, v any) {
	data, err := json.MarshalIndent(v, "", "  ")
	s.must(err)
	s.a.Attachment(name, []byte(s.redact(string(data))), "application/json")
}
func (s *scenario) redact(v string) string {
	for _, name := range []string{"FLIGHT_API_KEY", "E2E_FLIGHT_DATABASE_URL", "E2E_BOOKING_DATABASE_URL"} {
		if secret := os.Getenv(name); secret != "" {
			v = strings.ReplaceAll(v, secret, "[REDACTED]")
		}
	}
	v = strings.ReplaceAll(v, "E2E Passenger", "[REDACTED]")
	return regexp.MustCompile(`[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}`).ReplaceAllString(v, "[REDACTED EMAIL]")
}
func (s *scenario) remember(id string) { s.ids = append(s.ids, id) }
func (s *scenario) fixture(seats int, price int64, state string, departure time.Time) string {
	s.t.Helper()
	id := uuid.NewString()
	s.flights = append(s.flights, id)
	tx, err := s.db.BeginTx(s.t.Context(), nil)
	s.must(err)
	defer tx.Rollback()
	_, err = tx.Exec(`INSERT INTO flights(id,airline_id,flight_number,origin,destination,departure_time,arrival_time,total_seats,available_seats,price,status) VALUES($1,(SELECT id FROM airlines ORDER BY id LIMIT 1),$2,'QAA','QBB',$3,$4,$5,$5,$6,$7)`, id, "E2E-"+id, departure, departure.Add(time.Hour), seats, price, state)
	s.must(err)
	_, err = tx.Exec(`UPDATE cache_revision SET revision=revision+1 WHERE singleton`)
	s.must(err)
	s.must(tx.Commit())
	s.attach("Fixture flight", map[string]any{"id": id, "seats": seats, "price": price, "status": state, "departure_utc": departure.UTC()})
	return id
}
func (s *scenario) newFlight(seats int, price int64) string {
	return s.fixture(seats, price, "SCHEDULED", time.Date(2037, 4, 2, 12, 0, 0, 0, time.UTC))
}
func (s *scenario) cleanup() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Only IDs allocated by this case are removed; other Compose users keep their data.
	for _, fid := range s.flights {
		rows, err := s.bookings.QueryContext(ctx, `SELECT booking_id::text FROM booking_operations WHERE request->>'flight_id'=$1::text UNION SELECT id::text FROM bookings WHERE flight_id=$1::uuid`, fid)
		if err == nil {
			for rows.Next() {
				var id string
				if rows.Scan(&id) == nil {
					s.ids = append(s.ids, id)
				}
			}
			rows.Close()
		} else {
			s.t.Error("read cleanup IDs:", s.redact(err.Error()))
		}
	}
	for _, id := range s.ids {
		for _, q := range []string{`DELETE FROM booking_operations WHERE booking_id=$1`, `DELETE FROM bookings WHERE id=$1`} {
			if _, err := s.bookings.ExecContext(ctx, q, id); err != nil {
				s.t.Error("cleanup booking:", s.redact(err.Error()))
			}
		}
		for _, q := range []string{`DELETE FROM seat_reservations WHERE booking_id=$1`, `DELETE FROM reservation_cancellations WHERE booking_id=$1`} {
			if _, err := s.db.ExecContext(ctx, q, id); err != nil {
				s.t.Error("cleanup reservation:", s.redact(err.Error()))
			}
		}
	}
	for _, fid := range s.flights {
		if _, err := s.db.ExecContext(ctx, `DELETE FROM flights WHERE id=$1`, fid); err != nil {
			s.t.Error("cleanup flight:", s.redact(err.Error()))
		}
	}
	if len(s.flights) > 0 {
		if _, err := s.db.ExecContext(ctx, `UPDATE cache_revision SET revision=revision+1 WHERE singleton`); err != nil {
			s.t.Error("cleanup cache revision:", s.redact(err.Error()))
		}
	}
}
func requestBody(fid string, seats int) map[string]any {
	return map[string]any{"user_id": int64(7182349001), "flight_id": fid, "passenger_name": "E2E Passenger", "passenger_email": "e2e@example.test", "seat_count": seats}
}
func (s *scenario) exchange(method, path string, body []byte, key string) (int, []byte, error) {
	req, err := http.NewRequestWithContext(s.t.Context(), method, s.base+path, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := s.http.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, data, err
}
func (s *scenario) raw(method, path string, body []byte, key string, want ...int) []byte {
	s.t.Helper()
	code, data, err := s.exchange(method, path, body, key)
	s.must(err)
	var clean any
	if json.Unmarshal(data, &clean) != nil {
		clean = "[non-JSON response]"
	}
	if obj, ok := clean.(map[string]any); ok {
		delete(obj, "passenger_name")
		delete(obj, "passenger_email")
	}
	if list, ok := clean.([]any); ok {
		for _, item := range list {
			if obj, ok := item.(map[string]any); ok {
				delete(obj, "passenger_name")
				delete(obj, "passenger_email")
			}
		}
	}
	s.attach(method+" "+path, map[string]any{"status": code, "response": clean})
	accepted := false
	for _, allowed := range want {
		if code == allowed {
			accepted = true
		}
	}
	if !accepted {
		s.t.Fatalf("HTTP status=%d, want one of %v", code, want)
	}
	if code >= 400 {
		var failure api.Error
		s.must(json.Unmarshal(data, &failure))
		if failure.ErrorCode == "" || failure.Message == "" {
			s.t.Fatal("error response requires error_code and message")
		}
	}
	return data
}
func (s *scenario) request(method, path string, body any, key string, want int) []byte {
	s.t.Helper()
	var data []byte
	if body != nil {
		var err error
		data, err = json.Marshal(body)
		s.must(err)
	}
	return s.raw(method, path, data, key, want)
}
func (s *scenario) create(fid string, seats int, key string) api.Booking {
	s.t.Helper()
	var b api.Booking
	s.must(json.Unmarshal(s.request("POST", "/bookings", requestBody(fid, seats), key, 201), &b))
	s.remember(b.Id)
	return b
}
func (s *scenario) booking(id string) api.Booking {
	s.t.Helper()
	var b api.Booking
	s.must(json.Unmarshal(s.request("GET", "/bookings/"+id, nil, "", 200), &b))
	return b
}
func (s *scenario) seats(fid string) int {
	s.t.Helper()
	var n int
	s.must(s.db.QueryRow(`SELECT available_seats FROM flights WHERE id=$1`, fid).Scan(&n))
	s.attach("SQL available_seats", map[string]any{"flight_id": fid, "available_seats": n})
	return n
}
func (s *scenario) countBookings(fid string) int {
	s.t.Helper()
	var n int
	s.must(s.bookings.QueryRow(`SELECT count(*) FROM bookings WHERE flight_id=$1`, fid).Scan(&n))
	s.attach("SQL booking count", map[string]any{"flight_id": fid, "bookings": n})
	return n
}
func (s *scenario) poll(timeout time.Duration, description string, fn func() bool) {
	s.t.Helper()
	ctx, cancel := context.WithTimeout(s.t.Context(), timeout)
	defer cancel()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		if fn() {
			return
		}
		select {
		case <-ctx.Done():
			s.t.Fatalf("condition did not become true: %s", description)
		case <-tick.C:
		}
	}
}
func (s *scenario) container(service string) string {
	s.t.Helper()
	project := required(s.t, "E2E_COMPOSE_PROJECT")
	out, err := s.docker("ps", "-aq", "--filter", "label=com.docker.compose.project="+project, "--filter", "label=com.docker.compose.service="+service)
	s.must(err)
	ids := strings.Fields(out)
	if len(ids) != 1 {
		s.t.Fatalf("expected one %s container in E2E Compose project, got %d", service, len(ids))
	}
	return ids[0]
}
func (s *scenario) docker(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	return string(out), err
}
func (s *scenario) logs(service string) {
	id := s.container(service)
	out, err := s.docker("logs", "--tail", "180", id)
	if err == nil {
		s.a.Attachment(service+" logs", []byte(s.redact(out)), "text/plain")
	}
}
func (s *scenario) stop(service string, kill bool) string {
	s.t.Helper()
	id := s.container(service)
	s.t.Cleanup(func() {
		if _, err := s.docker("start", id); err != nil {
			s.t.Error("restore container:", service, err)
		}
	})
	var err error
	if kill {
		_, err = s.docker("kill", "--signal", "KILL", id)
	} else {
		_, err = s.docker("stop", "--time", "2", id)
	}
	s.must(err)
	return id
}
func (s *scenario) start(id string) { _, err := s.docker("start", id); s.must(err) }
func (s *scenario) redis() *redis.Client {
	s.t.Helper()
	sentinels := strings.Split(required(s.t, "E2E_REDIS_SENTINELS"), ",")
	c := redis.NewFailoverClient(&redis.FailoverOptions{MasterName: required(s.t, "E2E_REDIS_MASTER_NAME"), SentinelAddrs: sentinels, DialTimeout: 500 * time.Millisecond, DialerRetries: 1, ReadTimeout: 500 * time.Millisecond, WriteTimeout: 500 * time.Millisecond, PoolTimeout: 500 * time.Millisecond, ContextTimeoutEnabled: true, MaxRetries: -1})
	s.t.Cleanup(func() { c.Close() })
	var lastError error
	defer func() {
		if s.t.Failed() {
			s.sentinelDiagnostics(c, fmt.Sprint(lastError))
		}
	}()
	s.poll(15*time.Second, "Sentinel-aware Redis client ready", func() bool {
		ctx, cancel := context.WithTimeout(s.t.Context(), 1500*time.Millisecond)
		defer cancel()
		lastError = c.Ping(ctx).Err()
		return lastError == nil
	})
	return c
}
func (s *scenario) revision() string {
	var namespace string
	var revision int64
	s.must(s.db.QueryRow(`SELECT namespace::text,revision FROM cache_revision WHERE singleton`).Scan(&namespace, &revision))
	return namespace + ":" + strconv.FormatInt(revision, 10)
}

func (s *scenario) slowPartialBody() {
	s.t.Helper()
	timeout := 8 * time.Second
	if value := os.Getenv("E2E_HTTP_TIMEOUT"); value != "" {
		var err error
		timeout, err = time.ParseDuration(value)
		s.must(err)
		if timeout <= 0 {
			s.t.Fatal("E2E_HTTP_TIMEOUT must be positive")
		}
	}
	target, err := url.Parse(s.base)
	s.must(err)
	connection, err := net.DialTimeout("tcp", target.Host, 2*time.Second)
	s.must(err)
	defer connection.Close()
	s.must(connection.SetDeadline(time.Now().Add(timeout + 2*time.Second)))
	started := time.Now()
	_, err = fmt.Fprintf(connection, "POST /bookings HTTP/1.1\r\nHost: %s\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{\"user_id\":", target.Host)
	s.must(err)
	response, err := http.ReadResponse(bufio.NewReader(connection), &http.Request{Method: "POST"})
	elapsed := time.Since(started)
	if err != nil {
		if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
			s.t.Fatalf("server did not bound a partial request body: %v", err)
		}
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			s.t.Fatalf("partial body response: %v", err)
		}
		s.attach("Partial body connection closed by server", map[string]any{"milliseconds": elapsed.Milliseconds()})
		return
	}
	defer response.Body.Close()
	if response.StatusCode != 400 && response.StatusCode != 408 {
		s.t.Fatalf("partial body status=%d, want 400 or 408", response.StatusCode)
	}
	s.attach("Partial body rejected by server", map[string]any{"status": response.StatusCode, "milliseconds": elapsed.Milliseconds()})
}
