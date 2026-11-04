//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	"net"
	"strings"
	"time"

	"github.com/google/uuid"
	flightv1 "github.com/mchupryna/soa-flight-booking/gen/flight/v1"
)

func redisOutage(s *scenario) {
	fid := s.newFlight(5, 100)
	r := s.redis()
	_, err := s.flight.GetFlight(s.ctx, &flightv1.GetFlightRequest{FlightId: fid})
	s.must(err)
	first := s.stop("redis-master", false)
	second := s.stop("redis-replica", false)
	price := int64(350)
	_, err = s.flight.UpdateFlight(s.ctx, &flightv1.UpdateFlightRequest{FlightId: fid, Price: &price})
	s.must(err)
	f, err := s.flight.GetFlight(s.ctx, &flightv1.GetFlightRequest{FlightId: fid})
	s.must(err)
	s.equal(f.Price, price)
	found, err := s.flight.SearchFlights(s.ctx, &flightv1.SearchFlightsRequest{Origin: "QAA", Destination: "QBB"})
	s.must(err)
	s.equal(len(found.Flights), 1)
	s.equal(found.Flights[0].Price, price)
	b := s.create(fid, 2, uuid.NewString())
	s.equal(b.TotalPrice, 700)
	s.equal(s.seats(fid), 3)
	s.start(first)
	s.start(second)
	s.poll(45*time.Second, "Redis available after node restoration", func() bool {
		ctx, cancel := context.WithTimeout(s.t.Context(), 1500*time.Millisecond)
		defer cancel()
		return r.Ping(ctx).Err() == nil
	})
	f, err = s.flight.GetFlight(s.ctx, &flightv1.GetFlightRequest{FlightId: fid})
	s.must(err)
	s.equal(f.Price, price)
	s.equal(f.AvailableSeats, 3)
	s.request("POST", "/bookings/"+b.Id+"/cancel", nil, "", 200)
	s.equal(s.seats(fid), 5)
	s.logs("flight")
}
func sentinelFailover(s *scenario) {
	fid := s.newFlight(5, 100)
	r := s.redis()
	var lastRedisError string
	defer func() {
		if s.t.Failed() {
			s.sentinelDiagnostics(r, lastRedisError)
		}
	}()
	// Determine the live master instead of assuming the original role survived previous runs.
	masterService := ""
	for _, service := range []string{"redis-master", "redis-replica"} {
		out, err := s.docker("exec", s.container(service), "redis-cli", "--raw", "ROLE")
		s.must(err)
		if strings.HasPrefix(strings.TrimSpace(out), "master\n") {
			masterService = service
		}
	}
	if masterService == "" {
		s.t.Fatal("no live Redis master was found")
	}
	replicaService := "redis-master"
	if masterService == replicaService {
		replicaService = "redis-replica"
	}
	replicaID := s.container(replicaService)
	s.poll(30*time.Second, "replica synchronized before master loss", func() bool {
		out, err := s.docker("exec", replicaID, "redis-cli", "INFO", "replication")
		return err == nil && strings.Contains(out, "master_link_status:up")
	})
	oldID := s.stop(masterService, false)
	s.poll(45*time.Second, "Sentinel promoted the other Redis node", func() bool {
		out, err := s.docker("exec", replicaID, "redis-cli", "--raw", "ROLE")
		return err == nil && strings.HasPrefix(strings.TrimSpace(out), "master\n")
	})
	s.poll(20*time.Second, "Sentinel-aware client uses promoted master", func() bool {
		ctx, cancel := context.WithTimeout(s.t.Context(), 1500*time.Millisecond)
		defer cancel()
		err := r.Set(ctx, "e2e:"+fid, "promoted", time.Minute).Err()
		if err != nil {
			lastRedisError = err.Error()
		}
		return err == nil
	})
	s.t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		r.Del(ctx, "e2e:"+fid)
	})
	price := int64(456)
	_, err := s.flight.UpdateFlight(s.ctx, &flightv1.UpdateFlightRequest{FlightId: fid, Price: &price})
	s.must(err)
	f, err := s.flight.GetFlight(s.ctx, &flightv1.GetFlightRequest{FlightId: fid})
	s.must(err)
	s.equal(f.Price, price)
	key := "flight:" + s.revision() + ":" + fid
	s.poll(10*time.Second, "application populated cache on promoted master", func() bool {
		_, err = s.flight.GetFlight(s.ctx, &flightv1.GetFlightRequest{FlightId: fid})
		if err != nil {
			return false
		}
		ttl, e := r.TTL(s.t.Context(), key).Result()
		return e == nil && ttl > 0 && ttl <= 300*time.Second
	})
	s.attach("Sentinel failover", map[string]any{"stopped_service": masterService, "promoted_service": replicaService, "new_cache_key": key})
	s.start(oldID)
	s.poll(30*time.Second, "old master rejoins as connected replica", func() bool {
		out, err := s.docker("exec", oldID, "redis-cli", "INFO", "replication")
		return err == nil && strings.Contains(out, "role:slave") && strings.Contains(out, "master_link_status:up")
	})
}
func flightOutage(s *scenario) {
	fid := s.newFlight(3, 100)
	since := time.Now().UTC().Format(time.RFC3339Nano)
	id := s.stop("flight", false)
	for i := 0; i < 6; i++ {
		s.raw("GET", "/flights/"+fid, nil, "", 503, 504)
	}
	start := time.Now()
	body := s.request("GET", "/flights/"+fid, nil, "", 503)
	elapsed := time.Since(start)
	var failure struct {
		Message string `json:"message"`
	}
	s.must(json.Unmarshal(body, &failure))
	if !strings.Contains(failure.Message, "circuit breaker") {
		s.t.Fatalf("OPEN response lacks breaker explanation: %s", failure.Message)
	}
	if elapsed > time.Second {
		s.t.Fatalf("OPEN response took %s", elapsed)
	}
	key := uuid.NewString()
	s.request("POST", "/bookings", requestBody(fid, 1), key, 503)
	var bid string
	s.must(s.bookings.QueryRow(`SELECT booking_id::text FROM booking_operations WHERE idempotency_key=$1`, key).Scan(&bid))
	s.remember(bid)
	s.equal(s.countBookings(fid), 0)
	s.equal(s.seats(fid), 3)
	s.start(id)
	s.poll(50*time.Second, "circuit half-open probe succeeds and closes", func() bool {
		code, _, err := s.exchange("GET", "/flights/"+fid, nil, "")
		return err == nil && code == 200
	})
	s.poll(20*time.Second, "failed booking compensation completed after upstream recovery", func() bool {
		var state string
		err := s.bookings.QueryRow(`SELECT state FROM booking_operations WHERE idempotency_key=$1`, key).Scan(&state)
		return err == nil && state == "FAILED"
	})
	s.equal(s.countBookings(fid), 0)
	s.equal(s.seats(fid), 3)
	logs, err := s.docker("logs", "--since", since, s.container("booking"))
	s.must(err)
	for _, fragment := range []string{"retrying gRPC call", "OPEN", "HALF_OPEN", "CLOSED"} {
		if !strings.Contains(logs, fragment) {
			s.t.Fatalf("booking logs do not show %s", fragment)
		}
	}
	s.a.Attachment("Retry and circuit transitions", []byte(s.redact(logs)), "text/plain")
	s.attach("OPEN response duration", map[string]any{"milliseconds": elapsed.Milliseconds()})
}
func crashAfterReserve(s *scenario) {
	fid := s.newFlight(4, 100)
	key := uuid.NewString()
	lock, err := s.bookings.BeginTx(s.t.Context(), nil)
	s.must(err)
	defer lock.Rollback()
	_, err = lock.Exec(`LOCK TABLE bookings IN ACCESS EXCLUSIVE MODE`)
	s.must(err)
	done := make(chan error, 1)
	body, err := json.Marshal(requestBody(fid, 2))
	s.must(err)
	go func() { _, _, e := s.exchange("POST", "/bookings", body, key); done <- e }()
	var bid string
	s.poll(5*time.Second, "durable create intent and committed remote reservation", func() bool {
		if s.bookings.QueryRow(`SELECT booking_id::text FROM booking_operations WHERE idempotency_key=$1`, key).Scan(&bid) != nil {
			return false
		}
		var state string
		return s.db.QueryRow(`SELECT status FROM seat_reservations WHERE booking_id=$1`, bid).Scan(&state) == nil && state == "ACTIVE"
	})
	s.remember(bid)
	s.equal(s.seats(fid), 2)
	id := s.stop("booking", true)
	s.must(lock.Rollback())
	s.start(id)
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		s.t.Fatal("interrupted HTTP request did not finish")
	}
	s.poll(45*time.Second, "restart compensates orphaned reservation", func() bool {
		var op, state string
		var n int
		if s.bookings.QueryRow(`SELECT state,(SELECT count(*) FROM bookings WHERE id=o.booking_id) FROM booking_operations o WHERE idempotency_key=$1`, key).Scan(&op, &n) != nil {
			return false
		}
		if s.db.QueryRow(`SELECT status FROM seat_reservations WHERE booking_id=$1`, bid).Scan(&state) != nil {
			return false
		}
		return op == "FAILED" && state == "RELEASED" && n == 0
	})
	s.equal(s.seats(fid), 4)
	s.equal(s.countBookings(fid), 0)
	s.request("POST", "/bookings", requestBody(fid, 2), key, 503)
	s.equal(s.seats(fid), 4)
	s.attach("Post-crash recovery SQL", map[string]any{"booking_id": bid, "journal_state": "FAILED", "reservation": "RELEASED", "booking_rows": 0, "available_seats": 4})
	s.logs("booking")
}
func crashAfterRelease(s *scenario) {
	fid := s.newFlight(4, 100)
	b := s.create(fid, 2, uuid.NewString())
	// Stall Release until CANCEL intent commits, then stall the local finalization.
	flightLock, err := s.db.BeginTx(s.t.Context(), nil)
	s.must(err)
	defer flightLock.Rollback()
	_, err = flightLock.Exec(`SELECT booking_id FROM seat_reservations WHERE booking_id=$1 FOR UPDATE`, b.Id)
	s.must(err)
	done := make(chan error, 1)
	go func() { _, _, e := s.exchange("POST", "/bookings/"+b.Id+"/cancel", nil, ""); done <- e }()
	s.poll(4*time.Second, "durable cancel intent before remote release", func() bool {
		var n int
		err := s.bookings.QueryRow(`SELECT count(*) FROM booking_operations WHERE booking_id=$1 AND kind='CANCEL' AND state='COMPENSATING'`, b.Id).Scan(&n)
		return err == nil && n == 1
	})
	bookingLock, err := s.bookings.BeginTx(s.t.Context(), nil)
	s.must(err)
	defer bookingLock.Rollback()
	_, err = bookingLock.Exec(`SELECT id FROM bookings WHERE id=$1 FOR UPDATE`, b.Id)
	s.must(err)
	s.must(flightLock.Rollback())
	s.poll(4*time.Second, "remote release committed before local cancel status", func() bool {
		var state string
		return s.db.QueryRow(`SELECT status FROM seat_reservations WHERE booking_id=$1`, b.Id).Scan(&state) == nil && state == "RELEASED"
	})
	s.equal(s.seats(fid), 4)
	id := s.stop("booking", true)
	s.must(bookingLock.Rollback())
	s.start(id)
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		s.t.Fatal("interrupted cancel request did not finish")
	}
	s.poll(35*time.Second, "restart finishes cancellation without returning seats twice", func() bool {
		var state, op string
		err := s.bookings.QueryRow(`SELECT b.status,o.state FROM bookings b JOIN booking_operations o ON o.booking_id=b.id AND o.kind='CANCEL' WHERE b.id=$1`, b.Id).Scan(&state, &op)
		return err == nil && state == "CANCELLED" && op == "DONE"
	})
	s.equal(s.seats(fid), 4)
	s.equal(s.booking(b.Id).Status, "CANCELLED")
	s.request("POST", "/bookings/"+b.Id+"/cancel", nil, "", 200)
	s.equal(s.seats(fid), 4)
	s.attach("Post-crash cancel SQL", map[string]any{"booking_id": b.Id, "journal_state": "DONE", "booking_status": "CANCELLED", "reservation": "RELEASED", "available_seats": 4})
	s.logs("booking")
}

// Record discovery and DNS separately from ROLE: promotion can precede Sentinel's switch.
func (s *scenario) sentinelDiagnostics(r *redis.Client, lastError string) {
	s.attach("Sentinel client failure", map[string]any{"last_error": lastError, "max_retries": r.Options().MaxRetries, "dialer_retries": r.Options().DialerRetries, "context_timeout_enabled": r.Options().ContextTimeoutEnabled})
	for _, addr := range strings.Split(required(s.t, "E2E_REDIS_SENTINELS"), ",") {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		c := redis.NewSentinelClient(&redis.Options{Addr: addr, MaxRetries: -1, DialerRetries: 1, DialTimeout: 500 * time.Millisecond, ReadTimeout: 500 * time.Millisecond, WriteTimeout: 500 * time.Millisecond, ContextTimeoutEnabled: true})
		discovered, err := c.GetMasterAddrByName(ctx, required(s.t, "E2E_REDIS_MASTER_NAME")).Result()
		var addresses []string
		var dnsErr error
		if len(discovered) == 2 {
			addresses, dnsErr = net.DefaultResolver.LookupHost(ctx, discovered[0])
		}
		s.attach("Sentinel discovery", map[string]any{"sentinel": addr, "master": discovered, "discovery_error": fmt.Sprint(err), "master_dns": addresses, "dns_error": fmt.Sprint(dnsErr)})
		c.Close()
		cancel()
	}
	s.logs("sentinel-1")
	s.logs("sentinel-2")
	s.logs("sentinel-3")
}
