CREATE TABLE bookings (
    id UUID PRIMARY KEY,
    user_id BIGINT NOT NULL CHECK (user_id > 0),
    flight_id UUID NOT NULL,
    passenger_name TEXT NOT NULL CHECK (length(trim(passenger_name)) > 0),
    passenger_email TEXT NOT NULL CHECK (length(trim(passenger_email)) > 0),
    seat_count INTEGER NOT NULL CHECK (seat_count > 0),
    total_price BIGINT NOT NULL CHECK (total_price > 0),
    status TEXT NOT NULL CHECK (status IN ('CONFIRMED', 'CANCELLED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX bookings_user ON bookings(user_id, created_at, id);

CREATE TABLE booking_operations (
    id UUID PRIMARY KEY,
    booking_id UUID NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('CREATE', 'CANCEL')),
    state TEXT NOT NULL CHECK (state IN ('PENDING', 'COMPENSATING', 'DONE', 'FAILED')),
    idempotency_key UUID UNIQUE,
    request JSONB,
    error_status INTEGER,
    error_code TEXT,
    error_message TEXT,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (booking_id, kind),
    CHECK ((kind = 'CREATE' AND idempotency_key IS NOT NULL AND request IS NOT NULL) OR kind = 'CANCEL')
);
CREATE INDEX booking_operations_recovery ON booking_operations(expires_at) WHERE state IN ('PENDING', 'COMPENSATING');
