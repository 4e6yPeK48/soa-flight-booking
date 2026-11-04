CREATE TABLE airlines (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    code TEXT NOT NULL UNIQUE CHECK (code ~ '^[A-Z0-9]{2,3}$'),
    name TEXT NOT NULL CHECK (length(trim(name)) > 0)
);

CREATE TABLE flights (
    id UUID PRIMARY KEY,
    airline_id BIGINT NOT NULL REFERENCES airlines(id),
    flight_number TEXT NOT NULL CHECK (length(trim(flight_number)) > 0),
    origin TEXT NOT NULL CHECK (origin ~ '^[A-Z]{3}$'),
    destination TEXT NOT NULL CHECK (destination ~ '^[A-Z]{3}$' AND destination <> origin),
    departure_time TIMESTAMPTZ NOT NULL,
    arrival_time TIMESTAMPTZ NOT NULL CHECK (arrival_time > departure_time),
    total_seats INTEGER NOT NULL CHECK (total_seats > 0),
    available_seats INTEGER NOT NULL CHECK (available_seats >= 0 AND available_seats <= total_seats),
    price BIGINT NOT NULL CHECK (price > 0),
    status TEXT NOT NULL CHECK (status IN ('SCHEDULED', 'DEPARTED', 'CANCELLED', 'COMPLETED'))
);
CREATE UNIQUE INDEX flights_number_day ON flights (flight_number, ((departure_time AT TIME ZONE 'UTC')::date));
CREATE INDEX flights_route_date ON flights (origin, destination, departure_time) WHERE status = 'SCHEDULED';

CREATE TABLE seat_reservations (
    booking_id UUID PRIMARY KEY,
    flight_id UUID NOT NULL REFERENCES flights(id),
    seat_count INTEGER NOT NULL CHECK (seat_count > 0),
    status TEXT NOT NULL CHECK (status IN ('ACTIVE', 'RELEASED', 'EXPIRED'))
);
CREATE TABLE reservation_cancellations (
    booking_id UUID PRIMARY KEY
);

-- The revision commits with the data, including when a COMMIT response is lost.
CREATE TABLE cache_revision (
    singleton BOOLEAN PRIMARY KEY DEFAULT true CHECK (singleton),
    namespace UUID NOT NULL DEFAULT gen_random_uuid(),
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0)
);
INSERT INTO cache_revision DEFAULT VALUES;
