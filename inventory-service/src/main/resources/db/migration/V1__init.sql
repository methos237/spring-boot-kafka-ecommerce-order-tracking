CREATE TABLE processed_events (
    event_id     uuid PRIMARY KEY,
    processed_at timestamptz NOT NULL
);

CREATE TABLE stock (
    sku       varchar(64) PRIMARY KEY,
    available integer NOT NULL CHECK (available >= 0)
);

CREATE TABLE reservations (
    id         uuid PRIMARY KEY,
    order_id   uuid        NOT NULL UNIQUE,
    created_at timestamptz NOT NULL
);
