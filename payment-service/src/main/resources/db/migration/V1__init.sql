CREATE TABLE processed_events (
    event_id     uuid PRIMARY KEY,
    processed_at timestamptz NOT NULL
);

CREATE TABLE payments (
    id         uuid PRIMARY KEY,
    order_id   uuid           NOT NULL UNIQUE,
    amount     numeric(12, 2) NOT NULL,
    status     varchar(20)    NOT NULL,
    reason     varchar(200),
    created_at timestamptz    NOT NULL
);
