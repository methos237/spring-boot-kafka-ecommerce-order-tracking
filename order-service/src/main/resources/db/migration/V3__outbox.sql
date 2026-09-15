CREATE TABLE outbox (
    id         uuid PRIMARY KEY,
    order_id   uuid         NOT NULL,
    event_type varchar(200) NOT NULL,
    payload    bytea        NOT NULL,
    created_at timestamptz  NOT NULL
);

CREATE INDEX outbox_created_at_idx ON outbox (created_at);
