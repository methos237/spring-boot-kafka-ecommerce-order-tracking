ALTER TABLE orders
    ADD COLUMN payment_status   varchar(20) NOT NULL DEFAULT 'PENDING',
    ADD COLUMN inventory_status varchar(20) NOT NULL DEFAULT 'PENDING',
    ADD COLUMN cancel_reason    varchar(300),
    ADD COLUMN version          bigint      NOT NULL DEFAULT 0;

CREATE TABLE processed_events (
    event_id     uuid PRIMARY KEY,
    processed_at timestamptz NOT NULL
);
