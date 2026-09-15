CREATE TABLE orders (
    id           uuid PRIMARY KEY,
    customer_id  varchar(100)   NOT NULL,
    total_amount numeric(12, 2) NOT NULL,
    status       varchar(20)    NOT NULL,
    created_at   timestamptz    NOT NULL,
    updated_at   timestamptz    NOT NULL
);

CREATE TABLE order_items (
    order_id   uuid           NOT NULL REFERENCES orders (id),
    sku        varchar(64)    NOT NULL,
    quantity   integer        NOT NULL,
    unit_price numeric(12, 2) NOT NULL
);

CREATE INDEX order_items_order_id_idx ON order_items (order_id);
