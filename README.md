# Order Tracking

Event-driven e-commerce order tracking built with Spring Boot 4 and Apache Kafka.

A customer places an order. Payment and inventory are handled by independent services reacting to Kafka events. The order service listens for their outcomes and drives the order to `CONFIRMED` or `CANCELLED`, refunding payment when inventory falls through. A notification service tails every topic and prints the order timeline.

## Architecture

```
POST /api/orders
      │
      ▼
┌──────────────┐  OrderPlaced   ┌───────────────────┐
│ order-service├───────────────►│ payment-service   │──► payment-events
│  (Postgres)  │   order-events │ inventory-service │──► inventory-events
│              │◄───────────────┤ (each consumes    │
│ saga state   │ Payment*/Inv*  │  order-events)    │
└──────┬───────┘                └───────────────────┘
       │ OrderConfirmed / OrderCancelled  (order-events)
       ▼
 notification-service  (consumes all three topics, logs timeline)
 payment-service       (on OrderCancelled: refund if it charged)
```

| Module | Role | Port |
|---|---|---|
| `common-events` | Shared event records and topic names | – |
| `order-service` | REST API, order persistence, saga state | 8080 |
| `payment-service` | Charges and refunds | 8081 |
| `inventory-service` | Stock reservation | 8082 |
| `notification-service` | Order timeline log | 8083 |

Topics: `order-events`, `payment-events`, `inventory-events`. Every record is keyed by `orderId`, so all events for one order land on one partition and stay ordered.

## Stack

Java 25 · Spring Boot 4.1 · Spring for Apache Kafka 4.1 · Apache Kafka 4 (KRaft) · PostgreSQL 16 · Flyway · Testcontainers 2 · Maven

## Prerequisites

- JDK 25
- Maven 3.9+
- Docker with Compose v2

## Run

```bash
docker compose up -d        # Kafka on 9092, Kafka UI on http://localhost:8090, Postgres on 5433
mvn -B verify               # build and run all tests (Testcontainers starts its own Kafka and Postgres)
```

Start a service from its packaged jar (build first with `mvn -B verify`, or `mvn -B package -DskipTests`):

```bash
java -jar order-service/target/order-service-0.1.0-SNAPSHOT.jar
```

Peek at a topic:

```bash
scripts/peek.sh order-events 5
```

## order-service

REST entry point. Persists the order as `PENDING` in Postgres (Flyway-managed schema) and publishes `OrderPlaced` to `order-events`, keyed by the order id. The producer runs with `acks=all` and idempotence enabled.

```bash
curl -i -X POST localhost:8080/api/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"customer-1","items":[{"sku":"SKU-1","quantity":2,"unitPrice":19.99}]}'
```

```http
HTTP/1.1 201
Location: /api/orders/5455e5c3-5f3a-4d58-8c03-fc59098ad6d4

{"id":"5455e5c3-...","customerId":"customer-1","items":[{"sku":"SKU-1","quantity":2,"unitPrice":19.99}],
 "totalAmount":39.98,"status":"PENDING","createdAt":"2026-09-15T02:51:36Z","updatedAt":"2026-09-15T02:51:36Z"}
```

`GET /api/orders/{id}` returns the same shape. Errors are RFC 9457 problem details: validation failures return 400 with an `errors` array listing each offending field, unknown ids return 404.

The total is computed server-side from the line items. Money is `BigDecimal` end to end, `numeric(12,2)` in Postgres.

Tests: a `@WebMvcTest` slice covers request validation and error mapping without a database or broker. A `@SpringBootTest` integration test starts real Postgres and Kafka with Testcontainers, places an order over HTTP, reads it back, and consumes the resulting `OrderPlaced` record to assert its key and payload.

## payment-service and inventory-service

Both consume `order-events` in their own consumer group (`payment-group`, `inventory-group`), so each sees every order independently. A class-level `@KafkaListener` routes records to a `@KafkaHandler` by payload type; event types the service does not care about fall through to a no-op default handler.

Business rules are deterministic so every failure path can be reproduced from the request:

| Service | Rule | Outcome event |
|---|---|---|
| payment | `totalAmount <= 1000.00` | `PaymentSucceeded` |
| payment | `totalAmount > 1000.00` | `PaymentFailed` |
| inventory | every sku known and `quantity <= available` | `InventoryReserved`, stock decremented |
| inventory | any sku unknown or short | `InventoryFailed`, no stock touched |

Seeded stock: `SKU-1` 100, `SKU-2` 10, `SKU-3` 0.

Inventory locks every requested sku row (`PESSIMISTIC_WRITE`) and checks all of them before decrementing any, so a multi-line order that fails on one line never leaves a partial reservation, and two orders for the same sku on different partitions cannot both pass the check.

### Idempotent consumers

Kafka delivers at least once. Rebalances, retries, and offset resets all replay records. Each consumer keeps a `processed_events` table keyed by `eventId`; the handler checks it, inserts the id, does its work, and publishes the outcome in one database transaction. A replayed record is logged and dropped. You can watch this by resetting a group's offsets:

```bash
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group payment-group --topic order-events --reset-offsets --to-earliest --execute
```

Restart `payment-service` and the outcome topics do not grow.

Tests for both services run against Testcontainers Kafka and Postgres: publish `OrderPlaced` records, wait for the row, drain the outcome topic for a fixed window and assert exactly one event per order, including when the same `OrderPlaced` is published twice.

## notification-service

One consumer group (`notification-group`) subscribed to all three topics. Every event becomes one log line:

```
[order=5175eff1-...] OrderPlaced customer=carol items=1 total=9.99
[order=5175eff1-...] PaymentSucceeded amount=9.99
[order=5175eff1-...] InventoryFailed reason=insufficient stock for SKU-3: requested 1, available 0
[order=5175eff1-...] OrderCancelled reason=inventory: insufficient stock for SKU-3: requested 1, available 0
[order=5175eff1-...] PaymentRefunded amount=9.99
```

Stateless by design: no database, no idempotency table. Logging a replayed event twice is harmless, and keeping the service free of state means it can be restarted, rewound, or scaled without coordination. A real system would hand each line to an email, SMS or push channel here, and that channel's delivery guarantees would decide whether a `processed_events` table becomes necessary.

Test: publishes one event per topic to Testcontainers Kafka and asserts the four timeline lines with `OutputCaptureExtension`.

## Resilience: retries and dead letters

Every consumer runs the same `DefaultErrorHandler`:

| Failure | Attempts | Then |
|---|---|---|
| Transient exception (`TransientDataAccessException`, broker hiccup, anything else) | 1 + 3 retries, 2 s apart | record published to `<topic>.DLT` |
| Deserialization failure (poison payload) | 1 | straight to `<topic>.DLT` |
| `IllegalArgumentException` | 1 | straight to `<topic>.DLT` |

Poison payloads never reach a handler: values are read through `ErrorHandlingDeserializer`, which turns a broken payload into a `DeserializationException` the error handler classifies as non-retryable. A failed attempt rolls back the whole transaction, including the `processed_events` insert, so a retry starts clean and the idempotency guard still holds once it succeeds.

Dead-letter records keep the original key and bytes plus headers `kafka_dlt-original-topic`, `kafka_dlt-original-offset`, `kafka_dlt-original-consumer-group` and `kafka_dlt-exception-message`. Producers use a serializer that passes `byte[]` through untouched and JSON-encodes everything else, so a poison payload lands on the DLT exactly as it arrived rather than as a base64 string.

`notification-service` tails all three `.DLT` topics with a raw-bytes consumer and logs each dead letter at `WARN` with those headers. That is the operator's view; nothing is retried from the DLT automatically.

Try it:

```bash
scripts/poison.sh order-events                  # one non-JSON record
scripts/peek.sh order-events.DLT 1              # it arrives here, payload intact
```

Expect one DLT record per consumer group that reads the topic: a poison record on `order-events` is dead-lettered by `payment-group`, `inventory-group` and `notification-group` independently, each tagged with its `kafka_dlt-original-consumer-group`. The next valid order still processes; a dead letter costs each consumer one record, not the partition.

Tests: `PaymentResilienceTest` publishes raw garbage to `order-events` and asserts the DLT record carries the original bytes and headers; it also spies on the repository to fail `save` twice with a transient exception and asserts three attempts, one row, one outcome event and nothing on the DLT. `DeadLetterListenerTest` asserts the WARN line for a poison record on `payment-events`.

## Saga: from PENDING to CONFIRMED or CANCELLED

`order-service` consumes `payment-events` and `inventory-events` in group `order-group` and applies each outcome to the order. Arrival order does not matter.

```
                 PaymentSucceeded            InventoryReserved
   PENDING ─────────────────────► payment ok ──────────────────► CONFIRMED
   payment=PENDING                                                  │ publishes OrderConfirmed
   inventory=PENDING
        │
        │ PaymentFailed  or  InventoryFailed  (first failure wins)
        ▼
   CANCELLED ── publishes OrderCancelled{reason}
        │
        └── payment-service: refund if it had charged ── publishes PaymentRefunded
```

Rules:

- Both steps `SUCCEEDED` moves the order to `CONFIRMED`.
- The first `FAILED` step moves the order to `CANCELLED` and records the reason (`payment: ...` or `inventory: ...`).
- Once terminal, later outcomes are recorded as processed and otherwise ignored. A late `PaymentSucceeded` on a cancelled order does not change anything.
- `@Version` on the order guards against two outcomes updating the same row concurrently once listener concurrency goes above one.

`GET /api/orders/{id}` exposes `status`, `paymentStatus`, `inventoryStatus` and `cancelReason`.

### Compensation

When inventory fails after payment succeeded, the customer has been charged for an order that will never ship. `payment-service` listens for `OrderCancelled`, and if it holds a `SUCCEEDED` payment for that order it marks it `REFUNDED` and publishes `PaymentRefunded`. A cancelled order whose payment already failed produces no refund. Same `processed_events` guard, so a redelivered `OrderCancelled` refunds once.

### Choreography over orchestration

There is no central saga coordinator. Each service reacts to events and publishes its own. That keeps services independent (payment knows nothing about inventory) and adding a participant means adding a consumer, not editing a coordinator. The cost is that the overall flow is implicit; this README and the `order-events` topic are the documentation. For a flow with three participants that trade-off favors choreography. An orchestrator earns its keep when the flow has many conditional steps or needs timeouts and manual intervention.

Tests: the saga test places a real order, publishes outcome events straight to the topics, and asserts the final status plus exactly one `OrderConfirmed` or `OrderCancelled` on `order-events` for that key, covering both arrival orders, a late success after cancellation, and a redelivered outcome. The payment test covers refund after success and no refund after failure.

## Status

Work in progress. All five modules are in place, the saga runs end to end, and failures retry then dead-letter. Next: observability and a load script.
