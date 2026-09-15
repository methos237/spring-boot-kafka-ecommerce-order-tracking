# Order Tracking

[![CI](https://github.com/methos237/spring-boot-kafka-ecommerce-order-tracking/actions/workflows/ci.yml/badge.svg)](https://github.com/methos237/spring-boot-kafka-ecommerce-order-tracking/actions/workflows/ci.yml)

Event-driven e-commerce order tracking built with Spring Boot 4 and Apache Kafka.

A customer places an order. Payment and inventory are handled by independent services reacting to Kafka events. The order service listens for their outcomes and drives the order to `CONFIRMED` or `CANCELLED`, refunding payment when inventory falls through. A notification service tails every topic and prints the order timeline. Failures retry, then dead-letter. Every consumer is idempotent. Everything is covered by tests that run against real Kafka and Postgres in Docker.

## Ten-minute tour

```bash
docker compose up -d --wait                 # Kafka 9092, Kafka UI http://localhost:8090, Postgres 5433
mvn -B verify                               # build + 31 tests (Testcontainers starts its own Kafka and Postgres)
for s in order payment inventory notification; do
  java -jar $s-service/target/$s-service-0.1.0-SNAPSHOT.jar > /tmp/$s.log 2>&1 &
done
scripts/load.sh                             # 50 orders: 30 confirm, 20 cancel, verified via GET
scripts/status.sh                           # consumer lag per group and partition, should read 0
scripts/poison.sh order-events              # one bad record ...
scripts/peek.sh order-events.DLT 3          # ... lands here with headers, one per consuming group
grep 'DEAD LETTER' /tmp/notification.log
```

Open Kafka UI to watch keys spread across the three partitions of `order-events` and the DLTs fill.

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
 notification-service  (consumes all three topics + DLTs, logs timeline and dead letters)
 payment-service       (on OrderCancelled: refund if it charged)
```

| Module | Role | Port | Database |
|---|---|---|---|
| `common-events` | Event records, topic names, Kafka serializer and interceptors | – | – |
| `order-service` | REST API, order persistence, saga state | 8080 | `orders` |
| `payment-service` | Charges and refunds | 8081 | `payments` |
| `inventory-service` | Stock reservation | 8082 | `inventory` |
| `notification-service` | Order timeline and dead letter log | 8083 | – |

### Event flow

| Topic | Event | Producer | Consumers |
|---|---|---|---|
| `order-events` | `OrderPlaced{customerId, items, totalAmount}` | order | payment, inventory, notification |
| `payment-events` | `PaymentSucceeded{amount}` / `PaymentFailed{reason}` | payment | order, notification |
| `inventory-events` | `InventoryReserved` / `InventoryFailed{reason}` | inventory | order, notification |
| `order-events` | `OrderConfirmed` / `OrderCancelled{reason}` | order | payment (refund), notification |
| `payment-events` | `PaymentRefunded{amount}` | payment | notification |
| `*.DLT` | original bytes + `kafka_dlt-*` headers | any consumer's error handler | notification |

Every record is keyed by `orderId`, so all events for one order land on one partition and stay ordered. Every event carries `eventId`, `orderId` and `occurredAt`.

## Stack

Java 25 · Spring Boot 4.1 · Spring for Apache Kafka 4.1 · Apache Kafka 4 (KRaft) · PostgreSQL 16 · Flyway · Micrometer + Prometheus · Testcontainers 2 · Maven · GitHub Actions

Prerequisites: JDK 25, Maven 3.9+, Docker with Compose v2.

## Services

### order-service

REST entry point. Persists the order as `PENDING` in Postgres (Flyway-managed schema) and publishes `OrderPlaced`. The producer runs with `acks=all` and idempotence enabled.

```bash
curl -i -X POST localhost:8080/api/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"customer-1","items":[{"sku":"SKU-1","quantity":2,"unitPrice":19.99}]}'
```

```http
HTTP/1.1 201
Location: /api/orders/5455e5c3-5f3a-4d58-8c03-fc59098ad6d4

{"id":"5455e5c3-...","customerId":"customer-1","items":[{"sku":"SKU-1","quantity":2,"unitPrice":19.99}],
 "totalAmount":39.98,"status":"PENDING","paymentStatus":"PENDING","inventoryStatus":"PENDING",
 "cancelReason":null,"createdAt":"2026-09-15T02:51:36Z","updatedAt":"2026-09-15T02:51:36Z"}
```

`GET /api/orders/{id}` returns the same shape with the current saga state. Errors are RFC 9457 problem details: validation failures return 400 with an `errors` array listing each offending field, unknown ids return 404. The total is computed server-side from the line items. Money is `BigDecimal` end to end, `numeric(12,2)` in Postgres.

### payment-service and inventory-service

Both consume `order-events` in their own consumer group, so each sees every order independently. A class-level `@KafkaListener` routes records to a `@KafkaHandler` by payload type; event types the service does not care about fall through to a no-op default handler.

Business rules are deterministic so every failure path can be reproduced from the request:

| Service | Rule | Outcome event |
|---|---|---|
| payment | `totalAmount <= 1000.00` | `PaymentSucceeded` |
| payment | `totalAmount > 1000.00` | `PaymentFailed` |
| inventory | every sku known and `quantity <= available` | `InventoryReserved`, stock decremented |
| inventory | any sku unknown or short | `InventoryFailed`, no stock touched |

Seeded stock: `SKU-1` 100, `SKU-2` 10, `SKU-3` 0.

Inventory locks every requested sku row (`PESSIMISTIC_WRITE`) and checks all of them before decrementing any, so a multi-line order that fails on one line never leaves a partial reservation, and two orders for the same sku on different partitions cannot both pass the check.

### notification-service

One consumer group subscribed to all three topics. Every event becomes one log line, and a second listener logs every dead letter:

```
[order=5175eff1-...] OrderPlaced customer=carol items=1 total=9.99
[order=5175eff1-...] PaymentSucceeded amount=9.99
[order=5175eff1-...] InventoryFailed reason=insufficient stock for SKU-3: requested 1, available 0
[order=5175eff1-...] OrderCancelled reason=inventory: insufficient stock for SKU-3: requested 1, available 0
[order=5175eff1-...] PaymentRefunded amount=9.99
[order=poison] DEAD LETTER topic=order-events.DLT key=poison from=order-events@6 group=payment-group exception=failed to deserialize payload=this is not json
```

Stateless by design: no database, no idempotency table. Logging a replayed event twice is harmless, and keeping the service free of state means it can be restarted, rewound, or scaled without coordination. A real system would hand each line to an email, SMS or push channel here.

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

- Both steps `SUCCEEDED` moves the order to `CONFIRMED`.
- The first `FAILED` step moves the order to `CANCELLED` and records the reason (`payment: ...` or `inventory: ...`).
- Once terminal, later outcomes are recorded as processed and otherwise ignored.
- `@Version` on the order guards against two outcomes updating the same row concurrently once listener concurrency goes above one.

**Compensation.** When inventory fails after payment succeeded, the customer has been charged for an order that will never ship. `payment-service` listens for `OrderCancelled`, and if it holds a `SUCCEEDED` payment for that order it marks it `REFUNDED` and publishes `PaymentRefunded`. A cancelled order whose payment already failed produces no refund.

## Idempotent consumers

Kafka delivers at least once. Rebalances, retries, and offset resets all replay records. Each consumer with side effects keeps a `processed_events` table keyed by `eventId`; the handler checks it, inserts the id, does its work, and publishes the outcome in one database transaction. A replayed record is logged and dropped. Watch it:

```bash
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group payment-group --topic order-events --reset-offsets --to-earliest --execute
```

Restart `payment-service` and the outcome topics do not grow.

## Transactional outbox

`order-service` never calls Kafka from a request or a listener. `OrderEventPublisher` writes the event's JSON bytes and class name to an `outbox` table inside the caller's transaction (`Propagation.MANDATORY`, so a caller without a transaction fails loudly). `OutboxRelay` runs every 500 ms:

```
SELECT * FROM outbox ORDER BY created_at LIMIT 100 FOR UPDATE SKIP LOCKED
send each row: key = orderId, value = stored bytes, header __TypeId__ = stored class
wait for the broker ack, then DELETE the row
commit
```

What this buys:

- An order and its `OrderPlaced` commit together or not at all. Kafka down during `POST /api/orders` still returns 201; the row waits and drains when the broker returns.
- The relay resends the stored bytes, so a duplicate send after a crash carries the same `eventId` and every consumer drops it.
- `SKIP LOCKED` lets a second relay instance run without double-sending.

What it costs: up to 500 ms added latency, one extra write per event, a poller. Change data capture (Debezium tailing the `outbox` table) removes the poller and is the usual next step in production.

Test: `OutboxRelayTest` pauses the Kafka container, places an order, asserts the row is saved and stays in the outbox while relay ticks fail, unpauses, and asserts the event arrives with the stored type header and the row is gone.

## Resilience: retries and dead letters

Every consumer runs the same `DefaultErrorHandler`:

| Failure | Attempts | Then |
|---|---|---|
| Transient exception (database, broker, anything unclassified) | 1 + 3 retries, 2 s apart | record published to `<topic>.DLT` |
| Deserialization failure (poison payload) | 1 | straight to `<topic>.DLT` |
| `IllegalArgumentException` | 1 | straight to `<topic>.DLT` |

Poison payloads never reach a handler: values are read through `ErrorHandlingDeserializer`, which turns a broken payload into a `DeserializationException` the error handler classifies as non-retryable. A failed attempt rolls back the whole transaction, including the `processed_events` insert, so a retry starts clean.

Dead-letter records keep the original key and bytes plus headers `kafka_dlt-original-topic`, `kafka_dlt-original-offset`, `kafka_dlt-original-consumer-group` and `kafka_dlt-exception-message`. Producers use `EventSerializer`, which passes `byte[]` through untouched and JSON-encodes everything else, so a poison payload lands on the DLT exactly as it arrived rather than as a base64 string. Expect one DLT record per consumer group that reads the topic. Nothing is retried from the DLT automatically; `notification-service` logs each one at `WARN` for an operator.

## Observability

- **Correlation.** `OrderIdHeaderInterceptor` (a Kafka `ProducerInterceptor`) stamps every outgoing record with an `X-Order-Id` header from its key. `OrderIdMdcInterceptor` (a Spring Kafka `RecordInterceptor`) reads it on the consumer side and puts `orderId` into the MDC for the duration of the listener call. The log pattern prints `[order=<id>]` on every line, so `grep order=<id> /tmp/*.log` reconstructs one order's journey across all four services.
- **Metrics.** Actuator exposes `health`, `info`, `metrics` and `prometheus` on every service. Micrometer's Kafka binder publishes client metrics including consumer lag (`kafka_consumer_fetch_manager_records_lag_max`).
- **Lag at a glance.** `scripts/status.sh` prints per-partition lag for every consumer group from the broker's point of view.

## Design decisions

- **Key by `orderId`.** Per-order ordering is what the saga needs; customer-level ordering is not. The key also drives the `X-Order-Id` header and the MDC.
- **Choreography, not orchestration.** No central coordinator. Each service reacts to events and publishes its own, so payment knows nothing about inventory and adding a participant means adding a consumer. The cost is an implicit flow, which this README and the `order-events` topic document. With three participants that trade favors choreography; an orchestrator earns its keep with many conditional steps, timeouts, or manual intervention.
- **Idempotency table over exactly-once.** A `processed_events` table is a few lines per service, works with any broker semantics, and survives offset resets. Kafka transactions chained with the database are a stretch item, not a baseline.
- **Transactional outbox in order-service, direct publish elsewhere.** The order service owns the saga's source of truth, so its events must never diverge from its rows: `OrderPlaced`, `OrderConfirmed` and `OrderCancelled` go through an outbox table committed with the state change, and a relay drains it to Kafka. Payment and inventory publish directly inside their handler transaction. A commit failure there re-runs the handler with the same input and re-publishes; the outcome differs only in `eventId`, and the saga treats a second outcome for a settled order as a no-op. The asymmetry is deliberate: the outbox costs a table and a poller per service, and only the saga owner needs the guarantee.
- **Dead letter topic per source topic, one partition.** Dead letters are rare, read by one operator-facing consumer, and their original partition is preserved in a header.
- **Flyway over `ddl-auto`.** Schema is code-reviewed SQL. Hibernate runs with `validate` and fails fast on drift. The saga columns arrived as `V2`, which is how schemas evolve in practice.
- **Deterministic failures.** Every failure path is reproducible from the request. No random inventory misses.
- **Testcontainers, not embedded Kafka or H2.** Tests run against the same Kafka and Postgres the services run against. CI does the same on GitHub Actions.

## Tests

31 tests across five modules, all in `mvn verify`.

| Level | Where | What |
|---|---|---|
| Unit | `common-events` | every event round-trips through Jackson |
| Web slice (`@WebMvcTest`) | order-service | 201 with `Location`, every invalid field listed, 404 mapping |
| Integration (`@SpringBootTest` + Testcontainers) | order-service | place over HTTP, read back, consume `OrderPlaced`; saga in both arrival orders, late outcome ignored, redelivery applied once |
| Integration | payment-service | success, failure, redelivery; refund after success, no refund after failure; poison to DLT with original bytes; transient failure retried three times then processed exactly once |
| Integration | inventory-service | reservation, shortfall leaves other lines untouched, unknown sku, redelivery decrements once |
| Integration | notification-service | timeline lines via `OutputCaptureExtension`; dead letter WARN line |

Kafka assertions drain a topic for a fixed window and filter by key, so tests can assert exactly one event and also assert absence.

## Out of scope, on purpose

Authentication, a UI, real payment or inventory providers, schema registry, multi-broker deployment, Kubernetes manifests. Each would be a service concern layered on top; none changes the messaging design shown here. Landing next: Avro with Schema Registry, a Kafka Streams analytics module, Kubernetes manifests. Exactly-once with chained Kafka and database transactions was considered and dropped: the outbox already closes the dual-write gap for the saga owner, and shipping both would be two answers to one question.

## License

MIT
