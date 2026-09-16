# Order Tracking

[![CI](https://github.com/methos237/spring-boot-kafka-ecommerce-order-tracking/actions/workflows/ci.yml/badge.svg)](https://github.com/methos237/spring-boot-kafka-ecommerce-order-tracking/actions/workflows/ci.yml)

Event-driven e-commerce order tracking built with Spring Boot 4 and Apache Kafka.

A customer places an order. Payment and inventory are handled by independent services reacting to Kafka events. The order service listens for their outcomes and drives the order to `CONFIRMED` or `CANCELLED`, refunding payment when inventory falls through. A notification service tails every topic and prints the order timeline. Failures retry, then dead-letter. Every consumer is idempotent. Everything is covered by tests that run against real Kafka and Postgres in Docker.

## Ten-minute tour

```bash
docker compose up -d --wait                 # Kafka 9092, Schema Registry 8085, Kafka UI http://localhost:8090, Postgres 5433
mvn -B verify                               # build + 34 tests (Testcontainers starts its own Kafka, Postgres, Schema Registry)
for s in order-service payment-service inventory-service notification-service order-analytics; do
  java -jar $s/target/$s-0.1.0-SNAPSHOT.jar > /tmp/$s.log 2>&1 &
done
scripts/load.sh                             # 50 orders: 30 confirm, 20 cancel, verified via GET
scripts/status.sh                           # consumer lag per group and partition, should read 0
curl localhost:8084/analytics/orders-per-minute?last=5      # Kafka Streams window counts
curl localhost:8084/analytics/customers/customer-0/revenue   # confirmed revenue for one customer
scripts/poison.sh order-events              # one bad record ...
scripts/peek.sh order-events.DLT 3          # ... lands here with headers, one per consuming group
grep 'DEAD LETTER' /tmp/notification-service.log
```

Open Kafka UI to watch keys spread across the three partitions of `order-events`, browse the registered Avro schemas, and see the DLTs fill.

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
 order-analytics       (Kafka Streams over order-events: orders per minute, revenue per customer)
```

| Module | Role | Port | Database |
|---|---|---|---|
| `common-events` | Avro schemas and generated event classes, topic names, Kafka serializer and interceptors | – | – |
| `order-service` | REST API, order persistence, saga state | 8080 | `orders` |
| `payment-service` | Charges and refunds | 8081 | `payments` |
| `inventory-service` | Stock reservation | 8082 | `inventory` |
| `notification-service` | Order timeline and dead letter log | 8083 | – |
| `order-analytics` | Kafka Streams aggregations with interactive queries | 8084 | state stores |

### Event flow

| Topic | Event | Producer | Consumers |
|---|---|---|---|
| `order-events` | `OrderPlaced{customerId, items, totalAmount}` | order | payment, inventory, notification, analytics |
| `payment-events` | `PaymentSucceeded{amount}` / `PaymentFailed{reason}` | payment | order, notification |
| `inventory-events` | `InventoryReserved` / `InventoryFailed{reason}` | inventory | order, notification |
| `order-events` | `OrderConfirmed` / `OrderCancelled{reason}` | order | payment (refund), notification, analytics |
| `payment-events` | `PaymentRefunded{amount}` | payment | notification |
| `*.DLT` | original bytes + `kafka_dlt-*` headers | any consumer's error handler | notification |

Every record is keyed by `orderId`, so all events for one order land on one partition and stay ordered. Every event carries `eventId`, `orderId` and `occurredAt`.

## Stack

Java 25 · Spring Boot 4.1 · Spring for Apache Kafka 4.1 · Apache Kafka 4 (KRaft) · Kafka Streams · Avro 1.12 + Confluent Schema Registry · PostgreSQL 16 · Flyway · Micrometer + Prometheus · Testcontainers 2 · Maven · GitHub Actions

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

## Stream processing: order-analytics

A Kafka Streams application over `order-events`, no database. Two aggregations, both kept in local state stores and served straight from them:

| Store | Built from | Query |
|---|---|---|
| `orders-per-minute` | `OrderPlaced`, tumbling one-minute windows | `GET /analytics/orders-per-minute?last=10` |
| `revenue-by-customer` | `OrderConfirmed` joined to its `OrderPlaced`, summed per customer | `GET /analytics/customers/{id}/revenue` |

**Event time, not arrival time.** A `TimestampExtractor` reads `occurredAt` from every event, so windows describe when orders happened. Replaying last week's topic fills last week's windows instead of piling everything into "now".

**Why a KStream-KTable join for revenue.** `OrderConfirmed` carries no amount; the amount lives on `OrderPlaced`. Both events share the order id as key, so they sit on the same partition and the same Streams task processes them in offset order. `OrderPlaced` is materialised into a table and each `OrderConfirmed` looks its order up there. Cancelled orders never confirm, so refunds need no special handling: they were never counted. The join result is re-keyed by customer (one repartition topic) and summed. Amounts are stored as long cents, exact and with a built-in serde.

**Interactive queries.** The REST layer reads the state stores through `KafkaStreams.store(...)`. Until the instance is `RUNNING` (startup, rebalance, restore) it answers 503 with a problem detail rather than a wrong number. Single instance here; with several, a query would have to be routed to the instance that owns the key, which is what `KafkaStreams.queryMetadataForKey` exists for.

**Durability.** State stores are RocksDB on local disk, backed by changelog topics the application creates (`order-analytics-*-changelog`). Wipe the disk and the stores rebuild from the changelogs on restart.

**Poison records.** `LogAndContinueExceptionHandler` is configured, so a payload that fails Avro deserialization is logged and skipped. The default handler would kill the stream thread. The same `scripts/poison.sh order-events` that fills the DLTs elsewhere costs this app one skipped record.

**Idempotency, honestly.** The consumers in this system drop redelivered events by `eventId`. The analytics topology does not: a redelivered `OrderConfirmed` is a second join hit and double-counts that order. Deduplicating would take a keyed store of seen event ids with a retention window, or Kafka Streams exactly-once processing plus an idempotent upstream. Both are known patterns; neither is here yet, and the topology test pins the current behaviour so the gap is visible rather than hidden.

Tests: `AnalyticsTopologyTest` drives the topology with `TopologyTestDriver`, no broker, no containers, milliseconds per run: window counts across a minute boundary, revenue only for confirmed orders, unknown customer empty. `AnalyticsApiSmokeTest` runs the real application against Testcontainers Kafka with the mock registry and asserts the two endpoints over HTTP.

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

## Serialization: Avro and the Schema Registry

Events are Avro. Each event has an `.avsc` under `common-events/src/main/avro`; `avro-maven-plugin` generates the Java classes at build time, so producers and consumers share one compiled contract and a field rename is a compile error, not a runtime surprise. Logical types carry the domain types straight through: `uuid` for ids, `timestamp-millis` for `occurredAt`, `decimal(12,2)` for money.

On the wire, `KafkaAvroSerializer` writes the Confluent framing (magic byte, schema id, Avro binary) and registers the schema with the registry on first use. Subjects follow `TopicRecordNameStrategy` (`order-events-com.jamespolk.ordertracking.events.OrderPlaced`), which is what lets one topic carry several event types, each with its own schema and its own compatibility history. Consumers read through `KafkaAvroDeserializer` with `specific.avro.reader=true` and get the generated class back, so `@KafkaHandler` routing by type still works.

**Evolution.** Compatibility mode is `BACKWARD` (the registry default): a new schema must be able to read every record written with the old one. Adding a field with a default passes; adding a required field is rejected. Two tests pin that down: `SchemaEvolutionTest` writes a record with a v2 schema (new optional `channel` field) and reads it with the v1 generated class; `SchemaRegistryCompatibilityTest` runs a real registry container, registers v1, and asserts the registry accepts v2 and rejects a v3 with a required field.

**Everything else keeps working.** `EventSerializer` still passes `byte[]` through, so poison records land on the DLT as the exact bytes that broke. The outbox stores the Avro binary of the event and the relay decodes it back to the class before sending, so the registry is never on the request path: an order can be accepted while the registry is down. Tests use Confluent's `mock://` registry, an in-JVM implementation, so no registry container is needed for the service test suites.

**What JSON gave up.** Records were human-readable in `kafka-console-consumer`; `scripts/peek.sh` now goes through `kafka-avro-console-consumer` in the registry container. A type header (`__TypeId__`) told the consumer which class to build; the schema id does that now. And the hand-written Java records with sealed interfaces became generated classes: getters instead of accessors, no `sealed`, `Events.orderId(record)` where code needs the envelope without knowing the type.

**Avro 1.12 gotcha.** The library refuses to instantiate classes it has not been told to trust when resolving a schema to a Java class (a deserialization-gadget defence). `Events.trustEventClasses()` registers the events package; the serializer and the consumer interceptor both call it, so every JVM in the system is covered before its first record.

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
decode each row back to its event class, send with key = orderId through the Avro serializer
wait for the broker ack, then DELETE the row
commit
```

What this buys:

- An order and its `OrderPlaced` commit together or not at all. Kafka down during `POST /api/orders` still returns 201; the row waits and drains when the broker returns.
- The relay re-sends the stored event, so a duplicate send after a crash carries the same `eventId` and every consumer drops it.
- `SKIP LOCKED` lets a second relay instance run without double-sending.

What it costs: up to 500 ms added latency, one extra write per event, a poller. Change data capture (Debezium tailing the `outbox` table) removes the poller and is the usual next step in production.

Test: `OutboxRelayTest` pauses the Kafka container, places an order, asserts the row is saved and stays in the outbox while relay ticks fail, unpauses, and asserts the event arrives intact and the row is gone.

## Resilience: retries and dead letters

Every consumer runs the same `DefaultErrorHandler`:

| Failure | Attempts | Then |
|---|---|---|
| Transient exception (database, broker, anything unclassified) | 1 + 3 retries, 2 s apart | record published to `<topic>.DLT` |
| Deserialization failure (poison payload) | 1 | straight to `<topic>.DLT` |
| `IllegalArgumentException` | 1 | straight to `<topic>.DLT` |

Poison payloads never reach a handler: values are read through `ErrorHandlingDeserializer`, which turns a broken payload into a `DeserializationException` the error handler classifies as non-retryable. A failed attempt rolls back the whole transaction, including the `processed_events` insert, so a retry starts clean.

Dead-letter records keep the original key and bytes plus headers `kafka_dlt-original-topic`, `kafka_dlt-original-offset`, `kafka_dlt-original-consumer-group` and `kafka_dlt-exception-message`. Producers use `EventSerializer`, which passes `byte[]` through untouched and Avro-encodes everything else, so a poison payload lands on the DLT exactly as it arrived rather than re-encoded. Expect one DLT record per consumer group that reads the topic. Nothing is retried from the DLT automatically; `notification-service` logs each one at `WARN` for an operator.

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
- **Avro with a registry, not JSON.** JSON was fine for a demo and is easier to eyeball. Avro makes the contract explicit, compact, and enforced by something outside the codebase: the registry rejects an incompatible change before any consumer sees it. That trade is documented in the serialization section above.
- **Kafka Streams for read models, not another consumer with a database.** The two aggregations need no durability beyond what the changelog topics give and no query beyond key lookups and window scans. A state store answers that with less moving parts than a fifth Postgres database. If the questions grew into ad hoc SQL, the right move is a sink connector into an analytics database, not a bigger topology.
- **Testcontainers, not embedded Kafka or H2.** Tests run against the same Kafka, Postgres and Schema Registry the services run against. CI does the same on GitHub Actions.

## Tests

37 tests across six modules, all in `mvn verify`.

| Level | Where | What |
|---|---|---|
| Unit | `common-events` | every event round-trips through Avro binary; a v2 record reads with the v1 class |
| Integration | `common-events` | real Schema Registry container accepts a backward-compatible schema and rejects a breaking one |
| Web slice (`@WebMvcTest`) | order-service | 201 with `Location`, every invalid field listed, 404 mapping |
| Integration (`@SpringBootTest` + Testcontainers) | order-service | place over HTTP, read back, consume `OrderPlaced`; saga in both arrival orders, late outcome ignored, redelivery applied once |
| Integration | payment-service | success, failure, redelivery; refund after success, no refund after failure; poison to DLT with original bytes; transient failure retried three times then processed exactly once |
| Integration | inventory-service | reservation, shortfall leaves other lines untouched, unknown sku, redelivery decrements once |
| Integration | notification-service | timeline lines via `OutputCaptureExtension`; dead letter WARN line |
| Topology (`TopologyTestDriver`) | order-analytics | window counts by event time, revenue only for confirmed orders |
| Integration | order-analytics | real Streams app on Testcontainers Kafka answers both endpoints |

Kafka assertions drain a topic for a fixed window and filter by key, so tests can assert exactly one event and also assert absence.

## Out of scope, on purpose

Authentication, a UI, real payment or inventory providers, multi-broker deployment. Each would be a service concern layered on top; none changes the messaging design shown here. Landing next: Kubernetes manifests. Exactly-once with chained Kafka and database transactions was considered and dropped: the outbox already closes the dual-write gap for the saga owner, and shipping both would be two answers to one question.

## License

MIT
