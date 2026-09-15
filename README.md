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

## Status

Work in progress. Done: infrastructure, `common-events`, `order-service`. Next: `payment-service` and `inventory-service` consumers.
