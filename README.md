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
docker compose up -d        # Kafka on 9092, Kafka UI on http://localhost:8090, Postgres on 5432
mvn -B verify               # build and run all tests (Testcontainers starts its own Kafka and Postgres)
```

Peek at a topic:

```bash
scripts/peek.sh order-events 5
```

## Status

Work in progress. Modules land one at a time; see the issues for the plan.
