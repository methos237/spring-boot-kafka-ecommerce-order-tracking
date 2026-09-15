package com.jamespolk.ordertracking.events;

import java.time.Instant;
import java.util.UUID;

/** Common envelope for every event. Kafka message key is {@code orderId.toString()}. */
public interface DomainEvent {

    UUID eventId();

    UUID orderId();

    Instant occurredAt();
}
