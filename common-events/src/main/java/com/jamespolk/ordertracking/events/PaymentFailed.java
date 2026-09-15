package com.jamespolk.ordertracking.events;

import java.time.Instant;
import java.util.UUID;

public record PaymentFailed(UUID eventId, UUID orderId, Instant occurredAt, String reason)
        implements PaymentEvent {}
