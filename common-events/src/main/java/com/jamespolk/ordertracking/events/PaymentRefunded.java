package com.jamespolk.ordertracking.events;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record PaymentRefunded(UUID eventId, UUID orderId, Instant occurredAt, BigDecimal amount)
        implements PaymentEvent {}
