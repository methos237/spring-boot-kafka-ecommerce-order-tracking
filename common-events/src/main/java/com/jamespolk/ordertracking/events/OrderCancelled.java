package com.jamespolk.ordertracking.events;

import java.time.Instant;
import java.util.UUID;

public record OrderCancelled(UUID eventId, UUID orderId, Instant occurredAt, String reason)
        implements OrderEvent {}
