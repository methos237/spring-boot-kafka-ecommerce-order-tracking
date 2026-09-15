package com.jamespolk.ordertracking.events;

import java.time.Instant;
import java.util.UUID;

public record InventoryFailed(UUID eventId, UUID orderId, Instant occurredAt, String reason)
        implements InventoryEvent {}
