package com.jamespolk.ordertracking.events;

import java.time.Instant;
import java.util.UUID;

public record InventoryReserved(UUID eventId, UUID orderId, Instant occurredAt) implements InventoryEvent {}
