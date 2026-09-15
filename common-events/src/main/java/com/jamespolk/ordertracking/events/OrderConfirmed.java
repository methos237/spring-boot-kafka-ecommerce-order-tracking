package com.jamespolk.ordertracking.events;

import java.time.Instant;
import java.util.UUID;

public record OrderConfirmed(UUID eventId, UUID orderId, Instant occurredAt) implements OrderEvent {}
