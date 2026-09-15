package com.jamespolk.ordertracking.events;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record OrderPlaced(
        UUID eventId,
        UUID orderId,
        Instant occurredAt,
        String customerId,
        List<OrderItem> items,
        BigDecimal totalAmount)
        implements OrderEvent {}
