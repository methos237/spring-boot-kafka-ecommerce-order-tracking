package com.jamespolk.ordertracking.order.api;

import com.jamespolk.ordertracking.order.domain.Order;
import com.jamespolk.ordertracking.order.domain.OrderLine;
import com.jamespolk.ordertracking.order.domain.OrderStatus;
import com.jamespolk.ordertracking.order.domain.StepStatus;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record OrderResponse(
        UUID id,
        String customerId,
        List<OrderLine> items,
        BigDecimal totalAmount,
        OrderStatus status,
        StepStatus paymentStatus,
        StepStatus inventoryStatus,
        String cancelReason,
        Instant createdAt,
        Instant updatedAt) {

    public static OrderResponse from(Order order) {
        return new OrderResponse(
                order.getId(),
                order.getCustomerId(),
                order.getItems(),
                order.getTotalAmount(),
                order.getStatus(),
                order.getPaymentStatus(),
                order.getInventoryStatus(),
                order.getCancelReason(),
                order.getCreatedAt(),
                order.getUpdatedAt());
    }
}
