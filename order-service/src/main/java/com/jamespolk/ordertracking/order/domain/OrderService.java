package com.jamespolk.ordertracking.order.domain;

import com.jamespolk.ordertracking.events.OrderItem;
import com.jamespolk.ordertracking.events.OrderPlaced;
import com.jamespolk.ordertracking.order.messaging.OrderEventPublisher;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Service;

@Service
public class OrderService {

    private final OrderRepository orders;
    private final OrderEventPublisher publisher;

    public OrderService(OrderRepository orders, OrderEventPublisher publisher) {
        this.orders = orders;
        this.publisher = publisher;
    }

    /**
     * Saves the order in its own transaction, then publishes. A publish failure leaves a saved
     * order with no event; the transactional outbox pattern that closes that gap is out of scope
     * for now.
     */
    public Order placeOrder(String customerId, List<OrderLine> items) {
        Order order = orders.save(Order.place(customerId, items));
        publisher.publish(new OrderPlaced(
                UUID.randomUUID(),
                order.getId(),
                Instant.now(),
                order.getCustomerId(),
                order.getItems().stream()
                        .map(line -> new OrderItem(line.sku(), line.quantity(), line.unitPrice()))
                        .toList(),
                order.getTotalAmount()));
        return order;
    }

    public Order getOrder(UUID id) {
        return orders.findById(id).orElseThrow(() -> new OrderNotFoundException(id));
    }
}
