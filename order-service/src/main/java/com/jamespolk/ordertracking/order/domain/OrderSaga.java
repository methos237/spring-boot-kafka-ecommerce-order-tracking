package com.jamespolk.ordertracking.order.domain;

import com.jamespolk.ordertracking.events.DomainEvent;
import com.jamespolk.ordertracking.events.OrderCancelled;
import com.jamespolk.ordertracking.events.OrderConfirmed;
import com.jamespolk.ordertracking.order.messaging.OrderEventPublisher;
import java.time.Instant;
import java.util.UUID;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Choreographed saga: payment and inventory each publish their outcome; this applies them to the
 * order and announces the final state. Arrival order does not matter.
 */
@Service
public class OrderSaga {

    private static final Logger log = LoggerFactory.getLogger(OrderSaga.class);

    private final OrderRepository orders;
    private final ProcessedEventRepository processedEvents;
    private final OrderEventPublisher publisher;

    public OrderSaga(OrderRepository orders, ProcessedEventRepository processedEvents, OrderEventPublisher publisher) {
        this.orders = orders;
        this.processedEvents = processedEvents;
        this.publisher = publisher;
    }

    @Transactional
    public void paymentSucceeded(DomainEvent event) {
        apply(event, order -> order.recordPaymentResult(StepStatus.SUCCEEDED, null));
    }

    @Transactional
    public void paymentFailed(DomainEvent event, String reason) {
        apply(event, order -> order.recordPaymentResult(StepStatus.FAILED, "payment: " + reason));
    }

    @Transactional
    public void inventoryReserved(DomainEvent event) {
        apply(event, order -> order.recordInventoryResult(StepStatus.SUCCEEDED, null));
    }

    @Transactional
    public void inventoryFailed(DomainEvent event, String reason) {
        apply(event, order -> order.recordInventoryResult(StepStatus.FAILED, "inventory: " + reason));
    }

    private void apply(DomainEvent event, Function<Order, Boolean> transition) {
        if (processedEvents.existsById(event.eventId())) {
            log.info("duplicate {} ignored", event.getClass().getSimpleName());
            return;
        }
        processedEvents.save(new ProcessedEvent(event.eventId()));

        Order order = orders.findById(event.orderId()).orElse(null);
        if (order == null) {
            log.warn("{} for unknown order dropped", event.getClass().getSimpleName());
            return;
        }
        boolean settled = transition.apply(order);
        log.info(
                "{} applied, status={} payment={} inventory={}",
                event.getClass().getSimpleName(),
                order.getStatus(),
                order.getPaymentStatus(),
                order.getInventoryStatus());
        if (!settled) {
            return;
        }
        if (order.getStatus() == OrderStatus.CONFIRMED) {
            publisher.publish(new OrderConfirmed(UUID.randomUUID(), order.getId(), Instant.now()));
        } else {
            publisher.publish(
                    new OrderCancelled(UUID.randomUUID(), order.getId(), Instant.now(), order.getCancelReason()));
        }
    }
}
