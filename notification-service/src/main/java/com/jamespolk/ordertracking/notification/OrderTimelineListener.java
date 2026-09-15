package com.jamespolk.ordertracking.notification;

import com.jamespolk.ordertracking.events.DomainEvent;
import com.jamespolk.ordertracking.events.InventoryFailed;
import com.jamespolk.ordertracking.events.InventoryReserved;
import com.jamespolk.ordertracking.events.OrderCancelled;
import com.jamespolk.ordertracking.events.OrderConfirmed;
import com.jamespolk.ordertracking.events.OrderPlaced;
import com.jamespolk.ordertracking.events.PaymentFailed;
import com.jamespolk.ordertracking.events.PaymentRefunded;
import com.jamespolk.ordertracking.events.PaymentSucceeded;
import com.jamespolk.ordertracking.events.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Stateless fan-out consumer. One consumer group over all three topics gives a single, ordered-per-order
 * view of the whole flow. A real system would hand each line to email, SMS or a push channel here.
 */
@Component
@KafkaListener(topics = {Topics.ORDER_EVENTS, Topics.PAYMENT_EVENTS, Topics.INVENTORY_EVENTS})
class OrderTimelineListener {

    private static final Logger log = LoggerFactory.getLogger(OrderTimelineListener.class);

    @KafkaHandler
    void on(OrderPlaced e) {
        line(e, "customer=" + e.customerId() + " items=" + e.items().size() + " total=" + e.totalAmount());
    }

    @KafkaHandler
    void on(PaymentSucceeded e) {
        line(e, "amount=" + e.amount());
    }

    @KafkaHandler
    void on(PaymentFailed e) {
        line(e, "reason=" + e.reason());
    }

    @KafkaHandler
    void on(PaymentRefunded e) {
        line(e, "amount=" + e.amount());
    }

    @KafkaHandler
    void on(InventoryReserved e) {
        line(e, "");
    }

    @KafkaHandler
    void on(InventoryFailed e) {
        line(e, "reason=" + e.reason());
    }

    @KafkaHandler
    void on(OrderConfirmed e) {
        line(e, "");
    }

    @KafkaHandler
    void on(OrderCancelled e) {
        line(e, "reason=" + e.reason());
    }

    private static void line(DomainEvent event, String detail) {
        log.info("{}{}", event.getClass().getSimpleName(), detail.isEmpty() ? "" : " " + detail);
    }
}
