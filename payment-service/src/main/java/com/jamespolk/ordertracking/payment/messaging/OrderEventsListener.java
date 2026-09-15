package com.jamespolk.ordertracking.payment.messaging;

import com.jamespolk.ordertracking.events.OrderCancelled;
import com.jamespolk.ordertracking.events.OrderPlaced;
import com.jamespolk.ordertracking.events.Topics;
import com.jamespolk.ordertracking.payment.domain.PaymentService;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Routes each record on order-events to a handler by payload type. */
@Component
@KafkaListener(topics = Topics.ORDER_EVENTS)
class OrderEventsListener {

    private final PaymentService paymentService;

    OrderEventsListener(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    @KafkaHandler
    void on(OrderPlaced event) {
        paymentService.handle(event);
    }

    @KafkaHandler
    void on(OrderCancelled event) {
        paymentService.handle(event);
    }

    @KafkaHandler(isDefault = true)
    void ignore(Object other) {
        // OrderConfirmed: nothing to compensate.
    }
}
