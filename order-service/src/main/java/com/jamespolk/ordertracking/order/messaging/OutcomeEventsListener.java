package com.jamespolk.ordertracking.order.messaging;

import com.jamespolk.ordertracking.events.InventoryFailed;
import com.jamespolk.ordertracking.events.InventoryReserved;
import com.jamespolk.ordertracking.events.PaymentFailed;
import com.jamespolk.ordertracking.events.PaymentSucceeded;
import com.jamespolk.ordertracking.events.Topics;
import com.jamespolk.ordertracking.order.domain.OrderSaga;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Feeds payment and inventory outcomes into the order saga. */
@Component
@KafkaListener(topics = {Topics.PAYMENT_EVENTS, Topics.INVENTORY_EVENTS})
class OutcomeEventsListener {

    private final OrderSaga saga;

    OutcomeEventsListener(OrderSaga saga) {
        this.saga = saga;
    }

    @KafkaHandler
    void on(PaymentSucceeded event) {
        saga.paymentSucceeded(event);
    }

    @KafkaHandler
    void on(PaymentFailed event) {
        saga.paymentFailed(event, event.getReason());
    }

    @KafkaHandler
    void on(InventoryReserved event) {
        saga.inventoryReserved(event);
    }

    @KafkaHandler
    void on(InventoryFailed event) {
        saga.inventoryFailed(event, event.getReason());
    }

    @KafkaHandler(isDefault = true)
    void ignore(Object other) {
        // PaymentRefunded is compensation bookkeeping; the order is already cancelled.
    }
}
