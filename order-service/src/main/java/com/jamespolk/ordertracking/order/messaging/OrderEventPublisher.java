package com.jamespolk.ordertracking.order.messaging;

import com.jamespolk.ordertracking.events.OrderEvent;
import com.jamespolk.ordertracking.events.Topics;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class OrderEventPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public OrderEventPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /** Keyed by order id so every event for one order lands on the same partition, in order. */
    public void publish(OrderEvent event) {
        kafkaTemplate.send(Topics.ORDER_EVENTS, event.orderId().toString(), event);
    }
}
