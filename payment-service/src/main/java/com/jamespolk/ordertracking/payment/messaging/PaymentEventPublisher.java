package com.jamespolk.ordertracking.payment.messaging;

import com.jamespolk.ordertracking.events.PaymentEvent;
import com.jamespolk.ordertracking.events.Topics;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class PaymentEventPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public PaymentEventPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(PaymentEvent event) {
        kafkaTemplate.send(Topics.PAYMENT_EVENTS, event.orderId().toString(), event);
    }
}
