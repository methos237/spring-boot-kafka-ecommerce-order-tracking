package com.jamespolk.ordertracking.inventory.messaging;

import com.jamespolk.ordertracking.events.InventoryEvent;
import com.jamespolk.ordertracking.events.Topics;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class InventoryEventPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public InventoryEventPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(InventoryEvent event) {
        kafkaTemplate.send(Topics.INVENTORY_EVENTS, event.orderId().toString(), event);
    }
}
