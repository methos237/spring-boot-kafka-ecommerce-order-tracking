package com.jamespolk.ordertracking.inventory.messaging;

import com.jamespolk.ordertracking.events.Events;
import com.jamespolk.ordertracking.events.Topics;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class InventoryEventPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public InventoryEventPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(SpecificRecord event) {
        kafkaTemplate.send(Topics.INVENTORY_EVENTS, Events.orderId(event).toString(), event);
    }
}
