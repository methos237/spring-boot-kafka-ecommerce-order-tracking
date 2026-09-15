package com.jamespolk.ordertracking.inventory.messaging;

import com.jamespolk.ordertracking.events.OrderPlaced;
import com.jamespolk.ordertracking.events.Topics;
import com.jamespolk.ordertracking.inventory.domain.InventoryService;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Routes each record on order-events to a handler by payload type. */
@Component
@KafkaListener(topics = Topics.ORDER_EVENTS)
class OrderEventsListener {

    private final InventoryService inventoryService;

    OrderEventsListener(InventoryService inventoryService) {
        this.inventoryService = inventoryService;
    }

    @KafkaHandler
    void on(OrderPlaced event) {
        inventoryService.handle(event);
    }

    @KafkaHandler(isDefault = true)
    void ignore(Object other) {
        // OrderConfirmed / OrderCancelled carry nothing for inventory.
    }
}
