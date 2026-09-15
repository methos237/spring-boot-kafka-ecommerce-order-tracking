package com.jamespolk.ordertracking.order.messaging;

import com.jamespolk.ordertracking.events.OrderEvent;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

/**
 * Transactional outbox. The event is stored next to the state change it describes and commits or
 * rolls back with it; {@link OutboxRelay} moves it to Kafka afterwards. Callers must already be in a
 * transaction, otherwise the outbox would not be atomic with anything.
 */
@Component
public class OrderEventPublisher {

    private final OutboxRepository outbox;
    private final JsonMapper jsonMapper;

    public OrderEventPublisher(OutboxRepository outbox, JsonMapper jsonMapper) {
        this.outbox = outbox;
        this.jsonMapper = jsonMapper;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void publish(OrderEvent event) {
        outbox.save(
                new OutboxMessage(event.orderId(), event.getClass().getName(), jsonMapper.writeValueAsBytes(event)));
    }
}
