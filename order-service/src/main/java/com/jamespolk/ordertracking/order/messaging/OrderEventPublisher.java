package com.jamespolk.ordertracking.order.messaging;

import com.jamespolk.ordertracking.events.Events;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Transactional outbox. The event is stored next to the state change it describes and commits or
 * rolls back with it; {@link OutboxRelay} moves it to Kafka afterwards. Callers must already be in a
 * transaction, otherwise the outbox would not be atomic with anything.
 */
@Component
public class OrderEventPublisher {

    private final OutboxRepository outbox;

    public OrderEventPublisher(OutboxRepository outbox) {
        this.outbox = outbox;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void publish(SpecificRecord event) {
        outbox.save(new OutboxMessage(Events.orderId(event), event.getClass().getName(), Events.toBytes(event)));
    }
}
