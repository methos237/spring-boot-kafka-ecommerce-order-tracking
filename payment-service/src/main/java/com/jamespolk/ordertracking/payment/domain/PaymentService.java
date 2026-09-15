package com.jamespolk.ordertracking.payment.domain;

import com.jamespolk.ordertracking.events.OrderPlaced;
import com.jamespolk.ordertracking.events.PaymentFailed;
import com.jamespolk.ordertracking.events.PaymentSucceeded;
import com.jamespolk.ordertracking.payment.messaging.PaymentEventPublisher;
import java.time.Instant;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final PaymentRepository payments;
    private final ProcessedEventRepository processedEvents;
    private final PaymentEventPublisher publisher;

    public PaymentService(
            PaymentRepository payments, ProcessedEventRepository processedEvents, PaymentEventPublisher publisher) {
        this.payments = payments;
        this.processedEvents = processedEvents;
        this.publisher = publisher;
    }

    @Transactional
    public void handle(OrderPlaced event) {
        if (processedEvents.existsById(event.eventId())) {
            log.info("[order={}] duplicate OrderPlaced {} ignored", event.orderId(), event.eventId());
            return;
        }
        processedEvents.save(new ProcessedEvent(event.eventId()));

        Payment payment = payments.save(Payment.charge(event.orderId(), event.totalAmount()));
        log.info("[order={}] payment {} {}", event.orderId(), payment.getStatus(), event.totalAmount());

        if (payment.succeeded()) {
            publisher.publish(
                    new PaymentSucceeded(UUID.randomUUID(), event.orderId(), Instant.now(), payment.getAmount()));
        } else {
            publisher.publish(
                    new PaymentFailed(UUID.randomUUID(), event.orderId(), Instant.now(), payment.getReason()));
        }
    }
}
