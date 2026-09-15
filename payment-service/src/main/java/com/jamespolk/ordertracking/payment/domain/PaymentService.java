package com.jamespolk.ordertracking.payment.domain;

import com.jamespolk.ordertracking.events.OrderCancelled;
import com.jamespolk.ordertracking.events.OrderPlaced;
import com.jamespolk.ordertracking.events.PaymentFailed;
import com.jamespolk.ordertracking.events.PaymentRefunded;
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
            log.info("duplicate OrderPlaced {} ignored", event.eventId());
            return;
        }
        processedEvents.save(new ProcessedEvent(event.eventId()));

        Payment payment = payments.save(Payment.charge(event.orderId(), event.totalAmount()));
        log.info("payment {} {}", payment.getStatus(), event.totalAmount());

        if (payment.succeeded()) {
            publisher.publish(
                    new PaymentSucceeded(UUID.randomUUID(), event.orderId(), Instant.now(), payment.getAmount()));
        } else {
            publisher.publish(
                    new PaymentFailed(UUID.randomUUID(), event.orderId(), Instant.now(), payment.getReason()));
        }
    }

    @Transactional
    public void handle(OrderCancelled event) {
        if (processedEvents.existsById(event.eventId())) {
            log.info("duplicate OrderCancelled {} ignored", event.eventId());
            return;
        }
        processedEvents.save(new ProcessedEvent(event.eventId()));

        payments.findByOrderId(event.orderId())
                .filter(Payment::refund)
                .ifPresentOrElse(
                        payment -> {
                            log.info("refunded {}", payment.getAmount());
                            publisher.publish(new PaymentRefunded(
                                    UUID.randomUUID(), event.orderId(), Instant.now(), payment.getAmount()));
                        },
                        () -> log.info("cancelled, nothing to refund"));
    }
}
