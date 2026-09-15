package com.jamespolk.ordertracking.payment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.jamespolk.ordertracking.events.DomainEvent;
import com.jamespolk.ordertracking.events.OrderCancelled;
import com.jamespolk.ordertracking.events.OrderItem;
import com.jamespolk.ordertracking.events.OrderPlaced;
import com.jamespolk.ordertracking.events.Topics;
import com.jamespolk.ordertracking.payment.domain.Payment;
import com.jamespolk.ordertracking.payment.domain.PaymentRepository;
import com.jamespolk.ordertracking.payment.domain.PaymentStatus;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.kafka.KafkaContainer;

@SpringBootTest
@Import(TestcontainersConfiguration.class)
@ActiveProfiles("test")
class PaymentFlowIntegrationTest {

    @Autowired
    KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    PaymentRepository payments;

    @Autowired
    KafkaContainer kafka;

    @Test
    void chargesOrderWithinLimitAndPublishesPaymentSucceeded() {
        OrderPlaced placed = orderPlaced(new BigDecimal("500.00"));

        publish(placed);

        Payment payment = awaitPayment(placed.orderId());
        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.SUCCEEDED);
        assertThat(payment.getAmount()).isEqualByComparingTo("500.00");

        List<ConsumerRecord<String, String>> outcomes = outcomesFor(placed.orderId());
        assertThat(outcomes).hasSize(1);
        assertThat(outcomes.getFirst().value()).contains("\"amount\":500.00");
        assertThat(typeHeader(outcomes.getFirst())).endsWith("PaymentSucceeded");
    }

    @Test
    void failsOrderOverLimitAndPublishesPaymentFailed() {
        OrderPlaced placed = orderPlaced(new BigDecimal("1500.00"));

        publish(placed);

        Payment payment = awaitPayment(placed.orderId());
        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.FAILED);
        assertThat(payment.getReason()).contains("exceeds limit");

        List<ConsumerRecord<String, String>> outcomes = outcomesFor(placed.orderId());
        assertThat(outcomes).hasSize(1);
        assertThat(typeHeader(outcomes.getFirst())).endsWith("PaymentFailed");
    }

    @Test
    void ignoresRedeliveredEvent() {
        OrderPlaced placed = orderPlaced(new BigDecimal("10.00"));

        publish(placed);
        publish(placed);

        awaitPayment(placed.orderId());
        List<ConsumerRecord<String, String>> outcomes = outcomesFor(placed.orderId());
        assertThat(outcomes).hasSize(1);
        assertThat(payments.findAll().stream().filter(p -> p.getOrderId().equals(placed.orderId())))
                .hasSize(1);
    }

    @Test
    void refundsSuccessfulPaymentWhenOrderIsCancelled() {
        OrderPlaced placed = orderPlaced(new BigDecimal("200.00"));
        publish(placed);
        awaitPayment(placed.orderId());

        OrderCancelled cancelled = new OrderCancelled(UUID.randomUUID(), placed.orderId(), Instant.now(), "inventory");
        publish(cancelled);
        publish(cancelled);

        await().atMost(Duration.ofSeconds(20))
                .until(() ->
                        payments.findByOrderId(placed.orderId()).orElseThrow().getStatus() == PaymentStatus.REFUNDED);
        List<ConsumerRecord<String, String>> outcomes = outcomesFor(placed.orderId());
        assertThat(outcomes).hasSize(2);
        assertThat(typeHeader(outcomes.getLast())).endsWith("PaymentRefunded");
        assertThat(outcomes.getLast().value()).contains("\"amount\":200.00");
    }

    @Test
    void doesNotRefundFailedPaymentWhenOrderIsCancelled() {
        OrderPlaced placed = orderPlaced(new BigDecimal("5000.00"));
        publish(placed);
        awaitPayment(placed.orderId());

        publish(new OrderCancelled(UUID.randomUUID(), placed.orderId(), Instant.now(), "payment"));

        List<ConsumerRecord<String, String>> outcomes = outcomesFor(placed.orderId());
        assertThat(outcomes).hasSize(1);
        assertThat(typeHeader(outcomes.getFirst())).endsWith("PaymentFailed");
        assertThat(payments.findByOrderId(placed.orderId()).orElseThrow().getStatus())
                .isEqualTo(PaymentStatus.FAILED);
    }

    private static OrderPlaced orderPlaced(BigDecimal total) {
        return new OrderPlaced(
                UUID.randomUUID(),
                UUID.randomUUID(),
                Instant.now(),
                "customer-1",
                List.of(new OrderItem("SKU-1", 1, total)),
                total);
    }

    private void publish(DomainEvent event) {
        kafkaTemplate
                .send(Topics.ORDER_EVENTS, event.orderId().toString(), event)
                .join();
    }

    private Payment awaitPayment(UUID orderId) {
        return await().atMost(Duration.ofSeconds(20))
                .until(() -> payments.findByOrderId(orderId), java.util.Optional::isPresent)
                .orElseThrow();
    }

    private List<ConsumerRecord<String, String>> outcomesFor(UUID orderId) {
        return KafkaTestSupport.drain(kafka.getBootstrapServers(), Topics.PAYMENT_EVENTS, Duration.ofSeconds(5))
                .stream()
                .filter(r -> r.key().equals(orderId.toString()))
                .toList();
    }

    private static String typeHeader(ConsumerRecord<String, String> record) {
        return new String(record.headers().lastHeader("__TypeId__").value());
    }
}
