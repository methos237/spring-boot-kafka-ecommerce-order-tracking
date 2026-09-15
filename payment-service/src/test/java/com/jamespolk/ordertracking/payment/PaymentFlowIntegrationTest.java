package com.jamespolk.ordertracking.payment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.jamespolk.ordertracking.events.Events;
import com.jamespolk.ordertracking.events.OrderCancelled;
import com.jamespolk.ordertracking.events.OrderItem;
import com.jamespolk.ordertracking.events.OrderPlaced;
import com.jamespolk.ordertracking.events.PaymentRefunded;
import com.jamespolk.ordertracking.events.PaymentSucceeded;
import com.jamespolk.ordertracking.events.Topics;
import com.jamespolk.ordertracking.payment.domain.Payment;
import com.jamespolk.ordertracking.payment.domain.PaymentRepository;
import com.jamespolk.ordertracking.payment.domain.PaymentStatus;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.apache.avro.specific.SpecificRecord;
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

        Payment payment = awaitPayment(placed.getOrderId());
        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.SUCCEEDED);
        assertThat(payment.getAmount()).isEqualByComparingTo("500.00");

        List<ConsumerRecord<String, Object>> outcomes = outcomesFor(placed.getOrderId());
        assertThat(outcomes).hasSize(1);
        assertThat(outcomes.getFirst().value()).isInstanceOf(PaymentSucceeded.class);
        assertThat(((PaymentSucceeded) outcomes.getFirst().value()).getAmount()).isEqualByComparingTo("500.00");
    }

    @Test
    void failsOrderOverLimitAndPublishesPaymentFailed() {
        OrderPlaced placed = orderPlaced(new BigDecimal("1500.00"));

        publish(placed);

        Payment payment = awaitPayment(placed.getOrderId());
        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.FAILED);
        assertThat(payment.getReason()).contains("exceeds limit");

        List<ConsumerRecord<String, Object>> outcomes = outcomesFor(placed.getOrderId());
        assertThat(outcomes).hasSize(1);
        assertThat(KafkaTestSupport.type(outcomes.getFirst())).isEqualTo("PaymentFailed");
    }

    @Test
    void ignoresRedeliveredEvent() {
        OrderPlaced placed = orderPlaced(new BigDecimal("10.00"));

        publish(placed);
        publish(placed);

        awaitPayment(placed.getOrderId());
        List<ConsumerRecord<String, Object>> outcomes = outcomesFor(placed.getOrderId());
        assertThat(outcomes).hasSize(1);
        assertThat(payments.findAll().stream().filter(p -> p.getOrderId().equals(placed.getOrderId())))
                .hasSize(1);
    }

    @Test
    void refundsSuccessfulPaymentWhenOrderIsCancelled() {
        OrderPlaced placed = orderPlaced(new BigDecimal("200.00"));
        publish(placed);
        awaitPayment(placed.getOrderId());

        OrderCancelled cancelled =
                new OrderCancelled(UUID.randomUUID(), placed.getOrderId(), Instant.now(), "inventory");
        publish(cancelled);
        publish(cancelled);

        await().atMost(Duration.ofSeconds(20))
                .until(() -> payments.findByOrderId(placed.getOrderId())
                                .orElseThrow()
                                .getStatus()
                        == PaymentStatus.REFUNDED);
        List<ConsumerRecord<String, Object>> outcomes = outcomesFor(placed.getOrderId());
        assertThat(outcomes).hasSize(2);
        assertThat(outcomes.getLast().value()).isInstanceOf(PaymentRefunded.class);
        assertThat(((PaymentRefunded) outcomes.getLast().value()).getAmount()).isEqualByComparingTo("200.00");
    }

    @Test
    void doesNotRefundFailedPaymentWhenOrderIsCancelled() {
        OrderPlaced placed = orderPlaced(new BigDecimal("5000.00"));
        publish(placed);
        awaitPayment(placed.getOrderId());

        publish(new OrderCancelled(UUID.randomUUID(), placed.getOrderId(), Instant.now(), "payment"));

        List<ConsumerRecord<String, Object>> outcomes = outcomesFor(placed.getOrderId());
        assertThat(outcomes).hasSize(1);
        assertThat(KafkaTestSupport.type(outcomes.getFirst())).isEqualTo("PaymentFailed");
        assertThat(payments.findByOrderId(placed.getOrderId()).orElseThrow().getStatus())
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

    private void publish(SpecificRecord event) {
        kafkaTemplate
                .send(Topics.ORDER_EVENTS, Events.orderId(event).toString(), event)
                .join();
    }

    private Payment awaitPayment(UUID orderId) {
        return await().atMost(Duration.ofSeconds(20))
                .until(() -> payments.findByOrderId(orderId), java.util.Optional::isPresent)
                .orElseThrow();
    }

    private List<ConsumerRecord<String, Object>> outcomesFor(UUID orderId) {
        return KafkaTestSupport.drain(kafka.getBootstrapServers(), Topics.PAYMENT_EVENTS, Duration.ofSeconds(5))
                .stream()
                .filter(r -> r.key().equals(orderId.toString()))
                .toList();
    }
}
