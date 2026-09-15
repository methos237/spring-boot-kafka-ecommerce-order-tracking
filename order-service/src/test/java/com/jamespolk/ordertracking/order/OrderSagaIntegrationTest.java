package com.jamespolk.ordertracking.order;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.jamespolk.ordertracking.events.DomainEvent;
import com.jamespolk.ordertracking.events.InventoryFailed;
import com.jamespolk.ordertracking.events.InventoryReserved;
import com.jamespolk.ordertracking.events.PaymentSucceeded;
import com.jamespolk.ordertracking.events.Topics;
import com.jamespolk.ordertracking.order.domain.Order;
import com.jamespolk.ordertracking.order.domain.OrderLine;
import com.jamespolk.ordertracking.order.domain.OrderRepository;
import com.jamespolk.ordertracking.order.domain.OrderService;
import com.jamespolk.ordertracking.order.domain.OrderStatus;
import com.jamespolk.ordertracking.order.domain.ProcessedEventRepository;
import com.jamespolk.ordertracking.order.domain.StepStatus;
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
class OrderSagaIntegrationTest {

    @Autowired
    OrderService orderService;

    @Autowired
    OrderRepository orders;

    @Autowired
    ProcessedEventRepository processedEvents;

    @Autowired
    KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    KafkaContainer kafka;

    @Test
    void confirmsWhenPaymentThenInventorySucceed() {
        Order order = place();

        publish(Topics.PAYMENT_EVENTS, paymentSucceeded(order));
        publish(Topics.INVENTORY_EVENTS, inventoryReserved(order));

        Order settled = awaitStatus(order.getId(), OrderStatus.CONFIRMED);
        assertThat(settled.getPaymentStatus()).isEqualTo(StepStatus.SUCCEEDED);
        assertThat(settled.getInventoryStatus()).isEqualTo(StepStatus.SUCCEEDED);
        assertThat(emittedTypes(order.getId())).containsExactly("OrderPlaced", "OrderConfirmed");
    }

    @Test
    void confirmsWhenInventoryArrivesBeforePayment() {
        Order order = place();

        publish(Topics.INVENTORY_EVENTS, inventoryReserved(order));
        publish(Topics.PAYMENT_EVENTS, paymentSucceeded(order));

        awaitStatus(order.getId(), OrderStatus.CONFIRMED);
        assertThat(emittedTypes(order.getId())).containsExactly("OrderPlaced", "OrderConfirmed");
    }

    @Test
    void cancelsOnFirstFailureAndIgnoresLateSuccess() {
        Order order = place();

        publish(
                Topics.INVENTORY_EVENTS,
                new InventoryFailed(UUID.randomUUID(), order.getId(), Instant.now(), "insufficient stock for SKU-3"));
        Order cancelled = awaitStatus(order.getId(), OrderStatus.CANCELLED);
        assertThat(cancelled.getCancelReason()).isEqualTo("inventory: insufficient stock for SKU-3");

        PaymentSucceeded late = paymentSucceeded(order);
        publish(Topics.PAYMENT_EVENTS, late);
        awaitProcessed(late);

        Order after = orders.findById(order.getId()).orElseThrow();
        assertThat(after.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        assertThat(after.getPaymentStatus()).isEqualTo(StepStatus.PENDING);
        assertThat(emittedTypes(order.getId())).containsExactly("OrderPlaced", "OrderCancelled");
    }

    @Test
    void appliesRedeliveredOutcomeOnce() {
        Order order = place();
        PaymentSucceeded paid = paymentSucceeded(order);

        publish(Topics.PAYMENT_EVENTS, paid);
        publish(Topics.PAYMENT_EVENTS, paid);
        publish(Topics.INVENTORY_EVENTS, inventoryReserved(order));

        awaitStatus(order.getId(), OrderStatus.CONFIRMED);
        assertThat(emittedTypes(order.getId())).containsExactly("OrderPlaced", "OrderConfirmed");
    }

    private Order place() {
        return orderService.placeOrder("customer-1", List.of(new OrderLine("SKU-1", 1, new BigDecimal("10.00"))));
    }

    private static PaymentSucceeded paymentSucceeded(Order order) {
        return new PaymentSucceeded(UUID.randomUUID(), order.getId(), Instant.now(), order.getTotalAmount());
    }

    private static InventoryReserved inventoryReserved(Order order) {
        return new InventoryReserved(UUID.randomUUID(), order.getId(), Instant.now());
    }

    private void publish(String topic, DomainEvent event) {
        kafkaTemplate.send(topic, event.orderId().toString(), event).join();
    }

    private Order awaitStatus(UUID orderId, OrderStatus expected) {
        return await().atMost(Duration.ofSeconds(20))
                .until(() -> orders.findById(orderId).orElseThrow(), o -> o.getStatus() == expected);
    }

    private void awaitProcessed(DomainEvent event) {
        await().atMost(Duration.ofSeconds(20)).until(() -> processedEvents.existsById(event.eventId()));
    }

    private List<String> emittedTypes(UUID orderId) {
        return KafkaTestSupport.drain(kafka.getBootstrapServers(), Topics.ORDER_EVENTS, Duration.ofSeconds(5)).stream()
                .filter(r -> r.key().equals(orderId.toString()))
                .map(OrderSagaIntegrationTest::typeHeader)
                .map(type -> type.substring(type.lastIndexOf('.') + 1))
                .toList();
    }

    private static String typeHeader(ConsumerRecord<String, String> record) {
        return new String(record.headers().lastHeader("__TypeId__").value());
    }
}
