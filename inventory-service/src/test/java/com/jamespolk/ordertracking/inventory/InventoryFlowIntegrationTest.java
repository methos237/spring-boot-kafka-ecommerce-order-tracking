package com.jamespolk.ordertracking.inventory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.jamespolk.ordertracking.events.OrderItem;
import com.jamespolk.ordertracking.events.OrderPlaced;
import com.jamespolk.ordertracking.events.Topics;
import com.jamespolk.ordertracking.inventory.domain.ProcessedEventRepository;
import com.jamespolk.ordertracking.inventory.domain.ReservationRepository;
import com.jamespolk.ordertracking.inventory.domain.StockRepository;
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
class InventoryFlowIntegrationTest {

    @Autowired
    KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    StockRepository stock;

    @Autowired
    ReservationRepository reservations;

    @Autowired
    ProcessedEventRepository processedEvents;

    @Autowired
    KafkaContainer kafka;

    @Test
    void reservesStockAndPublishesInventoryReserved() {
        int before = available("SKU-2");
        OrderPlaced placed = orderPlaced(new OrderItem("SKU-2", 3, new BigDecimal("5.00")));

        publish(placed);
        awaitProcessed(placed);

        assertThat(available("SKU-2")).isEqualTo(before - 3);
        assertThat(reservations.existsByOrderId(placed.orderId())).isTrue();
        List<ConsumerRecord<String, String>> outcomes = outcomesFor(placed.orderId());
        assertThat(outcomes).hasSize(1);
        assertThat(typeHeader(outcomes.getFirst())).endsWith("InventoryReserved");
    }

    @Test
    void failsWhenStockIsInsufficientWithoutTouchingOtherLines() {
        int sku1Before = available("SKU-1");
        OrderPlaced placed = orderPlaced(
                new OrderItem("SKU-1", 1, new BigDecimal("5.00")), new OrderItem("SKU-3", 1, new BigDecimal("5.00")));

        publish(placed);
        awaitProcessed(placed);

        assertThat(available("SKU-1")).isEqualTo(sku1Before);
        assertThat(reservations.existsByOrderId(placed.orderId())).isFalse();
        List<ConsumerRecord<String, String>> outcomes = outcomesFor(placed.orderId());
        assertThat(outcomes).hasSize(1);
        assertThat(typeHeader(outcomes.getFirst())).endsWith("InventoryFailed");
        assertThat(outcomes.getFirst().value()).contains("insufficient stock for SKU-3");
    }

    @Test
    void failsForUnknownSku() {
        OrderPlaced placed = orderPlaced(new OrderItem("SKU-404", 1, new BigDecimal("5.00")));

        publish(placed);
        awaitProcessed(placed);

        List<ConsumerRecord<String, String>> outcomes = outcomesFor(placed.orderId());
        assertThat(outcomes).hasSize(1);
        assertThat(outcomes.getFirst().value()).contains("unknown sku SKU-404");
    }

    @Test
    void decrementsOnceForRedeliveredEvent() {
        int before = available("SKU-1");
        OrderPlaced placed = orderPlaced(new OrderItem("SKU-1", 2, new BigDecimal("5.00")));

        publish(placed);
        publish(placed);
        awaitProcessed(placed);

        assertThat(outcomesFor(placed.orderId())).hasSize(1);
        assertThat(available("SKU-1")).isEqualTo(before - 2);
    }

    private static OrderPlaced orderPlaced(OrderItem... items) {
        return new OrderPlaced(
                UUID.randomUUID(), UUID.randomUUID(), Instant.now(), "customer-1", List.of(items), BigDecimal.TEN);
    }

    private void publish(OrderPlaced event) {
        kafkaTemplate
                .send(Topics.ORDER_EVENTS, event.orderId().toString(), event)
                .join();
    }

    private void awaitProcessed(OrderPlaced event) {
        await().atMost(Duration.ofSeconds(20)).until(() -> processedEvents.existsById(event.eventId()));
    }

    private int available(String sku) {
        return stock.findById(sku).orElseThrow().getAvailable();
    }

    private List<ConsumerRecord<String, String>> outcomesFor(UUID orderId) {
        return KafkaTestSupport.drain(kafka.getBootstrapServers(), Topics.INVENTORY_EVENTS, Duration.ofSeconds(5))
                .stream()
                .filter(r -> r.key().equals(orderId.toString()))
                .toList();
    }

    private static String typeHeader(ConsumerRecord<String, String> record) {
        return new String(record.headers().lastHeader("__TypeId__").value());
    }
}
