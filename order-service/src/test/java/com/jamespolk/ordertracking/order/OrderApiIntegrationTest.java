package com.jamespolk.ordertracking.order;

import static org.assertj.core.api.Assertions.assertThat;

import com.jamespolk.ordertracking.events.OrderPlaced;
import com.jamespolk.ordertracking.events.Topics;
import com.jamespolk.ordertracking.order.api.OrderResponse;
import com.jamespolk.ordertracking.order.domain.OrderStatus;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureRestTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.client.RestTestClient;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.json.JsonMapper;

@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@Import(TestcontainersConfiguration.class)
@AutoConfigureRestTestClient
@ActiveProfiles("test")
class OrderApiIntegrationTest {

    @Autowired
    RestTestClient client;

    @Autowired
    JsonMapper jsonMapper;

    @Autowired
    KafkaContainer kafka;

    @Test
    void placesOrderAndPublishesOrderPlacedKeyedByOrderId() {
        String body = """
                {"customerId":"customer-1","items":[{"sku":"SKU-1","quantity":2,"unitPrice":19.99}]}
                """;

        OrderResponse created = client.post()
                .uri("/api/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .body(body)
                .exchange()
                .expectStatus()
                .isCreated()
                .expectHeader()
                .exists("Location")
                .expectBody(OrderResponse.class)
                .returnResult()
                .getResponseBody();

        assertThat(created.status()).isEqualTo(OrderStatus.PENDING);
        assertThat(created.totalAmount()).isEqualByComparingTo(new BigDecimal("39.98"));

        client.get()
                .uri("/api/orders/{id}", created.id())
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody()
                .jsonPath("$.status")
                .isEqualTo("PENDING");

        ConsumerRecord<String, String> record = consumeOne(Topics.ORDER_EVENTS);
        assertThat(record.key()).isEqualTo(created.id().toString());
        OrderPlaced event = jsonMapper.readValue(record.value(), OrderPlaced.class);
        assertThat(event.orderId()).isEqualTo(created.id());
        assertThat(event.customerId()).isEqualTo("customer-1");
        assertThat(event.totalAmount()).isEqualByComparingTo(new BigDecimal("39.98"));
        assertThat(event.items()).hasSize(1);
    }

    @Test
    void rejectsInvalidOrderWithProblemDetail() {
        client.post()
                .uri("/api/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .body("{\"customerId\":\"\",\"items\":[]}")
                .exchange()
                .expectStatus()
                .isBadRequest()
                .expectHeader()
                .contentType(MediaType.APPLICATION_PROBLEM_JSON)
                .expectBody()
                .jsonPath("$.title")
                .isEqualTo("Validation failed")
                .jsonPath("$.errors.length()")
                .isEqualTo(2);
    }

    @Test
    void returnsProblemDetailForUnknownOrder() {
        client.get()
                .uri("/api/orders/{id}", UUID.randomUUID())
                .exchange()
                .expectStatus()
                .isNotFound()
                .expectHeader()
                .contentType(MediaType.APPLICATION_PROBLEM_JSON)
                .expectBody()
                .jsonPath("$.title")
                .isEqualTo("Order not found");
    }

    private ConsumerRecord<String, String> consumeOne(String topic) {
        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG,
                "test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            return KafkaTestUtils.getSingleRecord(consumer, topic, Duration.ofSeconds(15));
        }
    }
}
