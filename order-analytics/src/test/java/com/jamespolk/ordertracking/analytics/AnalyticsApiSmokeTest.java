package com.jamespolk.ordertracking.analytics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.jamespolk.ordertracking.events.OrderConfirmed;
import com.jamespolk.ordertracking.events.OrderItem;
import com.jamespolk.ordertracking.events.OrderPlaced;
import com.jamespolk.ordertracking.events.Topics;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureRestTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.client.RestTestClient;

/** Real broker, real Streams app, mock registry: events in over Kafka, numbers out over HTTP. */
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@Import(TestcontainersConfiguration.class)
@AutoConfigureRestTestClient
@ActiveProfiles("test")
class AnalyticsApiSmokeTest {

    @Autowired
    KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    RestTestClient client;

    @Test
    void answersInteractiveQueriesFromStateStores() {
        String customer = "customer-" + UUID.randomUUID();
        OrderPlaced placed = new OrderPlaced(
                UUID.randomUUID(),
                UUID.randomUUID(),
                Instant.now(),
                customer,
                List.of(new OrderItem("SKU-1", 3, new BigDecimal("12.50"))),
                new BigDecimal("37.50"));
        kafkaTemplate
                .send(Topics.ORDER_EVENTS, placed.getOrderId().toString(), placed)
                .join();
        kafkaTemplate
                .send(
                        Topics.ORDER_EVENTS,
                        placed.getOrderId().toString(),
                        new OrderConfirmed(UUID.randomUUID(), placed.getOrderId(), Instant.now()))
                .join();

        await().atMost(Duration.ofSeconds(90))
                .untilAsserted(() -> client.get()
                        .uri("/analytics/customers/{id}/revenue", customer)
                        .exchange()
                        .expectStatus()
                        .isOk()
                        .expectBody()
                        .jsonPath("$.revenue")
                        .isEqualTo(37.50));

        List<?> minutes = client.get()
                .uri("/analytics/orders-per-minute?last=5")
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody(List.class)
                .returnResult()
                .getResponseBody();
        assertThat(minutes).isNotEmpty();
    }
}
