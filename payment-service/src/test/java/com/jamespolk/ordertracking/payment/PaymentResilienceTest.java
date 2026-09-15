package com.jamespolk.ordertracking.payment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.jamespolk.ordertracking.events.OrderItem;
import com.jamespolk.ordertracking.events.OrderPlaced;
import com.jamespolk.ordertracking.events.PaymentEvent;
import com.jamespolk.ordertracking.events.Topics;
import com.jamespolk.ordertracking.payment.domain.PaymentRepository;
import com.jamespolk.ordertracking.payment.messaging.PaymentEventPublisher;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.kafka.KafkaContainer;

@SpringBootTest
@Import(TestcontainersConfiguration.class)
@ActiveProfiles("test")
class PaymentResilienceTest {

    @Autowired
    KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    KafkaContainer kafka;

    @Autowired
    PaymentRepository payments;

    @MockitoSpyBean
    PaymentEventPublisher publisher;

    @Test
    void poisonRecordGoesStraightToDeadLetterTopicWithOriginalBytes() {
        byte[] poison = "{not json".getBytes(StandardCharsets.UTF_8);
        String key = "poison-" + UUID.randomUUID();
        try (var producer = new KafkaProducer<String, byte[]>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class))) {
            producer.send(new ProducerRecord<>(Topics.ORDER_EVENTS, key, poison));
            producer.flush();
        }

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            List<ConsumerRecord<String, String>> dead = KafkaTestSupport.drain(
                            kafka.getBootstrapServers(), Topics.ORDER_EVENTS + Topics.DLT_SUFFIX, Duration.ofSeconds(3))
                    .stream()
                    .filter(r -> key.equals(r.key()))
                    .toList();
            assertThat(dead).hasSize(1);
            ConsumerRecord<String, String> record = dead.getFirst();
            assertThat(record.value()).isEqualTo("{not json");
            assertThat(new String(record.headers()
                            .lastHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC)
                            .value()))
                    .isEqualTo(Topics.ORDER_EVENTS);
            assertThat(new String(record.headers()
                            .lastHeader(KafkaHeaders.DLT_EXCEPTION_MESSAGE)
                            .value()))
                    .contains("deserializ");
        });
    }

    @Test
    void retriesTransientFailureAndProcessesExactlyOnce() {
        // publish runs inside the handler's transaction, so each failure rolls back the row and the
        // processed_events insert, and the retry starts clean
        doThrow(new TransientDataAccessResourceException("broker hiccup 1"))
                .doThrow(new TransientDataAccessResourceException("broker hiccup 2"))
                .doCallRealMethod()
                .when(publisher)
                .publish(any(PaymentEvent.class));
        OrderPlaced placed = new OrderPlaced(
                UUID.randomUUID(),
                UUID.randomUUID(),
                Instant.now(),
                "customer-1",
                List.of(new OrderItem("SKU-1", 1, new BigDecimal("42.00"))),
                new BigDecimal("42.00"));

        kafkaTemplate
                .send(Topics.ORDER_EVENTS, placed.orderId().toString(), placed)
                .join();

        await().atMost(Duration.ofSeconds(30))
                .until(() -> payments.findByOrderId(placed.orderId()).isPresent());
        verify(publisher, times(3)).publish(any(PaymentEvent.class));
        List<ConsumerRecord<String, String>> outcomes =
                KafkaTestSupport.drain(kafka.getBootstrapServers(), Topics.PAYMENT_EVENTS, Duration.ofSeconds(5))
                        .stream()
                        .filter(r -> r.key().equals(placed.orderId().toString()))
                        .toList();
        assertThat(outcomes).hasSize(1);
        assertThat(KafkaTestSupport.drain(
                                kafka.getBootstrapServers(),
                                Topics.ORDER_EVENTS + Topics.DLT_SUFFIX,
                                Duration.ofSeconds(2))
                        .stream()
                        .filter(r -> r.key().equals(placed.orderId().toString())))
                .isEmpty();
    }
}
