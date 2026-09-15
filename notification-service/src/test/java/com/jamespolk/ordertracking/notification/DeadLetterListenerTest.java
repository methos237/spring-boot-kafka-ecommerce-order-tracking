package com.jamespolk.ordertracking.notification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.jamespolk.ordertracking.events.Topics;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.kafka.KafkaContainer;

@SpringBootTest
@Import(TestcontainersConfiguration.class)
@ActiveProfiles("test")
@ExtendWith(OutputCaptureExtension.class)
class DeadLetterListenerTest {

    @Autowired
    KafkaContainer kafka;

    @Test
    void poisonRecordOnAnyTopicIsDeadLetteredAndLogged(CapturedOutput output) {
        byte[] poison = "this is not json".getBytes(StandardCharsets.UTF_8);
        try (var producer = new KafkaProducer<String, byte[]>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class))) {
            producer.send(new ProducerRecord<>(Topics.PAYMENT_EVENTS, "poison", poison));
            producer.flush();
        }

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(output.getOut())
                        .contains("DEAD LETTER topic=payment-events.DLT key=poison from=payment-events@")
                        .contains("group=notification-group")
                        .contains("exception=")
                        .contains("payload=this is not json"));
    }
}
