package com.jamespolk.ordertracking.notification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.jamespolk.ordertracking.events.Events;
import com.jamespolk.ordertracking.events.InventoryReserved;
import com.jamespolk.ordertracking.events.OrderConfirmed;
import com.jamespolk.ordertracking.events.OrderItem;
import com.jamespolk.ordertracking.events.OrderPlaced;
import com.jamespolk.ordertracking.events.PaymentSucceeded;
import com.jamespolk.ordertracking.events.Topics;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.apache.avro.specific.SpecificRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@Import(TestcontainersConfiguration.class)
@ActiveProfiles("test")
@ExtendWith(OutputCaptureExtension.class)
class OrderTimelineListenerTest {

    @Autowired
    KafkaTemplate<String, Object> kafkaTemplate;

    @Test
    void logsOneTimelineLinePerEvent(CapturedOutput output) {
        UUID orderId = UUID.randomUUID();
        BigDecimal total = new BigDecimal("39.98");

        publish(
                Topics.ORDER_EVENTS,
                new OrderPlaced(
                        UUID.randomUUID(),
                        orderId,
                        Instant.now(),
                        "customer-1",
                        List.of(new OrderItem("SKU-1", 2, new BigDecimal("19.99"))),
                        total));
        publish(Topics.PAYMENT_EVENTS, new PaymentSucceeded(UUID.randomUUID(), orderId, Instant.now(), total));
        publish(Topics.INVENTORY_EVENTS, new InventoryReserved(UUID.randomUUID(), orderId, Instant.now()));
        publish(Topics.ORDER_EVENTS, new OrderConfirmed(UUID.randomUUID(), orderId, Instant.now()));

        String prefix = "\\[order=" + orderId + "\\] .*";
        await().atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> assertThat(output.getOut())
                        .containsPattern(prefix + "OrderPlaced customer=customer-1 items=1 total=39.98")
                        .containsPattern(prefix + "PaymentSucceeded amount=39.98")
                        .containsPattern(prefix + "InventoryReserved")
                        .containsPattern(prefix + "OrderConfirmed"));
    }

    private void publish(String topic, SpecificRecord event) {
        kafkaTemplate.send(topic, Events.orderId(event).toString(), event).join();
    }
}
