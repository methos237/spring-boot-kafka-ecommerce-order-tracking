package com.jamespolk.ordertracking.order;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.jamespolk.ordertracking.events.OrderPlaced;
import com.jamespolk.ordertracking.events.Topics;
import com.jamespolk.ordertracking.order.domain.Order;
import com.jamespolk.ordertracking.order.domain.OrderLine;
import com.jamespolk.ordertracking.order.domain.OrderRepository;
import com.jamespolk.ordertracking.order.domain.OrderService;
import com.jamespolk.ordertracking.order.messaging.OutboxRepository;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.kafka.KafkaContainer;

@SpringBootTest
@Import(TestcontainersConfiguration.class)
@ActiveProfiles("test")
class OutboxRelayTest {

    @Autowired
    OrderService orderService;

    @Autowired
    OrderRepository orders;

    @Autowired
    OutboxRepository outbox;

    @Autowired
    KafkaContainer kafka;

    @Test
    void orderSurvivesBrokerOutageAndEventFollowsWhenBrokerReturns() {
        kafka.getDockerClient().pauseContainerCmd(kafka.getContainerId()).exec();
        Order order;
        try {
            order = orderService.placeOrder("customer-1", List.of(new OrderLine("SKU-1", 1, new BigDecimal("10.00"))));

            assertThat(orders.findById(order.getId())).isPresent();
            assertThat(outbox.countByOrderId(order.getId())).isEqualTo(1);
            // relay ticks fail against the paused broker and roll back; the row must still be there
            await().pollDelay(Duration.ofSeconds(4))
                    .atMost(Duration.ofSeconds(5))
                    .untilAsserted(() ->
                            assertThat(outbox.countByOrderId(order.getId())).isEqualTo(1));
        } finally {
            kafka.getDockerClient().unpauseContainerCmd(kafka.getContainerId()).exec();
        }

        await().atMost(Duration.ofSeconds(60)).until(() -> outbox.countByOrderId(order.getId()) == 0);
        List<ConsumerRecord<String, Object>> records =
                KafkaTestSupport.drain(kafka.getBootstrapServers(), Topics.ORDER_EVENTS, Duration.ofSeconds(5)).stream()
                        .filter(r -> r.key().equals(order.getId().toString()))
                        .toList();
        assertThat(records).hasSize(1);
        assertThat(records.getFirst().value()).isInstanceOf(OrderPlaced.class);
        OrderPlaced event = (OrderPlaced) records.getFirst().value();
        assertThat(event.getOrderId()).isEqualTo(order.getId());
        assertThat(event.getTotalAmount()).isEqualByComparingTo("10.00");
    }
}
