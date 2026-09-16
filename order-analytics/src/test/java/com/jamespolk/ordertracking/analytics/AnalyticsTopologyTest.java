package com.jamespolk.ordertracking.analytics;

import static org.assertj.core.api.Assertions.assertThat;

import com.jamespolk.ordertracking.events.Events;
import com.jamespolk.ordertracking.events.OrderConfirmed;
import com.jamespolk.ordertracking.events.OrderItem;
import com.jamespolk.ordertracking.events.OrderPlaced;
import com.jamespolk.ordertracking.events.Topics;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Drives the topology in-process: no broker, no registry container, milliseconds per run. */
class AnalyticsTopologyTest {

    static final Instant T0 = Instant.parse("2026-09-16T10:00:00Z");
    static final String REGISTRY = "mock://analytics-topology-test";

    TopologyTestDriver driver;
    TestInputTopic<String, SpecificRecord> input;

    @BeforeEach
    void start() throws IOException {
        StreamsBuilder builder = new StreamsBuilder();
        new AnalyticsTopology(REGISTRY).orderEvents(builder);
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "analytics-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
        props.put(
                StreamsConfig.STATE_DIR_CONFIG,
                Files.createTempDirectory("analytics-test").toString());
        driver = new TopologyTestDriver(builder.build(), props);
        Serde<SpecificRecord> serde = AnalyticsTopology.avroSerde(REGISTRY);
        input = driver.createInputTopic(Topics.ORDER_EVENTS, new StringSerializer(), serde.serializer());
    }

    @AfterEach
    void stop() {
        driver.close();
    }

    @Test
    void countsPlacedOrdersPerEventTimeMinute() {
        pipe(placed("alice", "10.00", T0));
        pipe(placed("bob", "20.00", T0.plusSeconds(10)));
        pipe(placed("alice", "5.00", T0.plusSeconds(70)));

        WindowStore<String, Long> store = driver.getWindowStore(AnalyticsTopology.ORDERS_PER_MINUTE);
        List<KeyValue<Long, Long>> windows = new ArrayList<>();
        try (WindowStoreIterator<Long> it =
                store.fetch(AnalyticsTopology.ALL_ORDERS_KEY, T0.minusSeconds(60), T0.plusSeconds(120))) {
            it.forEachRemaining(windows::add);
        }
        assertThat(windows)
                .containsExactly(
                        KeyValue.pair(T0.toEpochMilli(), 2L),
                        KeyValue.pair(T0.plusSeconds(60).toEpochMilli(), 1L));
    }

    @Test
    void sumsRevenueOnlyForConfirmedOrdersPerCustomer() {
        OrderPlaced a1 = placed("alice", "10.00", T0);
        OrderPlaced a2 = placed("alice", "5.00", T0.plusSeconds(5));
        OrderPlaced b1 = placed("bob", "20.00", T0.plusSeconds(10));
        pipe(a1);
        pipe(a2);
        pipe(b1);
        pipe(confirmed(a1));
        pipe(confirmed(b1));
        pipe(confirmed(a1)); // a redelivered confirmation is a second join hit: see README

        KeyValueStore<String, Long> revenue = driver.getKeyValueStore(AnalyticsTopology.REVENUE_BY_CUSTOMER);
        assertThat(revenue.get("alice")).isEqualTo(2000L);
        assertThat(revenue.get("bob")).isEqualTo(2000L);
        assertThat(revenue.get("carol")).isNull();
    }

    /** Keyed by order id like every real producer; the stream-table join depends on it. */
    private void pipe(SpecificRecord event) {
        input.pipeInput(Events.orderId(event).toString(), event);
    }

    private static OrderPlaced placed(String customer, String total, Instant at) {
        return new OrderPlaced(
                UUID.randomUUID(),
                UUID.randomUUID(),
                at,
                customer,
                List.of(new OrderItem("SKU-1", 1, new BigDecimal(total))),
                new BigDecimal(total));
    }

    private static OrderConfirmed confirmed(OrderPlaced placed) {
        return new OrderConfirmed(
                UUID.randomUUID(), placed.getOrderId(), placed.getOccurredAt().plusSeconds(1));
    }
}
