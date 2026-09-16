package com.jamespolk.ordertracking.analytics;

import com.jamespolk.ordertracking.events.Events;
import com.jamespolk.ordertracking.events.OrderConfirmed;
import com.jamespolk.ordertracking.events.OrderPlaced;
import com.jamespolk.ordertracking.events.Topics;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Two aggregations over {@code order-events}, both queryable in place:
 *
 * <ul>
 *   <li>{@code orders-per-minute}: tumbling one-minute count of {@code OrderPlaced}, event time.
 *   <li>{@code revenue-by-customer}: {@code OrderConfirmed} joined back to its {@code OrderPlaced}
 *       (same key, same partition, so the table is always ahead of the stream), summed per customer.
 *       Cancelled orders never confirm, so they never count.
 * </ul>
 */
@Configuration
class AnalyticsTopology {

    static final String ORDERS_PER_MINUTE = "orders-per-minute";
    static final String REVENUE_BY_CUSTOMER = "revenue-by-customer";
    static final String PLACED_ORDERS = "placed-orders";
    static final String ALL_ORDERS_KEY = "all";
    static final Duration WINDOW = Duration.ofMinutes(1);

    private final String schemaRegistryUrl;

    AnalyticsTopology(@Value("${spring.kafka.properties.schema.registry.url}") String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
        Events.trustEventClasses();
    }

    /**
     * Streams refuses to start without its source topic. order-service owns the topic, but declaring it
     * here too (same partition count, idempotent create) removes a startup-order dependency.
     */
    @Bean
    NewTopic orderEventsTopic() {
        return TopicBuilder.name(Topics.ORDER_EVENTS).partitions(3).build();
    }

    @Bean
    KStream<String, SpecificRecord> orderEvents(StreamsBuilder builder) {
        Serde<SpecificRecord> eventSerde = avroSerde(schemaRegistryUrl);
        Serde<OrderPlaced> placedSerde = avroSerde(schemaRegistryUrl);

        KStream<String, SpecificRecord> events = builder.stream(
                Topics.ORDER_EVENTS,
                Consumed.with(Serdes.String(), eventSerde).withTimestampExtractor(new EventTimeExtractor()));

        KStream<String, OrderPlaced> placed =
                events.filter((orderId, event) -> event instanceof OrderPlaced).mapValues(OrderPlaced.class::cast);

        placed.groupBy((orderId, order) -> ALL_ORDERS_KEY, Grouped.with(Serdes.String(), placedSerde))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(WINDOW))
                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as(ORDERS_PER_MINUTE)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()));

        KTable<String, OrderPlaced> placedTable =
                placed.toTable(Materialized.<String, OrderPlaced, KeyValueStore<Bytes, byte[]>>as(PLACED_ORDERS)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(placedSerde));

        events.filter((orderId, event) -> event instanceof OrderConfirmed)
                .join(placedTable, (confirmed, order) -> order, Joined.with(Serdes.String(), eventSerde, placedSerde))
                .groupBy((orderId, order) -> order.getCustomerId(), Grouped.with(Serdes.String(), placedSerde))
                .aggregate(
                        () -> 0L,
                        (customerId, order, cents) -> cents + toCents(order.getTotalAmount()),
                        Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(REVENUE_BY_CUSTOMER)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Long()));
        return events;
    }

    /** Money in the state store as long cents: exact, and a built-in serde. */
    static long toCents(BigDecimal amount) {
        return amount.movePointRight(2).longValueExact();
    }

    static <T extends SpecificRecord> Serde<T> avroSerde(String schemaRegistryUrl) {
        SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
        serde.configure(
                Map.of(
                        KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
                        schemaRegistryUrl,
                        KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG,
                        true,
                        KafkaAvroDeserializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG,
                        true,
                        KafkaAvroDeserializerConfig.VALUE_SUBJECT_NAME_STRATEGY,
                        TopicRecordNameStrategy.class.getName()),
                false);
        return serde;
    }
}
