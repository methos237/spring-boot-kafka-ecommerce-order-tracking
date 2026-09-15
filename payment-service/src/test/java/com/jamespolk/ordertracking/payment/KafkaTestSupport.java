package com.jamespolk.ordertracking.payment;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Drains a topic from the beginning for a fixed window. Used to assert both presence and absence.
 * {@link #drain} decodes Avro through the in-JVM mock registry the application under test also uses;
 * {@link #drainRaw} reads bytes as strings for dead letter topics.
 */
final class KafkaTestSupport {

    private KafkaTestSupport() {}

    static List<ConsumerRecord<String, Object>> drain(String bootstrapServers, String topic, Duration window) {
        Map<String, Object> props = new HashMap<>(base(bootstrapServers));
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://order-tracking");
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        props.put(KafkaAvroDeserializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG, true);
        return poll(props, topic, window);
    }

    static List<ConsumerRecord<String, String>> drainRaw(String bootstrapServers, String topic, Duration window) {
        Map<String, Object> props = new HashMap<>(base(bootstrapServers));
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return poll(props, topic, window);
    }

    static String type(ConsumerRecord<String, Object> record) {
        return record.value().getClass().getSimpleName();
    }

    private static Map<String, Object> base(String bootstrapServers) {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG,
                "test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
    }

    private static <V> List<ConsumerRecord<String, V>> poll(Map<String, Object> props, String topic, Duration window) {
        List<ConsumerRecord<String, V>> records = new ArrayList<>();
        try (KafkaConsumer<String, V> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + window.toMillis();
            while (System.currentTimeMillis() < deadline) {
                consumer.poll(Duration.ofMillis(500)).forEach(records::add);
            }
        }
        return records;
    }
}
