package com.jamespolk.ordertracking.events.kafka;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Stamps every outgoing record with an {@code X-Order-Id} header taken from the record key, unless a
 * caller already set one. Registered through {@code interceptor.classes} so publishers stay unaware.
 */
public class OrderIdHeaderInterceptor implements ProducerInterceptor<Object, Object> {

    public static final String HEADER = "X-Order-Id";

    @Override
    public ProducerRecord<Object, Object> onSend(ProducerRecord<Object, Object> record) {
        if (record.headers().lastHeader(HEADER) == null && record.key() instanceof String key) {
            record.headers().add(HEADER, key.getBytes(StandardCharsets.UTF_8));
        }
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {}

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> configs) {}
}
