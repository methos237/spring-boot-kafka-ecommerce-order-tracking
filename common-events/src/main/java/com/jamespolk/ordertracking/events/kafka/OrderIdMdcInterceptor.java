package com.jamespolk.ordertracking.events.kafka;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.MDC;
import org.springframework.kafka.listener.RecordInterceptor;

/**
 * Puts the order id into the logging MDC for the duration of each record's listener call, so every
 * log line a consumer writes while handling an order carries {@code [order=<id>]} without the code
 * having to pass it around. Reads the {@code X-Order-Id} header, falls back to the record key.
 */
public class OrderIdMdcInterceptor implements RecordInterceptor<Object, Object> {

    public static final String MDC_KEY = "orderId";

    @Override
    public ConsumerRecord<Object, Object> intercept(
            ConsumerRecord<Object, Object> record, Consumer<Object, Object> consumer) {
        Header header = record.headers().lastHeader(OrderIdHeaderInterceptor.HEADER);
        String orderId = header != null && header.value() != null
                ? new String(header.value(), StandardCharsets.UTF_8)
                : String.valueOf(record.key());
        MDC.put(MDC_KEY, orderId);
        return record;
    }

    @Override
    public void afterRecord(ConsumerRecord<Object, Object> record, Consumer<Object, Object> consumer) {
        MDC.remove(MDC_KEY);
    }
}
