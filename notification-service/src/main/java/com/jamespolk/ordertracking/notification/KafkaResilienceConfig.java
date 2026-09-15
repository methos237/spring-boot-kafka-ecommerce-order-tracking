package com.jamespolk.ordertracking.notification;

import com.jamespolk.ordertracking.events.Topics;
import com.jamespolk.ordertracking.events.kafka.OrderIdMdcInterceptor;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RecordInterceptor;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Retry a failing record three times, two seconds apart, then publish it to {@code <topic>.DLT}.
 * Deserialization failures and other non-retryable exceptions skip the retries.
 */
@Configuration
class KafkaResilienceConfig {

    static final long RETRY_INTERVAL_MS = 2_000L;
    static final long MAX_RETRIES = 3L;

    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<Object, Object> kafkaTemplate) {
        var recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate, (record, ex) -> new TopicPartition(record.topic() + Topics.DLT_SUFFIX, -1));
        var handler = new DefaultErrorHandler(recoverer, new FixedBackOff(RETRY_INTERVAL_MS, MAX_RETRIES));
        handler.addNotRetryableExceptions(IllegalArgumentException.class);
        return handler;
    }

    /** Picked up by Boot's listener container factory; puts the order id in the MDC per record. */
    @Bean
    RecordInterceptor<Object, Object> orderIdMdcInterceptor() {
        return new OrderIdMdcInterceptor();
    }
}
