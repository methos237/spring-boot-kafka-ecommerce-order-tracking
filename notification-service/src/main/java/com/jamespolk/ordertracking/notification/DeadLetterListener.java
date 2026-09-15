package com.jamespolk.ordertracking.notification;

import com.jamespolk.ordertracking.events.Topics;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

/**
 * Operator view of every dead letter. Reads raw bytes so a payload that broke deserialization
 * upstream cannot break it again here.
 */
@Component
class DeadLetterListener {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterListener.class);

    @KafkaListener(
            topics = {
                Topics.ORDER_EVENTS + Topics.DLT_SUFFIX,
                Topics.PAYMENT_EVENTS + Topics.DLT_SUFFIX,
                Topics.INVENTORY_EVENTS + Topics.DLT_SUFFIX
            },
            groupId = "notification-dlt-group",
            properties =
                    "spring.deserializer.value.delegate.class=org.apache.kafka.common.serialization.ByteArrayDeserializer")
    void on(ConsumerRecord<String, byte[]> record) {
        log.warn(
                "DEAD LETTER topic={} key={} from={}@{} group={} exception={} payload={}",
                record.topic(),
                record.key(),
                header(record, KafkaHeaders.DLT_ORIGINAL_TOPIC),
                header(record, KafkaHeaders.DLT_ORIGINAL_OFFSET),
                header(record, KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP),
                header(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE),
                record.value() == null ? null : new String(record.value(), StandardCharsets.UTF_8));
    }

    private static String header(ConsumerRecord<?, ?> record, String name) {
        Header h = record.headers().lastHeader(name);
        if (h == null || h.value() == null) {
            return "?";
        }
        if (name.equals(KafkaHeaders.DLT_ORIGINAL_OFFSET)) {
            return Long.toString(java.nio.ByteBuffer.wrap(h.value()).getLong());
        }
        return new String(h.value(), StandardCharsets.UTF_8);
    }
}
