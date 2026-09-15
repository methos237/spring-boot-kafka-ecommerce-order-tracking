package com.jamespolk.ordertracking.order.messaging;

import com.jamespolk.ordertracking.events.Topics;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.mapping.AbstractJavaTypeMapper;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Drains the outbox to Kafka. Each row is sent with its stored bytes and a type header, so consumers
 * see exactly what a direct publish would have produced, including the original {@code eventId}. A
 * row is deleted only after the broker acknowledges it; a crash in between means a duplicate send,
 * which consumers already tolerate by {@code eventId}.
 */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private static final int BATCH_SIZE = 100;

    private final OutboxRepository outbox;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public OutboxRelay(OutboxRepository outbox, KafkaTemplate<String, Object> kafkaTemplate) {
        this.outbox = outbox;
        this.kafkaTemplate = kafkaTemplate;
    }

    // ponytail: single poller with a 500 ms tick; move to CDC (Debezium) if latency or DB load ever matters
    @Scheduled(fixedDelayString = "${outbox.relay.delay:500ms}")
    @Transactional
    public void relay() {
        List<OutboxMessage> batch = outbox.claimBatch(BATCH_SIZE);
        for (OutboxMessage message : batch) {
            try {
                kafkaTemplate.send(toRecord(message)).get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                // roll back the whole batch; every row stays and is retried on the next tick
                throw new IllegalStateException("outbox relay failed on " + message.getEventType(), e);
            }
            outbox.delete(message);
        }
        if (!batch.isEmpty()) {
            log.debug("relayed {} outbox rows", batch.size());
        }
    }

    private static ProducerRecord<String, Object> toRecord(OutboxMessage message) {
        var record = new ProducerRecord<String, Object>(
                Topics.ORDER_EVENTS, message.getOrderId().toString(), message.getPayload());
        record.headers()
                .add(new RecordHeader(
                        AbstractJavaTypeMapper.DEFAULT_CLASSID_FIELD_NAME,
                        message.getEventType().getBytes(StandardCharsets.UTF_8)));
        return record;
    }
}
