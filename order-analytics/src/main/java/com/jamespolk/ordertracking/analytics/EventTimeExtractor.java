package com.jamespolk.ordertracking.analytics;

import com.jamespolk.ordertracking.events.Events;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * Windows are keyed on when the order happened ({@code occurredAt}), not on when the record reached
 * the broker or this app. A replay of last week's topic lands in last week's windows.
 */
class EventTimeExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        if (record.value() instanceof SpecificRecord event) {
            return Events.occurredAt(event).toEpochMilli();
        }
        return record.timestamp();
    }
}
