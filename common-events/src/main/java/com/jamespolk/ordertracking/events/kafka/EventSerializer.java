package com.jamespolk.ordertracking.events.kafka;

import com.jamespolk.ordertracking.events.Events;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;

/**
 * Avro (Confluent wire format, schema id from the registry) for events, passthrough for raw bytes.
 * The dead-letter recoverer republishes a record that failed deserialization as its original
 * {@code byte[]}; without the passthrough the DLT would hold a re-encoded blob instead of the payload
 * that actually broke.
 */
public class EventSerializer extends DelegatingByTypeSerializer {

    public EventSerializer() {
        super(delegates(), true);
        Events.trustEventClasses();
    }

    private static Map<Class<?>, Serializer<?>> delegates() {
        Map<Class<?>, Serializer<?>> map = new LinkedHashMap<>();
        map.put(byte[].class, new ByteArraySerializer());
        map.put(SpecificRecord.class, new KafkaAvroSerializer());
        return map;
    }
}
