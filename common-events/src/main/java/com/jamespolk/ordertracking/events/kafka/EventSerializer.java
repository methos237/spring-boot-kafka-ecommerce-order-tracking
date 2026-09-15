package com.jamespolk.ordertracking.events.kafka;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

/**
 * JSON for events, passthrough for raw bytes. The dead-letter recoverer republishes a record that
 * failed deserialization as its original {@code byte[]}; without the passthrough the DLT would hold
 * a base64 string instead of the payload that actually broke.
 */
public class EventSerializer extends DelegatingByTypeSerializer {

    public EventSerializer() {
        super(delegates(), true);
    }

    private static Map<Class<?>, Serializer<?>> delegates() {
        Map<Class<?>, Serializer<?>> map = new LinkedHashMap<>();
        map.put(byte[].class, new ByteArraySerializer());
        map.put(Object.class, new JacksonJsonSerializer<>());
        return map;
    }
}
