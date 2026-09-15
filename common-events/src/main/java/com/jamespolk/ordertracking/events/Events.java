package com.jamespolk.ordertracking.events;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.UUID;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.util.ClassSecurityValidator;

/**
 * Envelope access and registry-free binary encoding for the generated event classes. Every event
 * schema starts with {@code eventId}, {@code orderId}, {@code occurredAt}; this is the one place
 * that knows that.
 */
public final class Events {

    private static final String PACKAGE = Events.class.getPackageName();

    static {
        trustEventClasses();
    }

    private Events() {}

    /**
     * Avro 1.12 refuses to resolve a schema to a Java class it has not been told to trust (a
     * deserialization-gadget defence). Every JVM that reads events with the specific reader, which
     * includes Confluent's deserializer, must call this once before the first record arrives.
     * Idempotent; wired from the serializer and the consumer interceptor so no service can forget.
     */
    public static void trustEventClasses() {
        ClassSecurityValidator.setGlobal(ClassSecurityValidator.composite(
                ClassSecurityValidator.DEFAULT, clazz -> clazz.getPackageName().equals(PACKAGE)));
    }

    public static UUID eventId(SpecificRecord event) {
        return (UUID) field(event, "eventId");
    }

    public static UUID orderId(SpecificRecord event) {
        return (UUID) field(event, "orderId");
    }

    public static Instant occurredAt(SpecificRecord event) {
        return (Instant) field(event, "occurredAt");
    }

    public static String type(SpecificRecord event) {
        return event.getSchema().getName();
    }

    /** Avro binary of the datum alone, no schema id. Used to park events in the outbox. */
    public static byte[] toBytes(SpecificRecord event) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            new SpecificDatumWriter<>(event.getSchema(), SpecificData.getForClass(event.getClass()))
                    .write(event, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static <T extends SpecificRecord> T fromBytes(Class<T> type, byte[] bytes) {
        try {
            SpecificData data = SpecificData.getForClass(type);
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
            SpecificDatumReader<T> reader = new SpecificDatumReader<>(data.getSchema(type), data.getSchema(type), data);
            return reader.read(null, decoder);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Object field(SpecificRecord event, String name) {
        return event.get(event.getSchema().getField(name).pos());
    }
}
