package com.jamespolk.ordertracking.events;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** A producer on schema v2 (new optional field) must still be readable by a consumer compiled against v1. */
class SchemaEvolutionTest {

    /** Reading into a generated class needs the trust list; production JVMs get it from the serializer and interceptor. */
    @BeforeAll
    static void trustEvents() {
        Events.trustEventClasses();
    }

    @Test
    void v1ConsumerReadsV2Record() throws IOException {
        Schema v2 = new Schema.Parser().parse(getClass().getResourceAsStream("/avro/OrderPlaced_v2.avsc"));
        Schema itemSchema = v2.getField("items").schema().getElementType();
        GenericData model = new GenericData();
        model.addLogicalTypeConversion(new Conversions.UUIDConversion());
        model.addLogicalTypeConversion(new Conversions.DecimalConversion());
        model.addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());
        UUID orderId = UUID.randomUUID();
        GenericRecord item = new GenericRecordBuilder(itemSchema)
                .set("sku", "SKU-1")
                .set("quantity", 1)
                .set("unitPrice", new BigDecimal("12.50"))
                .build();
        GenericRecord v2Record = new GenericRecordBuilder(v2)
                .set("eventId", UUID.randomUUID())
                .set("orderId", orderId)
                .set("occurredAt", Instant.parse("2026-09-15T10:00:00Z"))
                .set("customerId", "customer-1")
                .set("items", List.of(item))
                .set("totalAmount", new BigDecimal("12.50"))
                .set("channel", "mobile")
                .build();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        new GenericDatumWriter<GenericRecord>(v2, model).write(v2Record, encoder);
        encoder.flush();

        SpecificData v1Data = SpecificData.getForClass(OrderPlaced.class);
        SpecificDatumReader<OrderPlaced> v1Reader = new SpecificDatumReader<>(v2, OrderPlaced.getClassSchema(), v1Data);
        OrderPlaced read = v1Reader.read(null, DecoderFactory.get().binaryDecoder(out.toByteArray(), null));

        assertThat(read.getOrderId()).isEqualTo(orderId);
        assertThat(read.getCustomerId()).isEqualTo("customer-1");
        assertThat(read.getTotalAmount()).isEqualByComparingTo("12.50");
        assertThat(read.getItems()).hasSize(1);
    }
}
