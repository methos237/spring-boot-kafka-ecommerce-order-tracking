package com.jamespolk.ordertracking.events;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.avro.specific.SpecificRecord;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class EventAvroRoundTripTest {

    private static final UUID EVENT_ID = UUID.randomUUID();
    private static final UUID ORDER_ID = UUID.randomUUID();
    private static final Instant AT = Instant.parse("2026-09-14T12:00:00.123Z");

    static Stream<SpecificRecord> events() {
        return Stream.of(
                new OrderPlaced(
                        EVENT_ID,
                        ORDER_ID,
                        AT,
                        "customer-1",
                        List.of(new OrderItem("SKU-1", 2, new BigDecimal("19.99"))),
                        new BigDecimal("39.98")),
                new OrderConfirmed(EVENT_ID, ORDER_ID, AT),
                new OrderCancelled(EVENT_ID, ORDER_ID, AT, "payment failed"),
                new PaymentSucceeded(EVENT_ID, ORDER_ID, AT, new BigDecimal("39.98")),
                new PaymentFailed(EVENT_ID, ORDER_ID, AT, "amount over limit"),
                new PaymentRefunded(EVENT_ID, ORDER_ID, AT, new BigDecimal("39.98")),
                new InventoryReserved(EVENT_ID, ORDER_ID, AT),
                new InventoryFailed(EVENT_ID, ORDER_ID, AT, "insufficient stock"));
    }

    @ParameterizedTest
    @MethodSource("events")
    void roundTripsThroughAvroBinary(SpecificRecord event) {
        byte[] bytes = Events.toBytes(event);

        SpecificRecord back = Events.fromBytes(event.getClass(), bytes);

        assertThat(back).isEqualTo(event);
        assertThat(Events.eventId(back)).isEqualTo(EVENT_ID);
        assertThat(Events.orderId(back)).isEqualTo(ORDER_ID);
        assertThat(Events.occurredAt(back)).isEqualTo(AT);
        assertThat(Events.type(back)).isEqualTo(event.getClass().getSimpleName());
    }
}
