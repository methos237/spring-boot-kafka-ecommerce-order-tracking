package com.jamespolk.ordertracking.analytics;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;
import org.springframework.web.ErrorResponseException;

/** Interactive queries: read the local state stores directly, no extra database. */
@Service
class AnalyticsQueries {

    record MinuteCount(Instant minute, long orders) {}

    record CustomerRevenue(String customerId, BigDecimal revenue) {}

    private final StreamsBuilderFactoryBean streams;

    AnalyticsQueries(StreamsBuilderFactoryBean streams) {
        this.streams = streams;
    }

    List<MinuteCount> ordersPerMinute(int lastMinutes) {
        Instant to = Instant.now();
        Instant from = to.minus(Duration.ofMinutes(lastMinutes)).truncatedTo(ChronoUnit.MINUTES);
        ReadOnlyWindowStore<String, Long> store =
                store(AnalyticsTopology.ORDERS_PER_MINUTE, QueryableStoreTypes.windowStore());
        List<MinuteCount> result = new ArrayList<>();
        try (WindowStoreIterator<Long> windows = store.fetch(AnalyticsTopology.ALL_ORDERS_KEY, from, to)) {
            windows.forEachRemaining(w -> result.add(new MinuteCount(Instant.ofEpochMilli(w.key), w.value)));
        }
        return result;
    }

    CustomerRevenue revenue(String customerId) {
        ReadOnlyKeyValueStore<String, Long> store =
                store(AnalyticsTopology.REVENUE_BY_CUSTOMER, QueryableStoreTypes.keyValueStore());
        Long cents = store.get(customerId);
        return new CustomerRevenue(
                customerId, cents == null ? BigDecimal.ZERO.setScale(2) : BigDecimal.valueOf(cents, 2));
    }

    private <T> T store(String name, QueryableStoreType<T> type) {
        KafkaStreams kafkaStreams = streams.getKafkaStreams();
        if (kafkaStreams == null || kafkaStreams.state() != KafkaStreams.State.RUNNING) {
            throw unavailable("stream processor is " + (kafkaStreams == null ? "not started" : kafkaStreams.state()));
        }
        try {
            return kafkaStreams.store(StoreQueryParameters.fromNameAndType(name, type));
        } catch (InvalidStateStoreException e) {
            throw unavailable(e.getMessage());
        }
    }

    private static ErrorResponseException unavailable(String detail) {
        ErrorResponseException e = new ErrorResponseException(HttpStatus.SERVICE_UNAVAILABLE);
        e.setTitle("Analytics not ready");
        e.setDetail(detail);
        return e;
    }
}
