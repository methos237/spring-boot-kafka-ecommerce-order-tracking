package com.jamespolk.ordertracking.inventory.domain;

import com.jamespolk.ordertracking.events.InventoryFailed;
import com.jamespolk.ordertracking.events.InventoryReserved;
import com.jamespolk.ordertracking.events.OrderItem;
import com.jamespolk.ordertracking.events.OrderPlaced;
import com.jamespolk.ordertracking.inventory.messaging.InventoryEventPublisher;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class InventoryService {

    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);

    private final StockRepository stock;
    private final ReservationRepository reservations;
    private final ProcessedEventRepository processedEvents;
    private final InventoryEventPublisher publisher;

    public InventoryService(
            StockRepository stock,
            ReservationRepository reservations,
            ProcessedEventRepository processedEvents,
            InventoryEventPublisher publisher) {
        this.stock = stock;
        this.reservations = reservations;
        this.processedEvents = processedEvents;
        this.publisher = publisher;
    }

    @Transactional
    public void handle(OrderPlaced event) {
        if (processedEvents.existsById(event.eventId())) {
            log.info("duplicate OrderPlaced {} ignored", event.eventId());
            return;
        }
        processedEvents.save(new ProcessedEvent(event.eventId()));

        Optional<String> failure = reserve(event.items());
        if (failure.isPresent()) {
            log.info("inventory failed: {}", failure.get());
            publisher.publish(new InventoryFailed(UUID.randomUUID(), event.orderId(), Instant.now(), failure.get()));
            return;
        }
        reservations.save(new Reservation(event.orderId()));
        log.info("inventory reserved");
        publisher.publish(new InventoryReserved(UUID.randomUUID(), event.orderId(), Instant.now()));
    }

    /**
     * Locks every sku up front and checks all of them before decrementing any, so a failed order
     * never leaves a partial reservation behind. Returns the reason on failure.
     */
    private Optional<String> reserve(List<OrderItem> items) {
        Map<String, Stock> locked = stock
                .findAllBySkuIn(items.stream().map(OrderItem::sku).toList())
                .stream()
                .collect(Collectors.toMap(Stock::getSku, Function.identity()));
        for (OrderItem item : items) {
            Stock s = locked.get(item.sku());
            if (s == null) {
                return Optional.of("unknown sku " + item.sku());
            }
            if (!s.canReserve(item.quantity())) {
                return Optional.of("insufficient stock for " + item.sku() + ": requested " + item.quantity()
                        + ", available " + s.getAvailable());
            }
        }
        items.forEach(item -> locked.get(item.sku()).reserve(item.quantity()));
        return Optional.empty();
    }
}
