package com.jamespolk.ordertracking.inventory.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "reservations")
public class Reservation {

    @Id
    @Column(name = "id", nullable = false)
    private UUID id;

    @Column(name = "order_id", nullable = false, unique = true)
    private UUID orderId;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected Reservation() {}

    public Reservation(UUID orderId) {
        this.id = UUID.randomUUID();
        this.orderId = orderId;
        this.createdAt = Instant.now();
    }

    public UUID getOrderId() {
        return orderId;
    }
}
