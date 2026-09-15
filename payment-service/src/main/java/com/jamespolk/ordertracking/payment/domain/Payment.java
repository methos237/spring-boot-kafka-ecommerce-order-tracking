package com.jamespolk.ordertracking.payment.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "payments")
public class Payment {

    /** Deterministic rule so every failure is reproducible from the request. */
    public static final BigDecimal LIMIT = new BigDecimal("1000.00");

    @Id
    @Column(name = "id", nullable = false)
    private UUID id;

    @Column(name = "order_id", nullable = false, unique = true)
    private UUID orderId;

    @Column(name = "amount", nullable = false, precision = 12, scale = 2)
    private BigDecimal amount;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private PaymentStatus status;

    @Column(name = "reason")
    private String reason;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected Payment() {}

    private Payment(UUID orderId, BigDecimal amount, PaymentStatus status, String reason) {
        this.id = UUID.randomUUID();
        this.orderId = orderId;
        this.amount = amount;
        this.status = status;
        this.reason = reason;
        this.createdAt = Instant.now();
    }

    public static Payment charge(UUID orderId, BigDecimal amount) {
        if (amount.compareTo(LIMIT) > 0) {
            return new Payment(orderId, amount, PaymentStatus.FAILED, "amount " + amount + " exceeds limit " + LIMIT);
        }
        return new Payment(orderId, amount, PaymentStatus.SUCCEEDED, null);
    }

    public boolean succeeded() {
        return status == PaymentStatus.SUCCEEDED;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public PaymentStatus getStatus() {
        return status;
    }

    public String getReason() {
        return reason;
    }
}
