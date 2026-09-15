package com.jamespolk.ordertracking.order.domain;

import jakarta.persistence.CollectionTable;
import jakarta.persistence.Column;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Entity
@Table(name = "orders")
public class Order {

    @Id
    @Column(name = "id", nullable = false)
    private UUID id;

    @Column(name = "customer_id", nullable = false)
    private String customerId;

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(name = "order_items", joinColumns = @JoinColumn(name = "order_id"))
    private List<OrderLine> items = new ArrayList<>();

    @Column(name = "total_amount", nullable = false, precision = 12, scale = 2)
    private BigDecimal totalAmount;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private OrderStatus status;

    @Enumerated(EnumType.STRING)
    @Column(name = "payment_status", nullable = false)
    private StepStatus paymentStatus;

    @Enumerated(EnumType.STRING)
    @Column(name = "inventory_status", nullable = false)
    private StepStatus inventoryStatus;

    @Column(name = "cancel_reason")
    private String cancelReason;

    @Version
    @Column(name = "version", nullable = false)
    private long version;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected Order() {}

    private Order(UUID id, String customerId, List<OrderLine> items) {
        this.id = id;
        this.customerId = customerId;
        this.items = new ArrayList<>(items);
        this.totalAmount = items.stream().map(OrderLine::lineTotal).reduce(BigDecimal.ZERO, BigDecimal::add);
        this.status = OrderStatus.PENDING;
        this.paymentStatus = StepStatus.PENDING;
        this.inventoryStatus = StepStatus.PENDING;
    }

    public static Order place(String customerId, List<OrderLine> items) {
        return new Order(UUID.randomUUID(), customerId, items);
    }

    /**
     * Records the payment outcome. Returns true when this call moved the order to a terminal state.
     * Events that arrive after the order is already terminal are ignored, whatever their order.
     */
    public boolean recordPaymentResult(StepStatus result, String reason) {
        if (isTerminal()) {
            return false;
        }
        this.paymentStatus = result;
        return settle(result, reason);
    }

    public boolean recordInventoryResult(StepStatus result, String reason) {
        if (isTerminal()) {
            return false;
        }
        this.inventoryStatus = result;
        return settle(result, reason);
    }

    private boolean settle(StepStatus latest, String reason) {
        if (latest == StepStatus.FAILED) {
            this.status = OrderStatus.CANCELLED;
            this.cancelReason = reason;
            return true;
        }
        if (paymentStatus == StepStatus.SUCCEEDED && inventoryStatus == StepStatus.SUCCEEDED) {
            this.status = OrderStatus.CONFIRMED;
            return true;
        }
        return false;
    }

    public boolean isTerminal() {
        return status != OrderStatus.PENDING;
    }

    @PrePersist
    void onCreate() {
        createdAt = updatedAt = Instant.now();
    }

    @PreUpdate
    void onUpdate() {
        updatedAt = Instant.now();
    }

    public UUID getId() {
        return id;
    }

    public String getCustomerId() {
        return customerId;
    }

    public List<OrderLine> getItems() {
        return List.copyOf(items);
    }

    public BigDecimal getTotalAmount() {
        return totalAmount;
    }

    public OrderStatus getStatus() {
        return status;
    }

    public StepStatus getPaymentStatus() {
        return paymentStatus;
    }

    public StepStatus getInventoryStatus() {
        return inventoryStatus;
    }

    public String getCancelReason() {
        return cancelReason;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
