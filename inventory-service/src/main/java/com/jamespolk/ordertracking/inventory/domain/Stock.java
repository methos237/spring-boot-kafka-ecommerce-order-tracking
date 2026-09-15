package com.jamespolk.ordertracking.inventory.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "stock")
public class Stock {

    @Id
    @Column(name = "sku", nullable = false)
    private String sku;

    @Column(name = "available", nullable = false)
    private int available;

    protected Stock() {}

    public String getSku() {
        return sku;
    }

    public int getAvailable() {
        return available;
    }

    public boolean canReserve(int quantity) {
        return available >= quantity;
    }

    public void reserve(int quantity) {
        if (!canReserve(quantity)) {
            throw new IllegalStateException("insufficient stock for " + sku);
        }
        available -= quantity;
    }
}
