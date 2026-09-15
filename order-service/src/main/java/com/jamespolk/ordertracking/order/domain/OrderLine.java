package com.jamespolk.ordertracking.order.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import java.math.BigDecimal;

@Embeddable
public record OrderLine(
        @Column(name = "sku", nullable = false) String sku,
        @Column(name = "quantity", nullable = false) int quantity,

        @Column(name = "unit_price", nullable = false, precision = 12, scale = 2)
        BigDecimal unitPrice) {

    public BigDecimal lineTotal() {
        return unitPrice.multiply(BigDecimal.valueOf(quantity));
    }
}
