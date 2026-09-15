package com.jamespolk.ordertracking.order.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import java.math.BigDecimal;
import java.math.RoundingMode;

@Embeddable
public record OrderLine(
        @Column(name = "sku", nullable = false) String sku,
        @Column(name = "quantity", nullable = false) int quantity,

        @Column(name = "unit_price", nullable = false, precision = 12, scale = 2)
        BigDecimal unitPrice) {

    /** Money is always scale 2; the Avro decimal(12,2) encoding rejects anything else. */
    public OrderLine {
        unitPrice = unitPrice.setScale(2, RoundingMode.HALF_UP);
    }

    public BigDecimal lineTotal() {
        return unitPrice.multiply(BigDecimal.valueOf(quantity));
    }
}
