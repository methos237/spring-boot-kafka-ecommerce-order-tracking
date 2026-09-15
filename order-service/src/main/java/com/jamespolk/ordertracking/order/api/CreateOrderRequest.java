package com.jamespolk.ordertracking.order.api;

import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.math.BigDecimal;
import java.util.List;

public record CreateOrderRequest(
        @NotBlank String customerId, @NotEmpty List<@Valid Item> items) {

    public record Item(
            @NotBlank String sku,
            @Positive int quantity,
            @NotNull @DecimalMin("0.01") BigDecimal unitPrice) {}
}
