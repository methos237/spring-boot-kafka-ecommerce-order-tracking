package com.jamespolk.ordertracking.order.domain;

import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.web.ErrorResponseException;

/** Rendered as an RFC 9457 problem detail by the framework. */
public class OrderNotFoundException extends ErrorResponseException {

    public OrderNotFoundException(UUID orderId) {
        super(HttpStatus.NOT_FOUND);
        setTitle("Order not found");
        setDetail("No order with id " + orderId);
    }
}
