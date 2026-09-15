package com.jamespolk.ordertracking.order.domain;

/** Outcome of one saga participant (payment or inventory) for an order. */
public enum StepStatus {
    PENDING,
    SUCCEEDED,
    FAILED
}
