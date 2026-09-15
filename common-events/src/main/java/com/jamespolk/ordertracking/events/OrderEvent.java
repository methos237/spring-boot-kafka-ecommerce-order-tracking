package com.jamespolk.ordertracking.events;

/** Events published to {@link Topics#ORDER_EVENTS}. */
public sealed interface OrderEvent extends DomainEvent permits OrderPlaced, OrderConfirmed, OrderCancelled {}
