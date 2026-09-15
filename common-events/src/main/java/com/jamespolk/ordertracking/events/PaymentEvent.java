package com.jamespolk.ordertracking.events;

/** Events published to {@link Topics#PAYMENT_EVENTS}. */
public sealed interface PaymentEvent extends DomainEvent permits PaymentSucceeded, PaymentFailed, PaymentRefunded {}
