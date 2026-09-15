package com.jamespolk.ordertracking.events;

/** Events published to {@link Topics#INVENTORY_EVENTS}. */
public sealed interface InventoryEvent extends DomainEvent permits InventoryReserved, InventoryFailed {}
