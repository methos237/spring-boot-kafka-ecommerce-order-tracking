package com.jamespolk.ordertracking.inventory.domain;

import jakarta.persistence.LockModeType;
import java.util.Collection;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;

public interface StockRepository extends JpaRepository<Stock, String> {

    /** Row locks so two orders for the same sku on different partitions cannot both pass the check. */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    List<Stock> findAllBySkuIn(Collection<String> skus);
}
