package com.jamespolk.ordertracking.order.messaging;

import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

public interface OutboxRepository extends JpaRepository<OutboxMessage, UUID> {

    /** Oldest first, and skip rows another relay instance already holds. */
    @Query(value = "SELECT * FROM outbox ORDER BY created_at LIMIT :limit FOR UPDATE SKIP LOCKED", nativeQuery = true)
    List<OutboxMessage> claimBatch(int limit);

    long countByOrderId(UUID orderId);
}
