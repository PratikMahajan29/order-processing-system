package com.ops.order._processing.repository;

import com.ops.order._processing.entity.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Optional;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent,Long> {

    List<OutboxEvent> findByStatus(String status);

    Optional<OutboxEvent> findByEventId(String eventId);

    @Modifying
    @Query(value = """
    SELECT * FROM outbox_events
    WHERE status = 'NEW'
    ORDER BY created_at ASC
    LIMIT :limit
    FOR UPDATE SKIP LOCKED
""", nativeQuery = true)
    List<OutboxEvent> fetchAndLockEvents(@Param("limit") int limit);
}
