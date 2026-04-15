package com.ops.order._processing.repository;


import com.ops.order._processing.entity.ProcessedEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, String> {

    @Modifying
    @Query(value = "INSERT INTO processed_events (event_id, processed_at) VALUES (:eventId, NOW())", nativeQuery = true)
    void insertEvent(@Param("eventId") String eventId);

    @Query(value = """
    SELECT * FROM processed_events
    WHERE status = 'PROCESSING'
    AND updated_at < :cutoffTime
    ORDER BY updated_at ASC
    LIMIT :limit
""", nativeQuery = true)
    List<ProcessedEvent> findStuckEvents(
            @Param("cutoffTime") LocalDateTime cutoffTime,
            @Param("limit") int limit
    );

    boolean existsByEventId(String eventId);

    boolean existsByEventIdAndStatus(String eventId, String status);
}
