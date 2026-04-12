package com.ops.order._processing.service;

import com.ops.order._processing.repository.ProcessedEventRepository;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Map;

@Service
public class ProcessedEventService {

    private final JdbcTemplate jdbcTemplate;

    public ProcessedEventService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean tryStartProcessing(String eventId) {

        try {
            // STEP 1: Try to insert (only ONE thread will succeed)
            jdbcTemplate.update("""
            INSERT INTO processed_events(event_id, status, created_at, updated_at)
            VALUES (?, 'PROCESSING', NOW(), NOW())
            """, eventId);

            return true;

        } catch (DuplicateKeyException e) {

            // STEP 2: Already exists → fetch current state
            String status;
            LocalDateTime updatedAt;

            try {
                Map<String, Object> result = jdbcTemplate.queryForMap("""
                SELECT status, updated_at
                FROM processed_events
                WHERE event_id = ?
            """, eventId);

                status = (String) result.get("status");
                updatedAt = ((Timestamp) result.get("updated_at")).toLocalDateTime();

            } catch (EmptyResultDataAccessException ex) {
                throw new IllegalStateException("CRITICAL: Event exists but not found: " + eventId);
            }

            // STEP 3: Already completed → skip
            if ("COMPLETED".equalsIgnoreCase(status)) {
                return false;
            }

            // STEP 4: Still processing → check if stuck
            if ("PROCESSING".equalsIgnoreCase(status)) {

                LocalDateTime timeout = LocalDateTime.now().minusMinutes(5);

                if (updatedAt.isBefore(timeout)) {

                    //  TAKEOVER (only ONE thread will succeed)
                    int updated = jdbcTemplate.update("""
                    UPDATE processed_events
                    SET updated_at = NOW()
                    WHERE event_id = ? AND status = 'PROCESSING'
                    """, eventId);

                    return updated > 0;
                }

                // Another thread is actively processing
                return false;
            }

            // STEP 5: Unknown state → system corruption
            throw new IllegalStateException("Unknown status: " + status + " for eventId: " + eventId);
        }
    }

    @Transactional
    public void markCompleted(String eventId) {

        int updated = jdbcTemplate.update("""
        UPDATE processed_events
        SET status = 'COMPLETED', updated_at = NOW()
        WHERE event_id = ? AND status = 'PROCESSING'
        """, eventId);

        if (updated == 0) {
            throw new IllegalStateException(
                    "Failed to mark completed — invalid state for eventId: " + eventId
            );
        }
    }
    }
