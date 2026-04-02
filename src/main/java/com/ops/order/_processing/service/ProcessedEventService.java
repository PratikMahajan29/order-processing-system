package com.ops.order._processing.service;

import com.ops.order._processing.repository.ProcessedEventRepository;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;

@Service
public class ProcessedEventService {

    private final JdbcTemplate jdbcTemplate;

    public ProcessedEventService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean tryStartProcessing(String eventId) {
        try {
            jdbcTemplate.update("""
            INSERT INTO processed_events(event_id, status, created_at, updated_at)
            VALUES (?, 'PROCESSING', NOW(), NOW())
        """, eventId);
            return true;

        } catch (DuplicateKeyException e) {

            String status = jdbcTemplate.query("""
            SELECT status FROM processed_events WHERE event_id = ?
        """, rs -> {
                if (!rs.next()) {
                    throw new IllegalStateException("Event exists but not found during select: " + eventId);
                }
                return rs.getString("status");
            }, eventId);

            //  HARD VALIDATION (no silent failure)
            if (status == null) {
                throw new IllegalStateException("Invalid state: status is NULL for eventId: " + eventId);
            }

            if ("COMPLETED".equalsIgnoreCase(status)) {
                return false; // skip safely
            }

            if ("PROCESSING".equalsIgnoreCase(status)) {
                return true; // retry allowed
            }

            //  Unknown state = system corruption
            throw new IllegalStateException("Unknown status: " + status + " for eventId: " + eventId);
        }
    }

    @Transactional
    public void markCompleted(String eventId) {
        jdbcTemplate.update("""
            UPDATE processed_events
            SET status = 'COMPLETED', updated_at = NOW()
            WHERE event_id = ?
        """, eventId);
    }
}
