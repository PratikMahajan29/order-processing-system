package com.ops.order._processing.service;

import com.ops.order._processing.entity.OutboxEvent;
import com.ops.order._processing.entity.ProcessedEvent;
import com.ops.order._processing.exception.NonRetryableException;
import com.ops.order._processing.repository.OutboxEventRepository;
import com.ops.order._processing.repository.ProcessedEventRepository;
import jakarta.transaction.Transactional;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Service
public class ProcessingRecoveryService {

    private final ProcessedEventRepository processedRepo;
    private final OutboxEventRepository outboxRepo;

    public ProcessingRecoveryService(ProcessedEventRepository processedRepo,
                                     OutboxEventRepository outboxRepo) {
        this.processedRepo = processedRepo;
        this.outboxRepo = outboxRepo;
    }

    @Transactional
    public void recoverStuckEvents() {

        LocalDateTime cutoff = LocalDateTime.now().minusMinutes(5);

        List<ProcessedEvent> stuckEvents =
                processedRepo.findStuckEvents(cutoff, 50);

        for (ProcessedEvent event : stuckEvents) {

            String eventId = event.getEventId();

            System.out.println("Recovering stuck event: " + eventId +
                    " lastUpdated: " + event.getUpdatedAt());

            OutboxEvent outbox = outboxRepo.findByEventId(eventId)
                    .orElseThrow(() -> new NonRetryableException(
                            "CRITICAL: Outbox event missing for eventId: " + eventId));

            // Reset ONLY if needed
            if (!"NEW".equalsIgnoreCase(outbox.getStatus())) {
                outbox.setStatus("NEW");
                outbox.setUpdatedAt(LocalDateTime.now());
                outboxRepo.save(outbox);
            }

            // prevent immediate reprocessing loop
            event.setUpdatedAt(LocalDateTime.now());
            processedRepo.save(event);
        }
    }
}
