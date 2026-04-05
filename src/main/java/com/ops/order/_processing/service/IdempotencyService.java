package com.ops.order._processing.service;

import com.ops.order._processing.entity.IdempotencyKey;
import com.ops.order._processing.repository.IdempotencyRepository;
import jakarta.transaction.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Optional;

@Service
public class IdempotencyService {

    private final IdempotencyRepository repository;

    public IdempotencyService(IdempotencyRepository repository) {
        this.repository = repository;
    }

    private static final Logger log = LoggerFactory.getLogger(IdempotencyService.class);

    @Transactional
    public IdempotencyKey process(String key, String requestHash) {

        // Step 1: Try to fetch first (fast path)
        Optional<IdempotencyKey> existingOpt = repository.findByIdempotencyKey(key);

        if (existingOpt.isPresent()) {
            IdempotencyKey existing = existingOpt.get();

            validateExisting(existing, requestHash);
            return existing;
        }

        // Step 2: Try insert (first-time request)
        try {
            IdempotencyKey entity = new IdempotencyKey();
            entity.setIdempotencyKey(key);
            entity.setRequestHash(requestHash);
            entity.setStatus("PROCESSING");
            entity.setLockedUntil(LocalDateTime.now().plusMinutes(5));
            entity.setCreatedAt(LocalDateTime.now());
            entity.setUpdatedAt(LocalDateTime.now());

            return repository.save(entity);

        } catch (Exception e) {

            //  Another thread inserted → fetch again
            IdempotencyKey existing = repository.findByIdempotencyKey(key)
                    .orElseThrow(() -> new RuntimeException("Idempotency key missing after conflict"));

            validateExisting(existing, requestHash);
            return existing;
        }
    }

    private void validateExisting(IdempotencyKey existing, String requestHash) {

        //  Payload mismatch
        if (!existing.getRequestHash().equals(requestHash)) {
            throw new RuntimeException("Idempotency key reused with different payload");
        }

        //  Already completed
        if ("COMPLETED".equals(existing.getStatus())) {
            System.out.print("Request already completed, returning same response");
            return;
        }

        // ⚠ Still processing
        if ("PROCESSING".equals(existing.getStatus())
                && existing.getLockedUntil().isAfter(LocalDateTime.now())) {
            throw new RuntimeException("Request already in progress");
        }
    }

    public void markCompleted(String key, String response) {
        IdempotencyKey entity = repository.findByIdempotencyKey(key).orElseThrow();

        entity.setStatus("COMPLETED");
        entity.setResponsePayload(response);
        entity.setUpdatedAt(LocalDateTime.now());

        repository.save(entity);
    }
}
