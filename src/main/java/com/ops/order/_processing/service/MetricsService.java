package com.ops.order._processing.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicInteger;

@Service
public class MetricsService {

    private final AtomicInteger successCount = new AtomicInteger();
    private final AtomicInteger failureCount = new AtomicInteger();
    private final AtomicInteger retryCount = new AtomicInteger();
    private final AtomicInteger duplicateCount = new AtomicInteger();
    private final AtomicInteger incrementTotal = new AtomicInteger();

    private static final Logger log = LoggerFactory.getLogger(MetricsService.class);

    public void incrementSuccess() {
        successCount.incrementAndGet();
    }

    public void incrementFailure() {
        failureCount.incrementAndGet();
    }

    public void incrementRetry() {
        retryCount.incrementAndGet();
    }

    public void incrementDuplicate() {
        duplicateCount.incrementAndGet();
    }

    public void IncrementTotal() {
        incrementTotal.incrementAndGet();
    }

    public void logMetrics() {
        log.info("METRICS success={}, failure={}, retry={}, duplicate={} , total={}",
                successCount.get(),
                failureCount.get(),
                retryCount.get(),
                duplicateCount.get(),
                incrementTotal.get());

    }


}