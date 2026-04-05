package com.ops.order._processing.service;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class MetricsScheduler {

    private final MetricsService metricsService;

    public MetricsScheduler(MetricsService metricsService) {
        this.metricsService = metricsService;
    }

    @Scheduled(fixedDelay = 60000) // every 1 minute
    public void printMetrics() {
        metricsService.logMetrics();
    }
}