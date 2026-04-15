package com.ops.order._processing.service;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class ProcessingRecoveryScheduler {

    private final ProcessingRecoveryService recoveryService;

    public ProcessingRecoveryScheduler(ProcessingRecoveryService recoveryService) {
        this.recoveryService = recoveryService;
    }

    // Runs every 30 seconds
//    @Scheduled(fixedDelay = 30000)
    public void runRecovery() {
        recoveryService.recoverStuckEvents();
    }
}