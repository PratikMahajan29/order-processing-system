package com.ops.order._processing.service;

import com.ops.order._processing.entity.FailedEvent;
import com.ops.order._processing.repository.FailedEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Service
public class FailedEventRetryScheduler {

    private static final int MAX_RETRIES = 3;

    private final FailedEventRepository failedEventRepository;
    private final DLQReprocessingService reprocessingService;

    public FailedEventRetryScheduler(FailedEventRepository failedEventRepository,
                                     DLQReprocessingService reprocessingService) {
        this.failedEventRepository = failedEventRepository;
        this.reprocessingService = reprocessingService;
    }

    private static final Logger log = LoggerFactory.getLogger(FailedEventRetryScheduler.class);


    @Scheduled(fixedDelay = 10000) // every 10 seconds
    public void retryFailedEvents() {

        List<FailedEvent> events =
                failedEventRepository.findByStatusAndRetryCountLessThanAndNextRetryAtBefore("FAILED",
                        MAX_RETRIES, LocalDateTime.now());

        for (FailedEvent event : events) {
            try {
                log.info("Retrying event: " + event.getEventId());

                reprocessingService.reprocessById(event.getEventId());

            } catch (Exception e) {
                log.info("Retry failed for event: " + event.getEventId() + ", error: " + e.getMessage());
            }
        }
    }
}