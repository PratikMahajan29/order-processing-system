package com.ops.order._processing.service;

import com.ops.order._processing.entity.FailedEvent;
import com.ops.order._processing.repository.FailedEventRepository;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

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

    @Scheduled(fixedDelay = 10000) // every 10 seconds
    public void retryFailedEvents() {

        List<FailedEvent> events =
                failedEventRepository.findByStatusAndRetryCountLessThan("FAILED", MAX_RETRIES);

        for (FailedEvent event : events) {
            try {
                System.out.println("Retrying event: " + event.getEventId());

                reprocessingService.reprocessById(event.getEventId());

            } catch (Exception e) {
                System.out.println("Retry failed for event: " + event.getEventId());
            }
        }
    }
}