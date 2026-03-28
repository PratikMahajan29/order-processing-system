package com.ops.order._processing.service;

import com.ops.order._processing.entity.FailedEvent;
import com.ops.order._processing.event.OrderEvent;
import com.ops.order._processing.repository.FailedEventRepository;
import org.springframework.stereotype.Service;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;

@Service
public class DLQReprocessingService {

    private final KafkaProducerService producerService;
    private final FailedEventRepository failedEventRepository;
    private final ObjectMapper objectMapper;

    public DLQReprocessingService(KafkaProducerService producerService,
                                  FailedEventRepository failedEventRepository,
                                  ObjectMapper objectMapper) {
        this.producerService = producerService;
        this.failedEventRepository = failedEventRepository;
        this.objectMapper = objectMapper;
    }

    public void reprocessById(String eventId) {

        FailedEvent failedEvent = failedEventRepository.findById(eventId)
                .orElseThrow(() -> new RuntimeException("Event not found in DLQ DB"));

        try {
            OrderEvent event = objectMapper.readValue(failedEvent.getPayload(), OrderEvent.class);

            producerService.sendToMainTopic(event);

            failedEvent.setStatus("REPROCESSED");
            failedEvent.setUpdatedAt(LocalDateTime.now());

            failedEventRepository.save(failedEvent);

            System.out.println("Reprocessed event: " + eventId);

        } catch (Exception e) {
            throw new RuntimeException("Reprocessing failed: " + e.getMessage());
        }
    }
}