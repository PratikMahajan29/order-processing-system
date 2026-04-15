package com.ops.order._processing.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ops.order._processing.entity.OutboxEvent;
import com.ops.order._processing.event.OrderEvent;
import com.ops.order._processing.repository.OutboxEventRepository;
import jakarta.transaction.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Service
public class OutboxPublisherService {

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaProducerService kafkaProducerService;
    private final ObjectMapper objectMapper;

    public OutboxPublisherService(OutboxEventRepository outboxEventRepository,
                                  KafkaProducerService kafkaProducerService,
                                  ObjectMapper objectMapper) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaProducerService = kafkaProducerService;
        this.objectMapper = objectMapper;
    }

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisherService.class);

    //  Runs every 5 seconds
    @Scheduled(fixedDelay = 5000)
    @Transactional
    public void publishOutboxEvents() {

        int batchSize = 10;

        //  STEP 1: fetch + lock
        List<OutboxEvent> events =
                outboxEventRepository.fetchAndLockEvents(batchSize);

        if (events.isEmpty()) {
            return;
        }

        for (OutboxEvent outboxEvent : events) {
            try {

                // mark as PROCESSING (optional but good practice)
                outboxEvent.setStatus("PROCESSING");
                outboxEvent.setUpdatedAt(LocalDateTime.now());
                outboxEventRepository.save(outboxEvent);

                OrderEvent event = objectMapper.readValue(
                        outboxEvent.getPayload(),
                        OrderEvent.class
                );

                kafkaProducerService.sendOrderEvent(event);

                log.info("Kafka event sent: {}", event.getEventId());

                //  mark SENT
                outboxEvent.setStatus("SENT");
                outboxEvent.setUpdatedAt(LocalDateTime.now());

                outboxEventRepository.save(outboxEvent);

            } catch (Exception e) {

                log.error("Failed to publish outbox event: {}", outboxEvent.getEventId(), e);

                //  reset for retry
                outboxEvent.setStatus("NEW");
                outboxEvent.setUpdatedAt(LocalDateTime.now());

                outboxEventRepository.save(outboxEvent);
            }
        }
    }
}