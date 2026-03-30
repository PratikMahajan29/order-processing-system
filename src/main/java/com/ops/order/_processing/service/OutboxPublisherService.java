package com.ops.order._processing.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ops.order._processing.entity.OutboxEvent;
import com.ops.order._processing.event.OrderEvent;
import com.ops.order._processing.repository.OutboxEventRepository;
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

    //  Runs every 5 seconds
    @Scheduled(fixedDelay = 5000)
    public void publishOutboxEvents() {

        List<OutboxEvent> events = outboxEventRepository.findByStatus("NEW");

        for (OutboxEvent outboxEvent : events) {
            try {

                //  Convert payload back to event
                OrderEvent event = objectMapper.readValue(
                        outboxEvent.getPayload(),
                        OrderEvent.class
                );

                // Send to Kafka
                kafkaProducerService.sendOrderEvent(event);

                System.out.print("Kafka event sent");

                //  Mark as SENT
                outboxEvent.setStatus("SENT");
                outboxEvent.setUpdatedAt(LocalDateTime.now());

                outboxEventRepository.save(outboxEvent);

                System.out.println("Outbox event sent: " + outboxEvent.getEventId());

            } catch (Exception e) {

                System.out.println("Failed to publish outbox event: "
                        + outboxEvent.getEventId() + " due to: " + e.getMessage());

                // do NOT mark as SENT
                // it will retry automatically
            }
        }
    }
}