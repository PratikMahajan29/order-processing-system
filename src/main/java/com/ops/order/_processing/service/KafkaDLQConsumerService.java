package com.ops.order._processing.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ops.order._processing.entity.FailedEvent;
import com.ops.order._processing.event.OrderEvent;
import com.ops.order._processing.repository.FailedEventRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;

@Service
public class KafkaDLQConsumerService {

    private final FailedEventRepository failedEventRepository;
    private final ObjectMapper objectMapper;

    public KafkaDLQConsumerService(FailedEventRepository failedEventRepository, ObjectMapper objectMapper) {
        this.failedEventRepository = failedEventRepository;
        this.objectMapper = objectMapper;
    }

    private static final Logger log = LoggerFactory.getLogger(KafkaDLQConsumerService.class);

    @KafkaListener(topics = "order-topic-dlq", groupId = "order-dlq-group")
    public void consumeDLQ(ConsumerRecord<String, OrderEvent> record) {

        OrderEvent event = record.value();

        String errorMessage = getHeader(record, "error-message");
        String exceptionType = getHeader(record, "exception-type");

        try {
            FailedEvent failedEvent = new FailedEvent();
            failedEvent.setEventId(event.getEventId());
            failedEvent.setPayload(objectMapper.writeValueAsString(event));
            failedEvent.setExceptionType(exceptionType);
            failedEvent.setErrorMessage(errorMessage);
            failedEvent.setRetryCount(0);
            failedEvent.setNextRetryAt(LocalDateTime.now().plusSeconds(10)); // Schedule next retry after 10 secs
            failedEvent.setStatus("FAILED");
            failedEvent.setCreatedAt(LocalDateTime.now());
            failedEvent.setUpdatedAt(LocalDateTime.now());

            failedEventRepository.save(failedEvent);

        } catch (Exception e) {
            log.info("Failed to persist DLQ event: " + e.getMessage());
        }

        System.out.println("========== DLQ MESSAGE RECEIVED ==========");
        System.out.println("Event ID       : " + event.getEventId());
        System.out.println("Product        : " + event.getProductName());
        System.out.println("Quantity       : " + event.getQuantity());
        System.out.println("Status         : " + event.getStatus());
        System.out.println("Exception Type : " + exceptionType);
        System.out.println("Error Message  : " + errorMessage);
        System.out.println("==========================================");
    }

    private String getHeader(ConsumerRecord<String, OrderEvent> record, String key) {
        if (record.headers().lastHeader(key) != null) {
            return new String(record.headers().lastHeader(key).value(), StandardCharsets.UTF_8);
        }
        return "N/A";
    }
}