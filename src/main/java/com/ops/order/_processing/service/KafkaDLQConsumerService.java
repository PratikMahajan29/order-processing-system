package com.ops.order._processing.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ops.order._processing.entity.FailedEvent;
import com.ops.order._processing.event.OrderEvent;
import com.ops.order._processing.repository.FailedEventRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
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
            // STEP 1: prevent duplicate DLQ entries early
            if (failedEventRepository.existsByEventId(event.getEventId())) {
                log.warn("Duplicate DLQ event ignored | eventId={}", event.getEventId());
                return;
            }

            // STEP 2: extract failureType FIRST
            Header failureHeader = record.headers().lastHeader("failure-type");

            String failureType = "TRANSIENT"; // default

            if (failureHeader != null) {
                failureType = new String(failureHeader.value(), StandardCharsets.UTF_8);
            } else {
                log.error("Missing failure-type header | eventId={}", event.getEventId());
            }

            // STEP 3: build failed event
            FailedEvent failedEvent = new FailedEvent();
            failedEvent.setEventId(event.getEventId());
            failedEvent.setOrderId(event.getOrderId());
            failedEvent.setPayload(objectMapper.writeValueAsString(event));
            failedEvent.setExceptionType(exceptionType);
            failedEvent.setErrorMessage(errorMessage);
            failedEvent.setRetryCount(0);

            //  STEP 4: smart retry scheduling
            if ("ORDER".equalsIgnoreCase(failureType)) {
                // state-driven → retry immediately
                failedEvent.setNextRetryAt(LocalDateTime.now());
            } else {
                // time-driven retry
                failedEvent.setNextRetryAt(LocalDateTime.now().plusSeconds(10));
            }

            failedEvent.setFailureType(failureType);
            failedEvent.setStatus("FAILED");
            failedEvent.setCreatedAt(LocalDateTime.now());
            failedEvent.setUpdatedAt(LocalDateTime.now());

            // STEP 5: persist
            failedEventRepository.save(failedEvent);

            // structured logging
            log.info("DLQ event stored | eventId={} | type={} | error={}",
                    event.getEventId(), failureType, errorMessage);

        } catch (Exception e) {
            log.error("Failed to persist DLQ event | eventId={} | error={}",
                    event.getEventId(), e.getMessage(), e);
        }
    }

    private String getHeader(ConsumerRecord<String, OrderEvent> record, String key) {
        if (record.headers().lastHeader(key) != null) {
            return new String(record.headers().lastHeader(key).value(), StandardCharsets.UTF_8);
        }
        return "N/A";
    }
}