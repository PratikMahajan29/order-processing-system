package com.ops.order._processing.service;

import com.ops.order._processing.event.OrderEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;

@Service
public class KafkaDLQConsumerService {

    @KafkaListener(topics = "order-topic-dlq", groupId = "order-dlq-group")
    public void consumeDLQ(ConsumerRecord<String, OrderEvent> record) {

        OrderEvent event = record.value();

        String errorMessage = getHeader(record, "error-message");
        String exceptionType = getHeader(record, "exception-type");

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