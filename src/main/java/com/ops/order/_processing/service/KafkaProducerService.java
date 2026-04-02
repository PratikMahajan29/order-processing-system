package com.ops.order._processing.service;

import com.ops.order._processing.event.OrderEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.MDC;

@Service
public class KafkaProducerService {

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    public KafkaProducerService(KafkaTemplate<String, OrderEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendOrderEvent(OrderEvent event) {

        String correlationId = MDC.get("correlationId");

        ProducerRecord<String, OrderEvent> record =
                new ProducerRecord<>("order-topic", event.getEventId(), event);

        if (correlationId != null) {
            record.headers().add("correlationId", correlationId.getBytes());
        }

        kafkaTemplate.send(record);
    }

    public void sendToMainTopic(OrderEvent event) {
        kafkaTemplate.send("order-topic", event.getEventId(), event);
        System.out.println("Reprocessing event sent to main topic: " + event.getEventId());
    }
}