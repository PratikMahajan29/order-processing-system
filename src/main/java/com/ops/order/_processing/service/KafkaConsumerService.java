package com.ops.order._processing.service;

import com.ops.order._processing.entity.Order;
import com.ops.order._processing.event.OrderEvent;
import com.ops.order._processing.exception.RetryableException;
import com.ops.order._processing.repository.OrderRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
public class KafkaConsumerService {

    private final OrderRepository orderRepository;
    private final ProcessedEventService processedEventService;

    public KafkaConsumerService(OrderRepository orderRepository,ProcessedEventService processedEventService) {
        this.orderRepository = orderRepository;
        this.processedEventService = processedEventService;
    }

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerService.class);

    @KafkaListener(topics = "order-topic", groupId = "order-group")
    public void consume(ConsumerRecord<String, OrderEvent> record, Acknowledgment ack) {

        String correlationId = getHeader(record, "correlationId");
        MDC.put("correlationId", correlationId);

        OrderEvent event = record.value();

        try {
            log.info("START processing eventId={}, orderId={}",
                    event.getEventId(), event.getOrderId());

            boolean shouldProcess = processedEventService.tryStartProcessing(event.getEventId());

            if (!shouldProcess) {
                log.info("SKIP duplicate eventId={}", event.getEventId());
                ack.acknowledge();
                return;
            }

            Order order = orderRepository.findByOrderId(event.getOrderId())
                    .orElseThrow(() -> new RetryableException("Order not found"));

            order.setStatus("PROCESSING");
            order.setUpdatedAt(LocalDateTime.now());
            orderRepository.save(order);

            //  Business validation
            if (order.getQuantity() <= 0) {

                order.setStatus("FAILED");
                order.setFailureReason("Invalid quantity");
                order.setUpdatedAt(LocalDateTime.now());
                orderRepository.save(order);

                processedEventService.markCompleted(event.getEventId());

                log.warn("FAILED business validation eventId={}, orderId={}",
                        event.getEventId(), event.getOrderId());

                ack.acknowledge();
                return;
            }

            //  Success
            order.setStatus("COMPLETED");
            order.setUpdatedAt(LocalDateTime.now());
            orderRepository.save(order);

            processedEventService.markCompleted(event.getEventId());

            log.info("SUCCESS eventId={}, orderId={}",
                    event.getEventId(), event.getOrderId());

            ack.acknowledge();

        } catch (Exception e) {

            log.error("FAIL eventId={}, error={}",
                    event.getEventId(), e.getMessage(), e);

            throw new RetryableException(e.getMessage());

        } finally {
            MDC.clear();
        }
    }

    private String getHeader(ConsumerRecord<String, OrderEvent> record, String key) {
        if (record.headers().lastHeader(key) != null) {
            return new String(record.headers().lastHeader(key).value());
        }
        return "N/A";
    }
}
