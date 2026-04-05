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
    private final MetricsService metricsService;

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerService.class);

    public KafkaConsumerService(OrderRepository orderRepository,
                                ProcessedEventService processedEventService,
                                MetricsService metricsService) {
        this.orderRepository = orderRepository;
        this.processedEventService = processedEventService;
        this.metricsService = metricsService;
    }

    @KafkaListener(topics = "order-topic", groupId = "order-group")
    public void consume(ConsumerRecord<String, OrderEvent> record, Acknowledgment ack) {

        String thread = Thread.currentThread().getName();
        long startTime = System.currentTimeMillis();

        String correlationId = getHeader(record, "correlationId");
        MDC.put("correlationId", correlationId);

        OrderEvent event = record.value();

        log.info("START | orderId={} | eventId={} | partition={} | thread={} | time={}",
                event.getOrderId(),
                event.getEventId(),
                record.partition(),
                thread,
                startTime);

        try {
            // Simulate processing delay
            Thread.sleep(2000);

            metricsService.IncrementTotal();

            boolean shouldProcess = processedEventService.tryStartProcessing(event.getEventId());

            if (!shouldProcess) {
                log.info("SKIP duplicate | eventId={} | thread={}", event.getEventId(), thread);
                metricsService.incrementDuplicate();
                ack.acknowledge();
                return;
            }

            Order order = orderRepository.findByOrderId(event.getOrderId())
                    .orElseThrow(() -> new RetryableException("Order not found"));

            order.setStatus("PROCESSING");
            order.setUpdatedAt(LocalDateTime.now());
            orderRepository.save(order);

            if (order.getQuantity() <= 0) {

                order.setStatus("FAILED");
                order.setFailureReason("Invalid quantity");
                order.setUpdatedAt(LocalDateTime.now());
                orderRepository.save(order);

                processedEventService.markCompleted(event.getEventId());
                metricsService.incrementFailure();

                log.warn("FAILED validation | eventId={} | orderId={} | thread={}",
                        event.getEventId(), event.getOrderId(), thread);

                ack.acknowledge();
                return;
            }

            order.setStatus("COMPLETED");
            order.setUpdatedAt(LocalDateTime.now());
            orderRepository.save(order);

            processedEventService.markCompleted(event.getEventId());
            metricsService.incrementSuccess();

            log.info("SUCCESS | eventId={} | orderId={} | thread={}",
                    event.getEventId(), event.getOrderId(), thread);

            ack.acknowledge();

        } catch (Exception e) {

            log.error("ERROR | eventId={} | thread={} | error={}",
                    event.getEventId(), thread, e.getMessage(), e);

            metricsService.incrementFailure();

            if (isRetryable(e)) {
                metricsService.incrementRetry();
                throw (RetryableException) e;
            }

            processedEventService.markCompleted(event.getEventId());
            ack.acknowledge();

        } finally {
            long endTime = System.currentTimeMillis();

            log.info("END | orderId={} | eventId={} | thread={} | duration={}ms | endTime={}",
                    event.getOrderId(),
                    event.getEventId(),
                    thread,
                    (endTime - startTime),
                    endTime);

            MDC.clear();
        }
    }

    private boolean isRetryable(Exception e) {
        return e instanceof RetryableException;
    }

    private String getHeader(ConsumerRecord<String, OrderEvent> record, String key) {
        if (record.headers().lastHeader(key) != null) {
            return new String(record.headers().lastHeader(key).value());
        }
        return "N/A";
    }
}