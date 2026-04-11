package com.ops.order._processing.service;

import com.ops.order._processing.entity.Order;
import com.ops.order._processing.event.OrderEvent;
import com.ops.order._processing.exception.NonRetryableException;
import com.ops.order._processing.exception.RetryableException;
import com.ops.order._processing.repository.OrderRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import com.ops.order._processing.enums.OrderStatus;
import com.ops.order._processing.statemachine.OrderStateMachine;
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

        String correlationId = getHeader(record);
        MDC.put("correlationId", correlationId);

        OrderEvent event = record.value();

        log.info("START | orderId={} | eventId={} | partition={} | thread={} | time={}",
                event.getOrderId(),
                event.getEventId(),
                record.partition(),
                thread,
                startTime);

        try {
            metricsService.IncrementTotal();

            boolean shouldProcess = processedEventService.tryStartProcessing(event.getEventId());

            if (!shouldProcess) {
                log.info("SKIP duplicate | eventId={} | thread={}", event.getEventId(), thread);
                metricsService.incrementDuplicate();
                ack.acknowledge();
                return;
            }


            // STEP 1: FETCH ORDER
            Order order = orderRepository.findByOrderId(event.getOrderId())
                    .orElseThrow(() -> new NonRetryableException("Order not found"));


            // STEP 2: SEQUENCE VALIDATION
            Long nextSequence = order.getEventSequence() + 1;

            // STEP 3: STATE VALIDATION
            OrderStatus currentState = OrderStatus.valueOf(order.getStatus());
            OrderStatus nextState = OrderStatus.valueOf(event.getEventType());

            int currentOrder = currentState.getOrder();
            int incomingOrder = nextState.getOrder();

            // CASE 1: DUPLICATE / OLD
            if (incomingOrder <= currentOrder) {
                log.warn("Duplicate/old event ignored | orderId={} | current={} | incoming={}",
                        order.getOrderId(), currentState, nextState);

                metricsService.incrementDuplicate();
                ack.acknowledge();
                return;
            }

            // CASE 2: OUT-OF-ORDER
            if (incomingOrder > currentOrder + 1) {

                log.warn("Out-of-order event, sending to ORDER BUFFER | orderId={} | current={} | incoming={}",
                        order.getOrderId(), currentState, nextState);

                // Let it go to DLQ as ORDER type
                throw new RetryableException("ORDER_VIOLATION");
            }

            // CASE 3: BUSINESS VALIDATION
            if (!OrderStateMachine.isValidTransition(currentState, nextState)) {
                throw new NonRetryableException(
                        "Invalid business transition: " + currentState + " → " + nextState
                );
            }

            order.setStatus(nextState.name());
            order.setEventSequence(nextSequence);
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
                throw e; // Let it go to DLQ directly (no Kafka retry)
            }

            //  NON-RETRYABLE → MARK ORDER AS FAILED
            try {
                Order order = orderRepository.findByOrderId(event.getOrderId()).orElse(null);

                if (order != null) {
                    order.setStatus(OrderStatus.FAILED.name());
                    order.setFailureReason(e.getMessage());
                    order.setUpdatedAt(LocalDateTime.now());

                    orderRepository.save(order);
                }

            } catch (Exception dbEx) {
                log.error("Failed to update order as FAILED | orderId={}", event.getOrderId(), dbEx);
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

    private String getHeader(ConsumerRecord<String, OrderEvent> record) {
        if (record.headers().lastHeader("correlationId") != null) {
            return new String(record.headers().lastHeader("correlationId").value());
        }
        return "N/A";
    }
}