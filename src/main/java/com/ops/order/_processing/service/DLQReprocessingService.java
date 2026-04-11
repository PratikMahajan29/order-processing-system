package com.ops.order._processing.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ops.order._processing.entity.FailedEvent;
import com.ops.order._processing.entity.Order;
import com.ops.order._processing.entity.OutboxEvent;
import com.ops.order._processing.enums.OrderStatus;
import com.ops.order._processing.event.OrderEvent;
import com.ops.order._processing.repository.FailedEventRepository;
import com.ops.order._processing.repository.OrderRepository;
import com.ops.order._processing.repository.OutboxEventRepository;
import com.ops.order._processing.statemachine.OrderStateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class DLQReprocessingService {

    private final FailedEventRepository failedEventRepository;
    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;
    private static final Logger log = LoggerFactory.getLogger(DLQReprocessingService.class);
    private static final int MAX_RETRIES = 3;

    public DLQReprocessingService(FailedEventRepository failedEventRepository,
                                  OrderRepository orderRepository,
                                  OutboxEventRepository outboxEventRepository,
                                  ObjectMapper objectMapper) {
        this.failedEventRepository = failedEventRepository;
        this.orderRepository = orderRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    public void reprocessById(String eventId) {

        //  Fetch failed event
        FailedEvent failedEvent = failedEventRepository.findById(eventId)
                .orElseThrow(() -> new RuntimeException("Event not found in DLQ DB"));

        try {
            // Deserialize event
            OrderEvent event = objectMapper.readValue(
                    failedEvent.getPayload(),
                    OrderEvent.class
            );

            // Fetch order
            Optional<Order> orderOpt = orderRepository.findByOrderId(event.getOrderId());

            if (orderOpt.isEmpty()) {

                log.info("Order not found for event: " + eventId);

                handleRetryUpdate(failedEvent);
                return;
            }

            Order order = orderOpt.get();


            //  ORDER ALREADY COMPLETED

            if ("COMPLETED".equalsIgnoreCase(order.getStatus())) {
                log.info("Skipping reprocess. Order already COMPLETED: " + order.getOrderId());

                failedEvent.setStatus("IGNORED");
                failedEvent.setUpdatedAt(LocalDateTime.now());

                failedEventRepository.save(failedEvent);
                return;
            }


            //MAX RETRIES ALREADY REACHED

            if (failedEvent.getRetryCount() >= MAX_RETRIES) {

                log.info("Max retries reached for event: " + eventId);

                failedEvent.setStatus("DEAD");
                failedEvent.setUpdatedAt(LocalDateTime.now());

                failedEventRepository.save(failedEvent);
                return;
            }

            OrderStatus currentState = OrderStatus.valueOf(order.getStatus());
            OrderStatus eventState = OrderStatus.valueOf(event.getEventType());

            int currentOrder = currentState.getOrder();
            int incomingOrder = eventState.getOrder();

            String failureType = failedEvent.getFailureType();

// 🔥 HANDLE ORDER-BASED EVENTS
            if ("ORDER".equalsIgnoreCase(failureType)) {

                if (incomingOrder != currentOrder + 1) {
                    log.info("ORDER event not ready yet, waiting | orderId={} | current={} | incoming={}",
                            order.getOrderId(), currentState, eventState);

                    // DO NOTHING — just wait for next cycle
                    return;
                }
            }

            //  HANDLE TRANSIENT FAILURES
            else if ("TRANSIENT".equalsIgnoreCase(failureType)) {

                if (failedEvent.getRetryCount() >= MAX_RETRIES) {
                    log.info("Max retries reached (TRANSIENT) | eventId={}", event.getEventId());

                    failedEvent.setStatus("DEAD");
                    failedEvent.setUpdatedAt(LocalDateTime.now());
                    failedEventRepository.save(failedEvent);
                    return;
                }
            }

            //  HANDLE POISON EVENTS
            else if ("POISON".equalsIgnoreCase(failureType)) {

                log.info("Poison event — skipping permanently | eventId={}", event.getEventId());

                failedEvent.setStatus("DEAD");
                failedEvent.setUpdatedAt(LocalDateTime.now());
                failedEventRepository.save(failedEvent);
                return;
            }

            //  BUSINESS VALIDATION
            if (!OrderStateMachine.isValidTransition(currentState, eventState)) {
                log.error("Invalid transition during retry | orderId={} | current={} | incoming={}",
                        order.getOrderId(), currentState, eventState);

                failedEvent.setStatus("DEAD");
                failedEvent.setUpdatedAt(LocalDateTime.now());
                failedEventRepository.save(failedEvent);
                return;
            }

            Optional<OutboxEvent> existing = outboxEventRepository.findByEventId(event.getEventId());

            if (existing.isPresent()) {
                log.info("Event already reprocessed, skipping: {}", event.getEventId());
                return;
            }

            //  VALID RETRY → PUSH TO OUTBOX
            OutboxEvent outboxEvent = new OutboxEvent(
                    event.getEventId(),
                    event.getOrderId(),
                    objectMapper.writeValueAsString(event),
                    "NEW"
            );

            outboxEventRepository.save(outboxEvent);
            log.info("Reprocessing scheduled via Outbox for eventId: " + eventId);

            //  Update retry state
            failedEvent.setStatus("REPROCESSED");
            failedEvent.setUpdatedAt(LocalDateTime.now());
            failedEventRepository.save(failedEvent);

        } catch (Exception e) {
            throw new RuntimeException("Reprocessing failed: " + e.getMessage(), e);
        }
    }

    //  CENTRALIZED RETRY LOGIC (NO DUPLICATION)

    private void handleRetryUpdate(FailedEvent failedEvent) {

        int newRetryCount = failedEvent.getRetryCount() + 1;

        //  ALWAYS update retry count
        failedEvent.setRetryCount(newRetryCount);

        if (newRetryCount >= MAX_RETRIES) {

            failedEvent.setStatus("DEAD");

        } else {

            failedEvent.setStatus("FAILED");
            failedEvent.setNextRetryAt(calculateNextRetryTime(newRetryCount));
        }

        failedEvent.setUpdatedAt(LocalDateTime.now());
        failedEventRepository.save(failedEvent);
    }


    //  EXPONENTIAL BACKOFF + JITTER

    private LocalDateTime calculateNextRetryTime(int retryCount) {

        int baseDelaySeconds = 10;

        long exponentialDelay = (long) (baseDelaySeconds * Math.pow(2, retryCount));

        //  Thread-safe jitter (0 → 50% of delay)
        long jitter = ThreadLocalRandom.current()
                .nextLong(0, Math.max(1, exponentialDelay / 2));

        long finalDelay = exponentialDelay + jitter;

        return LocalDateTime.now().plusSeconds(finalDelay);
    }
}