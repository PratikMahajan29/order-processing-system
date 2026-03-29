package com.ops.order._processing.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ops.order._processing.entity.FailedEvent;
import com.ops.order._processing.entity.Order;
import com.ops.order._processing.entity.OutboxEvent;
import com.ops.order._processing.event.OrderEvent;
import com.ops.order._processing.repository.FailedEventRepository;
import com.ops.order._processing.repository.OrderRepository;
import com.ops.order._processing.repository.OutboxEventRepository;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
public class DLQReprocessingService {

    private final FailedEventRepository failedEventRepository;
    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

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
            Order order = orderRepository.findByOrderId(event.getOrderId())
                    .orElseThrow(() -> new RuntimeException("Order not found"));

            // State check (CRITICAL)
            if ("COMPLETED".equalsIgnoreCase(order.getStatus())) {

                System.out.println("Skipping reprocess. Order already COMPLETED: " + order.getOrderId());

                failedEvent.setStatus("IGNORED");
                failedEvent.setUpdatedAt(LocalDateTime.now());
                failedEventRepository.save(failedEvent);

                return;
            }

            if (failedEvent.getRetryCount() >= MAX_RETRIES) {

                System.out.println("Max retries reached for event: " + eventId);

                failedEvent.setStatus("DEAD");
                failedEvent.setUpdatedAt(LocalDateTime.now());

                failedEventRepository.save(failedEvent);
                return;
            }

            // Create NEW outbox event (retry)
            OutboxEvent outboxEvent = new OutboxEvent(
                    event.getEventId(),
                    event.getOrderId(),
                    objectMapper.writeValueAsString(event),
                    "NEW"
            );

            outboxEventRepository.save(outboxEvent); // ----> Event saved in outbox

            //  Update failed event status
            failedEvent.setRetryCount(failedEvent.getRetryCount() + 1);
            failedEvent.setStatus("RETRYING");
            failedEvent.setUpdatedAt(LocalDateTime.now());

            failedEventRepository.save(failedEvent);

            System.out.println("Reprocessing scheduled via Outbox for eventId: " + eventId);

        } catch (Exception e) {
            throw new RuntimeException("Reprocessing failed: " + e.getMessage(), e);
        }
    }
}