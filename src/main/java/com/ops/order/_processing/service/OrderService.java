package com.ops.order._processing.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ops.order._processing.dto.OrderRequestDTO;
import com.ops.order._processing.entity.Order;
import com.ops.order._processing.entity.OutboxEvent;
import com.ops.order._processing.event.OrderEvent;
import com.ops.order._processing.repository.OrderRepository;
import com.ops.order._processing.repository.OutboxEventRepository;
import jakarta.transaction.Transactional;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
public class OrderService {

    private final KafkaProducerService producerService;
    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;


    public OrderService(KafkaProducerService producerService, OrderRepository orderRepository, OutboxEventRepository outboxEventRepository, ObjectMapper objectMapper) {
        this.producerService = producerService;
        this.orderRepository = orderRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public Order createOrder(OrderRequestDTO dto) {

        // Basic validation (ONLY structural)
        if (dto.getQuantity() == null) {
            throw new IllegalArgumentException("Quantity is required");
        }

        String orderId = java.util.UUID.randomUUID().toString();
        String eventId = java.util.UUID.randomUUID().toString();

        // Save order
        Order order = new Order();
        order.setOrderId(orderId);
        order.setEventId(eventId);
        order.setProductName(dto.getProductName());
        order.setQuantity(dto.getQuantity());
        order.setStatus("CREATED");
        order.setCreatedAt(LocalDateTime.now());
        order.setUpdatedAt(LocalDateTime.now());
        order.setFailureReason(null);

        orderRepository.save(order);

        System.out.print("Order Saved in the Orders");

        try {
            // Create event
            OrderEvent event = new OrderEvent();
            event.setEventId(eventId);
            event.setOrderId(orderId);
            event.setProductName(dto.getProductName());
            event.setQuantity(dto.getQuantity());
            event.setStatus("CREATED");

            String payload = objectMapper.writeValueAsString(event);

            // Save OUTBOX
            OutboxEvent outboxEvent = new OutboxEvent(
                    eventId,
                    orderId,
                    payload,
                    "NEW"
            );

            outboxEventRepository.save(outboxEvent);

            System.out.print("Event Stored in outbox");

        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize event", e);
        }

        return order;
    }

    public Order getOrder(String orderId) {
        return orderRepository.findByOrderId(orderId)
                .orElseThrow(() -> new RuntimeException("Order not found"));
    }
}