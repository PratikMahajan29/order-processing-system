package com.ops.order._processing.service;

import com.ops.order._processing.dto.OrderRequestDTO;
import com.ops.order._processing.entity.Order;
import com.ops.order._processing.event.OrderEvent;
import com.ops.order._processing.repository.OrderRepository;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
public class OrderService {

    private final KafkaProducerService producerService;
    private final OrderRepository orderRepository;

    public OrderService(KafkaProducerService producerService, OrderRepository orderRepository) {
        this.producerService = producerService;
        this.orderRepository = orderRepository;
    }

    public Order createOrder(OrderRequestDTO dto) {

        // Basic validation
        if (dto.getQuantity() == null) {
             Order order = new Order();
                order.setOrderId(java.util.UUID.randomUUID().toString());
                order.setStatus(("FAILED"));
                order.setFailureReason("Quantity is required");

                return order;

        }

        // Generate IDs
        String orderId = java.util.UUID.randomUUID().toString();
        String eventId = java.util.UUID.randomUUID().toString();

        // Save order in DB (SOURCE OF TRUTH)
        Order order = new Order();
        order.setOrderId(orderId);
        order.setEventId(eventId);
        order.setProductName(dto.getProductName());
        order.setQuantity(dto.getQuantity());
        order.setStatus("CREATED");
        order.setCreatedAt(java.time.LocalDateTime.now());
        order.setUpdatedAt(java.time.LocalDateTime.now());
        order.setFailureReason("");

        orderRepository.save(order);

        System.out.print("Order saved with ID: " + orderId + " and Event ID: " + eventId);

        // Publish event
        OrderEvent event = new OrderEvent();
        event.setEventId(eventId);
        event.setOrderId(orderId);
        event.setProductName(dto.getProductName());
        event.setQuantity(dto.getQuantity());
        event.setStatus("CREATED");

        producerService.sendOrderEvent(event);

        System.out.print("Order event sent to Kafka for Order ID: " + orderId + " with Event ID: " + eventId);

        return order;
    }


    public Order getOrder(String orderId) {
        return orderRepository.findByOrderId(orderId)
                .orElseThrow(() -> new RuntimeException("Order not found"));
    }
}