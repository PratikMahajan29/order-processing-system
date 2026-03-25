package com.ops.order._processing.service;

import com.ops.order._processing.entity.Order;
import com.ops.order._processing.repository.OrderRepository;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
public class OrderService {

    private final OrderRepository orderRepository;

    public OrderService(OrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }

    public Order createOrder(Order order) {

        // Defensive logic (don’t trust input blindly)
        if (order.getProductName() == null || order.getQuantity() == null) {
            throw new RuntimeException("Invalid order data");
        }

        // System-controlled fields
        order.setStatus("CREATED");
        order.setCreatedAt(LocalDateTime.now());
        order.setUpdatedAt(LocalDateTime.now());

        return orderRepository.save(order);
    }

    public Order getOrder(Long id) {
        return orderRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Order not found with id: " + id));
    }
}