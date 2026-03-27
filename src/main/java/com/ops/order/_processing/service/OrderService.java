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

    public OrderService(KafkaProducerService producerService) {
        this.producerService = producerService;
    }

    public void createOrder(OrderRequestDTO dto) {

        OrderEvent event = new OrderEvent();

        event.setEventId(java.util.UUID.randomUUID().toString());
        event.setProductName(dto.getProductName());
        event.setQuantity(dto.getQuantity());
        event.setStatus("CREATED");

        producerService.sendOrderEvent(event);
    }
}