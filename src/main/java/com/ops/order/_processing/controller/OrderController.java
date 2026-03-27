package com.ops.order._processing.controller;

import com.ops.order._processing.dto.OrderRequestDTO;
import com.ops.order._processing.entity.Order;
import com.ops.order._processing.service.OrderService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping
    public ResponseEntity<String> createOrder(@RequestBody OrderRequestDTO dto) {
        orderService.createOrder(dto);
        return ResponseEntity.ok("Order event sent to Kafka");
    }
}