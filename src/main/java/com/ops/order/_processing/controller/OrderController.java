package com.ops.order._processing.controller;

import com.ops.order._processing.dto.OrderRequestDTO;
import com.ops.order._processing.entity.Order;
import com.ops.order._processing.event.OrderEvent;
import com.ops.order._processing.service.DLQReprocessingService;
import com.ops.order._processing.service.OrderService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderService orderService;
    private final DLQReprocessingService dlqReprocessingService;

    public OrderController(OrderService orderService,DLQReprocessingService dlqReprocessingService) {
        this.orderService = orderService;
        this.dlqReprocessingService = dlqReprocessingService;
    }

    @PostMapping
    public ResponseEntity<?> createOrder(@RequestBody OrderRequestDTO dto) {

        Order order = orderService.createOrder(dto);
        return ResponseEntity.ok(Map.of("OrderId",order.getOrderId(),
                "status",order.getStatus(),"failureReason",order.getFailureReason()));
    }

    @GetMapping("/{orderId}")
    public ResponseEntity<?> getOrder(@PathVariable String orderId) {

        Order order = orderService.getOrder(orderId);

        Map<String, Object> response = new HashMap<>();
        response.put("orderId", order.getOrderId());
        response.put("status", order.getStatus());
        response.put("failureReason", order.getFailureReason());

        return ResponseEntity.ok(response);
    }

    @PostMapping("/reprocess/{eventId}")
    public String reprocess(@PathVariable String eventId) {

        dlqReprocessingService.reprocessById(eventId);

        return "Reprocessing triggered for eventId: " + eventId;
    }


}