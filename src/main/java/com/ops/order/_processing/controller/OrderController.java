package com.ops.order._processing.controller;

import com.ops.order._processing.dto.OrderRequestDTO;
import com.ops.order._processing.entity.Order;
import com.ops.order._processing.service.DLQReprocessingService;
import com.ops.order._processing.service.IdempotencyService;
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
    private final IdempotencyService idempotencyService;

    public OrderController(OrderService orderService, DLQReprocessingService dlqReprocessingService, IdempotencyService idempotencyService) {
        this.orderService = orderService;
        this.dlqReprocessingService = dlqReprocessingService;
        this.idempotencyService = idempotencyService;
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

    @ExceptionHandler({IllegalArgumentException.class, RuntimeException.class})
    public ResponseEntity<?> handleBadRequest(Exception e) {

        Map<String, Object> error = new HashMap<>();
        error.put("error", e.getMessage());

        return ResponseEntity.badRequest().body(error);
    }

    @PostMapping
    public ResponseEntity<?> createOrder(
            @RequestHeader("Idempotency-Key") String key,
            @RequestBody OrderRequestDTO dto) {

        Object response = orderService.createOrderWithIdempotency(key, dto);

        return ResponseEntity.ok(response);
    }

    @PostMapping("/reprocess/{eventId}")
    public String reprocess(@PathVariable String eventId) {

        dlqReprocessingService.reprocessById(eventId);

        return "Reprocessing triggered for eventId: " + eventId;
    }


}