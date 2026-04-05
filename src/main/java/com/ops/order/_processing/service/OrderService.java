package com.ops.order._processing.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ops.order._processing.Util.HashUtil;
import com.ops.order._processing.dto.OrderRequestDTO;
import com.ops.order._processing.entity.IdempotencyKey;
import com.ops.order._processing.entity.Order;
import com.ops.order._processing.entity.OutboxEvent;
import com.ops.order._processing.event.OrderEvent;
import com.ops.order._processing.repository.OrderRepository;
import com.ops.order._processing.repository.OutboxEventRepository;
import jakarta.transaction.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;
    private final IdempotencyService idempotencyService;

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);


    public OrderService(OrderRepository orderRepository, OutboxEventRepository outboxEventRepository, ObjectMapper objectMapper, IdempotencyService idempotencyService) {
        this.orderRepository = orderRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
        this.idempotencyService = idempotencyService;
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

        log.info("Order created with ID: {}", order.getOrderId());

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

            log.info("Outbox event created with ID: {}", event.getEventId());

        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize event", e);
        }

        return order;
    }

    public Order getOrder(String orderId) {
        return orderRepository.findByOrderId(orderId)
                .orElseThrow(() -> new RuntimeException("Order not found"));
    }

    @Transactional
    public Object createOrderWithIdempotency(String key, OrderRequestDTO dto) {

        String requestJson;
        try{
            requestJson = objectMapper.writeValueAsString(dto);
        }catch(Exception e){
            throw new RuntimeException("Failed to serialize request", e);
        }
        String requestHash = HashUtil.sha256(requestJson);

        IdempotencyKey idem = idempotencyService.process(key, requestHash);

        //  Already completed
        if ("COMPLETED".equals(idem.getStatus())) {
            System.out.print("Already completed ---> Returning response for idempotency key: " + key);
            return idem.getResponsePayload();
        }

        //  BUSINESS LOGIC
        Order order = createOrder(dto);

        Map<String, Object> response = new HashMap<>();
        response.put("orderId", order.getOrderId());
        response.put("status", order.getStatus());

        //  Mark completed INSIDE SAME TX
        String jsonResponse;
        try {
            jsonResponse = objectMapper.writeValueAsString(response);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize response", e);
        }
        idempotencyService.markCompleted(key, jsonResponse);

        return response;
    }
}