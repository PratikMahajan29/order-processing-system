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

    public OrderService(OrderRepository orderRepository,
                        OutboxEventRepository outboxEventRepository,
                        ObjectMapper objectMapper,
                        IdempotencyService idempotencyService) {
        this.orderRepository = orderRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
        this.idempotencyService = idempotencyService;
    }

    @Transactional
    public Order createOrder(OrderRequestDTO dto) {

        if (dto.getQuantity() == null) {
            throw new IllegalArgumentException("Quantity is required");
        }

        String orderId = java.util.UUID.randomUUID().toString();
        String eventId = java.util.UUID.randomUUID().toString();

        Order order = new Order();
        order.setOrderId(orderId);
        order.setProductName(dto.getProductName());
        order.setQuantity(dto.getQuantity());

        // 🔥 FIXED
        order.setStatus("PENDING");

        order.setEventSequence(0L);
        order.setCreatedAt(LocalDateTime.now());
        order.setUpdatedAt(LocalDateTime.now());
        order.setFailureReason(null);

        orderRepository.save(order);

        log.info("Order created | orderId={}", orderId);

        try {
            OrderEvent event = new OrderEvent();
            event.setEventId(eventId);
            event.setOrderId(orderId);
            event.setProductName(dto.getProductName());
            event.setQuantity(dto.getQuantity());

            event.setEventType("CREATED");
            event.setSequence(0L); // ignored
            event.setTimestamp(System.currentTimeMillis());

            String payload = objectMapper.writeValueAsString(event);

            OutboxEvent outboxEvent = new OutboxEvent(
                    eventId,
                    orderId,
                    payload,
                    "NEW"
            );

            outboxEventRepository.save(outboxEvent);

            log.info("Outbox event created | eventId={} | orderId={}",
                    eventId, orderId);

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
        try {
            requestJson = objectMapper.writeValueAsString(dto);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize request", e);
        }

        String requestHash = HashUtil.sha256(requestJson);

        IdempotencyKey idem = idempotencyService.process(key, requestHash);

        if ("COMPLETED".equals(idem.getStatus())) {
            log.info("Idempotency hit | key={} | returning cached response", key);
            return idem.getResponsePayload();
        }

        Order order = createOrder(dto);

        Map<String, Object> response = new HashMap<>();
        response.put("orderId", order.getOrderId());
        response.put("status", order.getStatus());

        String jsonResponse;
        try {
            jsonResponse = objectMapper.writeValueAsString(response);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize response", e);
        }

        idempotencyService.markCompleted(key, jsonResponse);

        return response;
    }

    @Transactional
    public Long publishEvent(String orderId, String eventType) {

        Order order = orderRepository.findByOrderId(orderId)
                .orElseThrow(() -> new RuntimeException("Order not found"));

        String eventId = java.util.UUID.randomUUID().toString();

        try {
            OrderEvent event = new OrderEvent();
            event.setEventId(eventId);
            event.setOrderId(orderId);
            event.setProductName(order.getProductName());
            event.setQuantity(order.getQuantity());

            event.setEventType(eventType);
            event.setSequence(0L);
            event.setTimestamp(System.currentTimeMillis());

            String payload = objectMapper.writeValueAsString(event);

            OutboxEvent outboxEvent = new OutboxEvent(
                    eventId,
                    orderId,
                    payload,
                    "NEW"
            );

            outboxEventRepository.save(outboxEvent);

            log.info("Event published | orderId={} | eventType={}",
                    orderId, eventType);

            return 0L;

        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize event", e);
        }
    }

    @Transactional
    public Object publishEventWithIdempotency(
            String key,
            String orderId,
            String eventType) {

        String requestJson;
        try {
            Map<String, Object> map = new HashMap<>();
            map.put("orderId", orderId);
            map.put("eventType", eventType);

            requestJson = objectMapper.writeValueAsString(map);

        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize request", e);
        }

        String requestHash = HashUtil.sha256(requestJson);

        IdempotencyKey idem = idempotencyService.process(key, requestHash);

        if ("COMPLETED".equals(idem.getStatus())) {
            log.info("Idempotency hit (event) | key={}", key);
            return idem.getResponsePayload();
        }

        publishEvent(orderId, eventType);

        Map<String, Object> response = new HashMap<>();
        response.put("orderId", orderId);
        response.put("eventType", eventType);
        response.put("status", "ACCEPTED");

        String jsonResponse;
        try {
            jsonResponse = objectMapper.writeValueAsString(response);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        idempotencyService.markCompleted(key, jsonResponse);

        return response;
    }
}