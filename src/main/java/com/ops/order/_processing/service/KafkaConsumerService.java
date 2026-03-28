package com.ops.order._processing.service;

import com.ops.order._processing.entity.Order;
import com.ops.order._processing.event.OrderEvent;
import com.ops.order._processing.exception.NonRetryableException;
import com.ops.order._processing.exception.RetryableException;
import com.ops.order._processing.repository.OrderRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Optional;

@Service
public class KafkaConsumerService {

    private final OrderRepository orderRepository;

    public KafkaConsumerService(OrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }

    @KafkaListener(topics = "order-topic", groupId = "order-group")
    public void consume(OrderEvent event, Acknowledgment ack) {

        System.out.println("Processing event: " + event.getEventId());

        try {

            //  Fetch existing order
            Order order = orderRepository.findByOrderId(event.getOrderId())
                    .orElseThrow(() -> new RetryableException("Order not found"));

            // Move to PROCESSING
            order.setStatus("PROCESSING");
            order.setUpdatedAt(LocalDateTime.now());
            orderRepository.save(order);

            // Business validation (NO DLQ)
            if (order.getQuantity() <= 0) {
                order.setStatus("FAILED");
                order.setFailureReason("Invalid quantity: " + order.getQuantity());
                order.setUpdatedAt(LocalDateTime.now());

                orderRepository.save(order);

                ack.acknowledge();
                return;
            }

            // Success
            order.setStatus("COMPLETED");
            order.setUpdatedAt(LocalDateTime.now());

            orderRepository.save(order);

            ack.acknowledge();

        } catch (NonRetryableException e) {
            throw e;

        } catch (Exception e) {
            System.out.println("Retry triggered for: " + event.getEventId() + " due to: " + e.getMessage());
            throw new RetryableException(e.getMessage());
        }
    }
}
