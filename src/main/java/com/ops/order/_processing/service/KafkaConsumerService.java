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

            if(event.getQuantity() <= 0){
                throw new NonRetryableException("Invalid quantity: " + event.getQuantity());
            }

            Order order = new Order();
            order.setEventId(event.getEventId());
            order.setProductName(event.getProductName());
            order.setQuantity(event.getQuantity());
            order.setStatus(event.getStatus());
            order.setCreatedAt(LocalDateTime.now());
            order.setUpdatedAt(LocalDateTime.now());

            orderRepository.save(order);

            ack.acknowledge(); // ---> This tells kafka that mark message as processed.

        } catch (org.springframework.dao.DataIntegrityViolationException ex) {

            if (isDuplicateEvent(ex)) {
                System.out.println("Duplicate event ignored: " + event.getEventId());
                ack.acknowledge(); //  treat duplicate as success
            } else {
                throw new RetryableException("Retryable error: " + ex);
            }

        } catch (NonRetryableException e) {
            throw e;

        } catch (Exception e) {
            System.out.println("Retry triggered for: " + event.getEventId() + " due to: " + e.getMessage());
            throw new RetryableException(e.getMessage());
        }
    }

    private boolean isDuplicateEvent(org.springframework.dao.DataIntegrityViolationException ex) {
        return ex.getMessage() != null &&
                ex.getMessage().contains("unique_event_id");
    }
}
