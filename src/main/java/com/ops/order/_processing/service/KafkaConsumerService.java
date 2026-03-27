package com.ops.order._processing.service;

import com.ops.order._processing.entity.Order;
import com.ops.order._processing.event.OrderEvent;
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

        try {
             // Maintain the idempotency by checking if the event has already been processed
            Optional<Order> existing  = orderRepository.findByEventId(event.getEventId());
            if(existing.isPresent()){
                System.out.print("Duplicate Event ignored: " + event.getEventId());
                ack.acknowledge(); // Acknowledge to avoid reprocessing
                return;
            }

            Order order = new Order();
            order.setEventId(event.getEventId());
            order.setProductName(event.getProductName());
            order.setQuantity(event.getQuantity());
            order.setStatus(event.getStatus());
            order.setCreatedAt(LocalDateTime.now());
            order.setUpdatedAt(LocalDateTime.now());

            orderRepository.save(order);

            ack.acknowledge(); // ---> This tells kafka that marks message as processed.

        } catch (Exception e) {
            System.out.println("Error processing message: " + e.getMessage());
        }
    }
}
