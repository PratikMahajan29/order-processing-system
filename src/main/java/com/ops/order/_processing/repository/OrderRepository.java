package com.ops.order._processing.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import com.ops.order._processing.entity.Order;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface OrderRepository extends JpaRepository<Order, Long> {

    Optional<Order> findByEventId(String eventId);
}
