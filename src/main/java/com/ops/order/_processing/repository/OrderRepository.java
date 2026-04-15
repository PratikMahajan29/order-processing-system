package com.ops.order._processing.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import com.ops.order._processing.entity.Order;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface OrderRepository extends JpaRepository<Order, Long> {

    Optional<Order> findByOrderId(String orderId);

    @Modifying
    @Query("""
        UPDATE Order o 
        SET o.eventSequence = o.eventSequence + 1 
        WHERE o.orderId = :orderId
        """)
    int incrementSequence(@Param("orderId") String orderId);

    @Query("SELECT o.eventSequence FROM Order o WHERE o.orderId = :orderId")
    Long getCurrentSequence(@Param("orderId") String orderId);
}
