package com.ops.order._processing.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import com.ops.order._processing.entity.Order;

public interface OrderRepository extends JpaRepository<Order, Long> {

}
