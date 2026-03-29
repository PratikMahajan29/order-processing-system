package com.ops.order._processing.repository;

import com.ops.order._processing.entity.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent,Long> {

    List<OutboxEvent> findByStatus(String status);
}
