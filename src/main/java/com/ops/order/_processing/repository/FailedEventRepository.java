package com.ops.order._processing.repository;

import com.ops.order._processing.entity.FailedEvent;
import org.springframework.data.jpa.repository.JpaRepository;

public interface FailedEventRepository extends JpaRepository<FailedEvent, String> {
}
