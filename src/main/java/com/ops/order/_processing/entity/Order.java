package com.ops.order._processing.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "orders")
public class Order {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "order_id",unique = true)
    private String orderId;

    private String productName;

    private Integer quantity;

    private String status;

    @Column(name = "failure_reason")
    private String failureReason;

    private LocalDateTime createdAt;

    private LocalDateTime updatedAt;

    @Column(name = "event_sequence")
    private Long eventSequence = 0L;  //

    // Constructors
    public Order() {}

    public Order(String orderId, String productName, Integer quantity, String status,Long eventSequence) {
        this.orderId = orderId;
        this.productName = productName;
        this.quantity = quantity;
        this.status = status;
        this.eventSequence = eventSequence;
    }

    public Long getId() { return id; }

    public String getOrderId() { return orderId; }

    public void setOrderId(String orderId) { this.orderId = orderId; }

    public String getProductName() { return productName; }

    public void setProductName(String productName) { this.productName = productName; }

    public Integer getQuantity() { return quantity; }

    public void setQuantity(Integer quantity) { this.quantity = quantity; }

    public String getStatus() { return status; }

    public void setStatus(String status) { this.status = status; }

    public String getFailureReason() { return failureReason; }

    public void setFailureReason(String failureReason) { this.failureReason = failureReason; }

    public LocalDateTime getCreatedAt() { return createdAt; }

    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }

    public LocalDateTime getUpdatedAt() { return updatedAt; }

    public void setUpdatedAt(LocalDateTime updatedAt) { this.updatedAt = updatedAt; }

    public Long getEventSequence() {
        return eventSequence;
    }

    public void setEventSequence(Long eventSequence) {
        this.eventSequence = eventSequence;
    }
}