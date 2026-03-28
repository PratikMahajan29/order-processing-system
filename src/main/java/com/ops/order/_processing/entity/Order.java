package com.ops.order._processing.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "orders")
public class Order {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "order_id")
    private String orderId;

    @Column(name = "event_id")
    private String eventId;

    private String productName;

    private Integer quantity;

    private String status;

    @Column(name = "failure_reason")
    private String failureReason;

    private LocalDateTime createdAt;

    private LocalDateTime updatedAt;

    // Constructors
    public Order() {}

    public Order(String orderId, String eventId, String productName, Integer quantity, String status) {
        this.orderId = orderId;
        this.eventId = eventId;
        this.productName = productName;
        this.quantity = quantity;
        this.status = status;
    }

    // Getters & Setters

    public Long getId() { return id; }

    public String getOrderId() { return orderId; }

    public void setOrderId(String orderId) { this.orderId = orderId; }

    public String getEventId() { return eventId; }

    public void setEventId(String eventId) { this.eventId = eventId; }

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
}