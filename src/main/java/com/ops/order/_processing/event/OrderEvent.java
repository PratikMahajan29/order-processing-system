package com.ops.order._processing.event;

import java.util.UUID;

public class OrderEvent {

    private String eventId;

    private String productName;

    private Integer quantity;

    private String status;

    // Constructors
    public OrderEvent() {
    }

    public OrderEvent(String eventId,String productName, Integer quantity, String status) {
        this.eventId = eventId;
        this.productName = productName;
        this.quantity = quantity;
        this.status = status;
    }

    // Getters and Setters

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public Integer getQuantity() {
        return quantity;
    }

    public void setQuantity(Integer quantity) {
        this.quantity = quantity;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "OrderEvent{" +
                "productName='" + productName + '\'' +
                ", quantity=" + quantity +
                ", status='" + status + '\'' +
                ", eventId='" + eventId + '\'' +
                '}';
    }


}


