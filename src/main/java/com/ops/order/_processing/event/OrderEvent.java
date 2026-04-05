package com.ops.order._processing.event;

public class OrderEvent {

    private String eventId;

    private String productName;

    private Integer quantity;

    private String status;

    private String orderId;


    // Constructors
    public OrderEvent() {
    }

    public OrderEvent(String eventId,String productName, Integer quantity, String status,String orderId) {
        this.eventId = eventId;
        this.productName = productName;
        this.quantity = quantity;
        this.status = status;
        this.orderId = orderId;
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

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }


    @Override
    public String toString() {
        return "OrderEvent{" +
                "productName='" + productName + '\'' +
                ", quantity=" + quantity +
                ", status='" + status + '\'' +
                ", eventId='" + eventId + '\'' +
                ", orderId='" + getOrderId() + '\'' +
                '}';
    }



}


