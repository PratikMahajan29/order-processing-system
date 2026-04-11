package com.ops.order._processing.event;

public class OrderEvent {

    private String eventId;
    private String orderId;

    private String eventType;
    private Long sequence;

    private Long timestamp;

    private String productName;
    private Integer quantity;


    // Constructors
    public OrderEvent() {
    }

    public OrderEvent(String eventId,String productName, Integer quantity, String eventType,Long sequence,String orderId) {
        this.eventId = eventId;
        this.productName = productName;
        this.quantity = quantity;
        this.orderId = orderId;
        this.eventType = eventType;
        this.sequence = sequence;
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

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }


    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public Long getSequence() {
        return sequence;
    }

    public void setSequence(Long sequence) {
        this.sequence = sequence;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }


    @Override
    public String toString() {
        return "OrderEvent{" +
                "productName='" + productName + '\'' +
                ", quantity=" + quantity + '\'' +
                ", eventId='" + eventId + '\'' +
                ", orderId='" + orderId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", sequence='" + sequence + '\'' +
                '}';
    }


}


