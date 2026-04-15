package com.ops.order._processing.enums;

public enum OrderStatus {

    PENDING(0),
    CREATED(1),
    PAYMENT_INITIATED(2),
    PAYMENT_COMPLETED(3),
    SHIPPED(4),
    DELIVERED(5),
    COMPLETED(6),
    FAILED(-1);

    private final int order;

    OrderStatus(int order) {
        this.order = order;
    }

    public int getOrder() {
        return order;
    }
}

