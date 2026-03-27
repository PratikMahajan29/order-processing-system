package com.ops.order._processing.dto;

public class OrderRequestDTO {

    private String productName;

    private Integer quantity;

    // Constructors
    public OrderRequestDTO() {}

    public OrderRequestDTO(String productName, Integer quantity) {
        this.productName = productName;
        this.quantity = quantity;
    }

    // Getters and Setters
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
}

