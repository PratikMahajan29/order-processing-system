package com.ops.order._processing.statemachine;

import com.ops.order._processing.enums.OrderStatus;

import java.util.Map;
import java.util.Set;

public class OrderStateMachine {

    private static final Map<OrderStatus, Set<OrderStatus>> transitions = Map.of(

            // Normal flow + failure allowed
            OrderStatus.PENDING,
            Set.of(OrderStatus.CREATED),

            OrderStatus.CREATED,
            Set.of(OrderStatus.PAYMENT_INITIATED, OrderStatus.FAILED),

            OrderStatus.PAYMENT_INITIATED,
            Set.of(OrderStatus.PAYMENT_COMPLETED, OrderStatus.FAILED),

            OrderStatus.PAYMENT_COMPLETED,
            Set.of(OrderStatus.SHIPPED, OrderStatus.FAILED),

            OrderStatus.SHIPPED,
            Set.of(OrderStatus.DELIVERED, OrderStatus.FAILED),

            OrderStatus.DELIVERED,
            Set.of(OrderStatus.COMPLETED),

            // Terminal states
            OrderStatus.COMPLETED,
            Set.of(),

            OrderStatus.FAILED,
            Set.of()
    );

     // Validates whether a transition is allowed
    public static boolean isValidTransition(OrderStatus current, OrderStatus next) {

        if (current == null || next == null) {
            return false;
        }

        return transitions
                .getOrDefault(current, Set.of())
                .contains(next);
    }
}