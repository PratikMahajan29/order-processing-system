package com.ops.order._processing.exception;

import com.ops.order._processing.enums.FailureType;

public class ClassifiedException extends RuntimeException {

    private final FailureType failureType;

    public ClassifiedException(FailureType failureType, String message, Throwable cause) {
        super(message, cause);
        this.failureType = failureType;
    }

    public FailureType getFailureType() {
        return failureType;
    }
}
