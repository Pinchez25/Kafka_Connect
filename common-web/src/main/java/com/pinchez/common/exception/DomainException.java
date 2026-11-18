package com.pinchez.common.exception;

public abstract sealed class DomainException extends RuntimeException
        permits ResourceNotFoundException, DuplicateResourceException, ServiceUnavailableException {

    protected DomainException(String message) {
        super(message);
    }
}
