package com.pinchez.common.exception;

import lombok.Getter;

@Getter
public final class DuplicateResourceException extends DomainException {

    private final String resource;
    private final String field;
    private final String value;

    public DuplicateResourceException(Class<?> resourceType, String field, Object value) {
        var resource = resourceType.getSimpleName();
        super("%s with %s '%s' already exists".formatted(resource, field, value));
        this.resource = resource;
        this.field = field;
        this.value = String.valueOf(value);
    }
}
