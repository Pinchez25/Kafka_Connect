package com.pinchez.common.exception;

import lombok.Getter;

@Getter
public final class ResourceNotFoundException extends DomainException {

    private final String resource;
    private final String identifier;

    public ResourceNotFoundException(Class<?> resourceType, Object identifier) {
        var resource = resourceType.getSimpleName();
        super("%s '%s' was not found".formatted(resource, identifier));
        this.resource = resource;
        this.identifier = String.valueOf(identifier);
    }
}
