package com.pinchez.common.enums;

import java.net.URI;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;

public enum ProblemType {
    RESOURCE_NOT_FOUND("resource-not-found", "Resource not found", HttpStatus.NOT_FOUND),
    DUPLICATE_RESOURCE("duplicate-resource", "Resource already exists", HttpStatus.CONFLICT),
    CONCURRENT_MODIFICATION("concurrent-modification", "Resource was modified concurrently", HttpStatus.CONFLICT),
    DATA_INTEGRITY_VIOLATION("data-integrity-violation", "Request conflicts with existing data", HttpStatus.CONFLICT),
    MALFORMED_REQUEST("malformed-request", "Malformed request", HttpStatus.BAD_REQUEST),
    VALIDATION_FAILED("validation-failed", "Request validation failed", HttpStatus.BAD_REQUEST),
    INTERNAL_ERROR("internal-error", "Internal server error", HttpStatus.INTERNAL_SERVER_ERROR);

    private static final String URN_PREFIX = "urn:pesaflow:problem:";

    private final URI type;
    private final String title;
    private final HttpStatus status;

    ProblemType(String slug, String title, HttpStatus status) {
        this.type = URI.create(URN_PREFIX + slug);
        this.title = title;
        this.status = status;
    }

    public URI type() {
        return type;
    }

    public String title() {
        return title;
    }

    public HttpStatus status() {
        return status;
    }

    public ProblemDetail toProblemDetail(String detail) {
        return applyTo(ProblemDetail.forStatusAndDetail(status, detail));
    }

    public ProblemDetail applyTo(ProblemDetail problem) {
        problem.setType(type);
        problem.setTitle(title);
        return problem;
    }
}
