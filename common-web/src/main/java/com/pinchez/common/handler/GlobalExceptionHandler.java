package com.pinchez.common.handler;

import com.pinchez.common.enums.ProblemType;
import com.pinchez.common.exception.DomainException;
import com.pinchez.common.exception.DuplicateResourceException;
import com.pinchez.common.exception.ResourceNotFoundException;
import com.pinchez.common.exception.ServiceUnavailableException;
import java.util.List;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.TypeMismatchException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {

    public record Violation(String field, @Nullable String message) {}

    @ExceptionHandler
    ProblemDetail handleDomainException(DomainException exception) {
        return switch (exception) {
            case ResourceNotFoundException e -> {
                var problem = ProblemType.RESOURCE_NOT_FOUND.toProblemDetail(e.getMessage());
                problem.setProperty("resource", e.getResource());
                problem.setProperty("identifier", e.getIdentifier());
                yield problem;
            }
            case DuplicateResourceException e -> {
                var problem = ProblemType.DUPLICATE_RESOURCE.toProblemDetail(e.getMessage());
                problem.setProperty("resource", e.getResource());
                problem.setProperty("field", e.getField());
                problem.setProperty("value", e.getValue());
                yield problem;
            }
            case ServiceUnavailableException e -> ProblemType.SERVICE_UNAVAILABLE.toProblemDetail(e.getMessage());
        };
    }

    @ExceptionHandler
    ProblemDetail handleOptimisticLocking(OptimisticLockingFailureException exception) {
        log.info("Optimistic locking conflict: {}", exception.getMessage());
        return ProblemType.CONCURRENT_MODIFICATION.toProblemDetail(
                "The resource was changed by another request. Reload it and try again.");
    }

    @ExceptionHandler
    ProblemDetail handleDataIntegrityViolation(DataIntegrityViolationException exception) {
        log.warn("Data integrity violation", exception);
        return ProblemType.DATA_INTEGRITY_VIOLATION.toProblemDetail(
                "The request conflicts with the current state of the data.");
    }

    @ExceptionHandler
    ProblemDetail handleUnexpected(Exception exception) {
        log.error("Unhandled exception", exception);
        return ProblemType.INTERNAL_ERROR.toProblemDetail("An unexpected error occurred.");
    }

    @Override
    protected @Nullable ResponseEntity<Object> handleMethodArgumentNotValid(
            MethodArgumentNotValidException exception, HttpHeaders headers, HttpStatusCode status, WebRequest request) {

        var bindingResult = exception.getBindingResult();
        var violations = Stream.concat(
                        bindingResult.getFieldErrors().stream()
                                .map(error -> new Violation(error.getField(), error.getDefaultMessage())),
                        bindingResult.getGlobalErrors().stream()
                                .map(error -> new Violation(error.getObjectName(), error.getDefaultMessage())))
                .toList();

        return handleExceptionInternal(
                exception, validationProblem(exception.getBody(), violations), headers, status, request);
    }

    @Override
    protected @Nullable ResponseEntity<Object> handleTypeMismatch(
            TypeMismatchException exception, HttpHeaders headers, HttpStatusCode status, WebRequest request) {

        var field = exception.getPropertyName() != null ? exception.getPropertyName() : "unknown";
        var expected = exception.getRequiredType() != null
                ? exception.getRequiredType().getSimpleName()
                : "value";
        var violations = List.of(new Violation(field, "must be a valid " + expected));
        var problem = validationProblem(ProblemDetail.forStatus(status), violations);
        return handleExceptionInternal(exception, problem, headers, status, request);
    }

    @Override
    protected @Nullable ResponseEntity<Object> handleHttpMessageNotReadable(
            HttpMessageNotReadableException exception, HttpHeaders headers, HttpStatusCode status, WebRequest request) {

        var problem = ProblemType.MALFORMED_REQUEST.toProblemDetail(
                "The request body is missing, is not valid JSON, or has an unknown 'type'.");
        return handleExceptionInternal(exception, problem, headers, status, request);
    }

    private static ProblemDetail validationProblem(ProblemDetail problem, List<Violation> violations) {
        ProblemType.VALIDATION_FAILED.applyTo(problem);
        problem.setDetail("The request contains %d invalid value(s).".formatted(violations.size()));
        problem.setProperty("errors", violations);
        return problem;
    }
}
