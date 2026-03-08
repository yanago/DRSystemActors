package com.example.replay.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Result of validating a create-job request. Empty errors means valid.
 */
public final class ValidationResult {

    private static final ValidationResult VALID = new ValidationResult(List.of());

    private final List<String> errors;

    @JsonCreator
    public ValidationResult(@JsonProperty("errors") List<String> errors) {
        this.errors = errors == null ? List.of() : List.copyOf(errors);
    }

    public static ValidationResult valid() {
        return VALID;
    }

    public static ValidationResult invalid(String... messages) {
        return new ValidationResult(List.of(messages));
    }

    public static ValidationResult invalid(List<String> messages) {
        return new ValidationResult(messages);
    }

    @JsonProperty("errors")
    public List<String> getErrors() {
        return Collections.unmodifiableList(errors);
    }

    public boolean isValid() {
        return errors.isEmpty();
    }
}
