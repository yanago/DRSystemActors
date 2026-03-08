package com.example.replay.api;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Validates {@link CreateReplayJobRequest} for job creation.
 * Mandatory: name (non-blank), parameters (non-null, non-empty, must contain "source").
 */
public final class CreateReplayJobValidator {

    private static final String PARAM_SOURCE = "source";

    private CreateReplayJobValidator() {
    }

    public static ValidationResult validate(CreateReplayJobRequest request) {
        if (request == null) {
            return ValidationResult.invalid("Request body is required");
        }
        List<String> errors = new ArrayList<>();

        if (request.getName() == null || request.getName().isBlank()) {
            errors.add("name is required and must be non-blank");
        }

        Map<String, Object> params = request.getParameters();
        if (params == null) {
            errors.add("parameters is required");
        } else if (params.isEmpty()) {
            errors.add("parameters must not be empty");
        } else if (!params.containsKey(PARAM_SOURCE)) {
            errors.add("parameters must contain mandatory field: " + PARAM_SOURCE);
        }

        return errors.isEmpty() ? ValidationResult.valid() : ValidationResult.invalid(errors);
    }
}
