package com.example.replay.rest;

import com.example.replay.api.CreateReplayJobRequest;
import com.example.replay.api.CreateReplayJobResponse;
import com.example.replay.api.CreateReplayJobValidator;
import com.example.replay.api.ValidationResult;
import com.example.replay.util.JsonUtil;

import java.util.UUID;

/**
 * Handles POST /api/v1/replay/jobs: validates request, generates job ID, returns response.
 */
public final class ReplayJobHandler {

    private static final String STATUS_PENDING = "PENDING";

    private ReplayJobHandler() {
    }

    /**
     * Handles create-job body. Returns HTTP status and JSON body.
     * - 201: created, body = CreateReplayJobResponse
     * - 400: invalid input or validation failed, body = ValidationResult or error message
     */
    public static HttpResponse handleCreateJob(String body) {
        if (body == null || body.isBlank()) {
            return HttpResponse.badRequest(JsonUtil.toJson(ValidationResult.invalid("Request body is required")));
        }

        CreateReplayJobRequest request;
        try {
            request = JsonUtil.fromJson(body, CreateReplayJobRequest.class);
        } catch (Exception e) {
            return HttpResponse.badRequest(JsonUtil.toJson(ValidationResult.invalid("Invalid JSON: " + e.getMessage())));
        }

        ValidationResult validation = CreateReplayJobValidator.validate(request);
        if (!validation.isValid()) {
            return HttpResponse.badRequest(JsonUtil.toJson(validation));
        }

        String jobId = UUID.randomUUID().toString();
        CreateReplayJobResponse response = new CreateReplayJobResponse(jobId, STATUS_PENDING);
        return HttpResponse.created(JsonUtil.toJson(response));
    }

    public static final class HttpResponse {
        private final int statusCode;
        private final String body;

        private HttpResponse(int statusCode, String body) {
            this.statusCode = statusCode;
            this.body = body;
        }

        public static HttpResponse created(String jsonBody) {
            return new HttpResponse(201, jsonBody);
        }

        public static HttpResponse badRequest(String jsonBody) {
            return new HttpResponse(400, jsonBody);
        }

        public int getStatusCode() {
            return statusCode;
        }

        public String getBody() {
            return body;
        }
    }
}
