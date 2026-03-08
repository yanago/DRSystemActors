package com.example.replay.rest;

import com.example.replay.api.CreateReplayJobRequest;
import com.example.replay.api.CreateReplayJobResponse;
import com.example.replay.api.CreateReplayJobValidator;
import com.example.replay.api.ValidationResult;
import com.example.replay.model.ReplayJob;
import com.example.replay.storage.ReplayJobRepository;
import com.example.replay.util.JsonUtil;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Handles replay job API: create (POST), list (GET), get by id (GET).
 */
public final class ReplayJobHandler {

    private static final String STATUS_PENDING = "PENDING";

    private ReplayJobHandler() {
    }

    /**
     * POST create job. Validates, persists, returns 201 with job_id and status.
     */
    public static HttpResponse handleCreateJob(ReplayJobRepository repository, String body) {
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

        Instant now = Instant.now();
        String jobId = UUID.randomUUID().toString();
        Map<String, Object> params = new HashMap<>(request.getParameters() != null ? request.getParameters() : Map.of());
        if (request.getName() != null) {
            params.put("name", request.getName());
        }
        ReplayJob job = new ReplayJob(jobId, ReplayJob.ReplayJobStatus.PENDING, params, now, now, null);
        repository.save(job);

        CreateReplayJobResponse response = new CreateReplayJobResponse(jobId, STATUS_PENDING);
        return HttpResponse.created(JsonUtil.toJson(response));
    }

    /**
     * GET list all jobs. Returns 200 with JSON array of jobs.
     */
    public static HttpResponse handleListJobs(ReplayJobRepository repository) {
        return HttpResponse.ok(JsonUtil.toJson(repository.findAll()));
    }

    /**
     * GET job by id. Returns 200 with job JSON or 404.
     */
    public static HttpResponse handleGetJob(ReplayJobRepository repository, String jobId) {
        return repository.findById(jobId)
                .map(job -> HttpResponse.ok(JsonUtil.toJson(job)))
                .orElse(HttpResponse.notFound("{\"error\":\"Job not found\",\"job_id\":\"" + jobId + "\"}"));
    }

    public static final class HttpResponse {
        private final int statusCode;
        private final String body;

        private HttpResponse(int statusCode, String body) {
            this.statusCode = statusCode;
            this.body = body;
        }

        public static HttpResponse ok(String jsonBody) {
            return new HttpResponse(200, jsonBody);
        }

        public static HttpResponse created(String jsonBody) {
            return new HttpResponse(201, jsonBody);
        }

        public static HttpResponse badRequest(String jsonBody) {
            return new HttpResponse(400, jsonBody);
        }

        public static HttpResponse notFound(String jsonBody) {
            return new HttpResponse(404, jsonBody);
        }

        public int getStatusCode() {
            return statusCode;
        }

        public String getBody() {
            return body;
        }
    }
}
