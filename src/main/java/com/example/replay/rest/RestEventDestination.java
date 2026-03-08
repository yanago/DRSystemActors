package com.example.replay.rest;

import com.example.replay.api.EventDestination;
import com.example.replay.util.JsonUtil;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * POSTs events to a downstream REST API. Sends each batch as a single POST (JSON array)
 * or one POST per record, depending on configuration.
 */
public final class RestEventDestination implements EventDestination {

    public static final String REST_URL_KEY = "rest_url";
    public static final String REST_BATCH_MODE_KEY = "rest_batch_mode";
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);

    private final HttpClient client;
    private final String baseUrl;
    private final boolean batchMode;

    public RestEventDestination(String baseUrl) {
        this(baseUrl, true);
    }

    public RestEventDestination(String baseUrl, boolean batchMode) {
        this.baseUrl = Objects.requireNonNull(baseUrl, "baseUrl").trim().replaceAll("/+$", "");
        this.batchMode = batchMode;
        this.client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    public static RestEventDestination fromConfig(Map<String, Object> config) {
        Object urlObj = config.get(REST_URL_KEY);
        String url = urlObj != null ? urlObj.toString() : "http://localhost:8080/events";
        Object batchObj = config.get(REST_BATCH_MODE_KEY);
        boolean batch = batchObj == null || Boolean.TRUE.equals(batchObj);
        return new RestEventDestination(url, batch);
    }

    @Override
    public void sendBatch(List<Object> records) throws Exception {
        if (records == null || records.isEmpty()) return;
        if (batchMode) {
            postJson(baseUrl + "/events", JsonUtil.toJson(records));
        } else {
            for (Object record : records) {
                postJson(baseUrl + "/events", JsonUtil.toJson(record));
            }
        }
    }

    private void postJson(String url, String json) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(REQUEST_TIMEOUT)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json, StandardCharsets.UTF_8))
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        if (response.statusCode() >= 400) {
            throw new RuntimeException("REST POST failed: " + response.statusCode() + " " + response.body());
        }
    }

    @Override
    public void close() {
        // HttpClient doesn't need explicit close
    }
}
