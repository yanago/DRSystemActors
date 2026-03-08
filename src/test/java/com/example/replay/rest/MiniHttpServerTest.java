package com.example.replay.rest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MiniHttpServerTest {

    private MiniHttpServer server;

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.close();
        }
    }

    @Test
    void healthReturnsOk() throws IOException, InterruptedException {
        server = new MiniHttpServer(0);
        server.start();
        int port = server.getLocalPort();

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/health"))
                .GET()
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        assertEquals("OK", response.body());
    }

    @Test
    void otherPathsReturn404() throws IOException, InterruptedException {
        server = new MiniHttpServer(0);
        server.start();
        int port = server.getLocalPort();

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/other"))
                .GET()
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(404, response.statusCode());
    }

    @Test
    void postReplayJobsCreatesJobAndReturns201WithJobId() throws IOException, InterruptedException {
        server = new MiniHttpServer(0);
        server.start();
        int port = server.getLocalPort();

        String body = "{\"name\":\"my-replay\",\"parameters\":{\"source\":\"kafka-topic-1\",\"target\":\"s3://bucket/path\"}}";
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/api/v1/replay/jobs"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(201, response.statusCode());
        assertTrue(response.body().contains("\"job_id\""));
        assertTrue(response.body().contains("\"status\":\"PENDING\""));
    }

    @Test
    void postReplayJobsWithMissingNameReturns400() throws IOException, InterruptedException {
        server = new MiniHttpServer(0);
        server.start();
        int port = server.getLocalPort();

        String body = "{\"parameters\":{\"source\":\"kafka\"}}";
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/api/v1/replay/jobs"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(400, response.statusCode());
        assertTrue(response.body().contains("errors"));
        assertTrue(response.body().contains("name"));
    }

    @Test
    void postReplayJobsWithMissingSourceParamReturns400() throws IOException, InterruptedException {
        server = new MiniHttpServer(0);
        server.start();
        int port = server.getLocalPort();

        String body = "{\"name\":\"job\",\"parameters\":{\"other\":\"value\"}}";
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/api/v1/replay/jobs"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(400, response.statusCode());
        assertTrue(response.body().contains("source"));
    }

    @Test
    void postReplayJobsWithInvalidJsonReturns400() throws IOException, InterruptedException {
        server = new MiniHttpServer(0);
        server.start();
        int port = server.getLocalPort();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/api/v1/replay/jobs"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("not json"))
                .build();
        HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(400, response.statusCode());
    }

    @Test
    void getReplayJobsReturns200AndJsonArray() throws IOException, InterruptedException {
        server = new MiniHttpServer(0);
        server.start();
        int port = server.getLocalPort();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/api/v1/replay/jobs"))
                .GET()
                .build();
        HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        assertTrue(response.body().startsWith("["));
        assertTrue(response.body().endsWith("]"));
    }

    @Test
    void getReplayJobsReturnsCreatedJobs() throws IOException, InterruptedException {
        server = new MiniHttpServer(0);
        server.start();
        int port = server.getLocalPort();
        HttpClient client = HttpClient.newHttpClient();

        String createBody = "{\"name\":\"list-test\",\"parameters\":{\"source\":\"kafka\"}}";
        HttpRequest create = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/api/v1/replay/jobs"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(createBody))
                .build();
        HttpResponse<String> createResponse = client.send(create, HttpResponse.BodyHandlers.ofString());
        assertEquals(201, createResponse.statusCode());
        String jobId = createResponse.body().replaceAll(".*\"job_id\":\"([^\"]+)\".*", "$1");
        assertTrue(jobId.length() > 10);

        HttpRequest list = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/api/v1/replay/jobs"))
                .GET()
                .build();
        HttpResponse<String> listResponse = client.send(list, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, listResponse.statusCode());
        assertTrue(listResponse.body().contains(jobId));
    }

    @Test
    void getReplayJobByIdReturns200WhenExists() throws IOException, InterruptedException {
        server = new MiniHttpServer(0);
        server.start();
        int port = server.getLocalPort();
        HttpClient client = HttpClient.newHttpClient();

        String createBody = "{\"name\":\"get-test\",\"parameters\":{\"source\":\"kafka\"}}";
        HttpRequest create = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/api/v1/replay/jobs"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(createBody))
                .build();
        HttpResponse<String> createResponse = client.send(create, HttpResponse.BodyHandlers.ofString());
        assertEquals(201, createResponse.statusCode());
        String jobId = createResponse.body().replaceAll(".*\"job_id\":\"([^\"]+)\".*", "$1");

        HttpRequest get = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/api/v1/replay/jobs/" + jobId))
                .GET()
                .build();
        HttpResponse<String> getResponse = client.send(get, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, getResponse.statusCode());
        assertTrue(getResponse.body().contains(jobId), "body should contain job_id: " + getResponse.body());
        assertTrue(getResponse.body().contains("\"status\":") && (getResponse.body().contains("PENDING") || getResponse.body().contains("RUNNING")), "body should contain status");
    }

    @Test
    void postJobStartReturns200WhenJobExists() throws IOException, InterruptedException {
        server = new MiniHttpServer(0);
        server.start();
        int port = server.getLocalPort();
        HttpClient client = HttpClient.newHttpClient();

        String createBody = "{\"name\":\"lifecycle\",\"parameters\":{\"source\":\"kafka\"}}";
        HttpRequest create = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/api/v1/replay/jobs"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(createBody))
                .build();
        HttpResponse<String> createResponse = client.send(create, HttpResponse.BodyHandlers.ofString());
        assertEquals(201, createResponse.statusCode());
        String jobId = createResponse.body().replaceAll(".*\"job_id\":\"([^\"]+)\".*", "$1");

        HttpRequest start = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/api/v1/replay/jobs/" + jobId + "/start"))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        HttpResponse<String> startResponse = client.send(start, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, startResponse.statusCode());
        assertTrue(startResponse.body().contains("accepted"));
        assertTrue(startResponse.body().contains(jobId));
    }

    @Test
    void postJobPauseReturns200WhenJobExists() throws IOException, InterruptedException {
        server = new MiniHttpServer(0);
        server.start();
        int port = server.getLocalPort();
        HttpClient client = HttpClient.newHttpClient();

        String createBody = "{\"name\":\"pause-test\",\"parameters\":{\"source\":\"kafka\"}}";
        HttpRequest create = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/api/v1/replay/jobs"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(createBody))
                .build();
        HttpResponse<String> createResponse = client.send(create, HttpResponse.BodyHandlers.ofString());
        String jobId = createResponse.body().replaceAll(".*\"job_id\":\"([^\"]+)\".*", "$1");

        HttpRequest pause = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/api/v1/replay/jobs/" + jobId + "/pause"))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        HttpResponse<String> pauseResponse = client.send(pause, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, pauseResponse.statusCode());
        assertTrue(pauseResponse.body().contains("accepted"));
    }

    @Test
    void postJobCancelReturns200WhenJobExists() throws IOException, InterruptedException {
        server = new MiniHttpServer(0);
        server.start();
        int port = server.getLocalPort();
        HttpClient client = HttpClient.newHttpClient();

        String createBody = "{\"name\":\"cancel-test\",\"parameters\":{\"source\":\"kafka\"}}";
        HttpRequest create = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/api/v1/replay/jobs"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(createBody))
                .build();
        HttpResponse<String> createResponse = client.send(create, HttpResponse.BodyHandlers.ofString());
        String jobId = createResponse.body().replaceAll(".*\"job_id\":\"([^\"]+)\".*", "$1");

        HttpRequest cancel = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/api/v1/replay/jobs/" + jobId + "/cancel"))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        HttpResponse<String> cancelResponse = client.send(cancel, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, cancelResponse.statusCode());
        assertTrue(cancelResponse.body().contains("accepted"));
    }

    @Test
    void postJobLifecycleReturns404WhenJobNotExists() throws IOException, InterruptedException {
        server = new MiniHttpServer(0);
        server.start();
        int port = server.getLocalPort();

        HttpRequest start = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/api/v1/replay/jobs/nonexistent-id-999/start"))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        HttpResponse<String> response = HttpClient.newHttpClient().send(start, HttpResponse.BodyHandlers.ofString());
        assertEquals(404, response.statusCode());
        assertTrue(response.body().contains("Job not found"));
    }

    @Test
    void getReplayJobByIdReturns404WhenNotExists() throws IOException, InterruptedException {
        server = new MiniHttpServer(0);
        server.start();
        int port = server.getLocalPort();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/api/v1/replay/jobs/nonexistent-id-12345"))
                .GET()
                .build();
        HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(404, response.statusCode());
        assertTrue(response.body().contains("Job not found"));
    }

    @Test
    void getJobStatusReturns200WithProgress() throws IOException, InterruptedException {
        server = new MiniHttpServer(0);
        server.start();
        int port = server.getLocalPort();
        HttpClient client = HttpClient.newHttpClient();

        String createBody = "{\"name\":\"status-job\",\"parameters\":{\"source\":\"kafka\"}}";
        HttpRequest create = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/api/v1/replay/jobs"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(createBody))
                .build();
        HttpResponse<String> createResponse = client.send(create, HttpResponse.BodyHandlers.ofString());
        assertEquals(201, createResponse.statusCode());
        String jobId = createResponse.body().replaceAll(".*\"job_id\":\"([^\"]+)\".*", "$1");

        HttpRequest statusReq = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/api/v1/replay/jobs/" + jobId + "/status"))
                .GET()
                .build();
        HttpResponse<String> statusResponse = client.send(statusReq, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, statusResponse.statusCode());
        assertTrue(statusResponse.body().contains("\"job_id\":\"" + jobId + "\""));
        assertTrue(statusResponse.body().contains("\"status\":"));
        assertTrue(statusResponse.body().contains("progress") || statusResponse.body().contains("events_processed"));
    }

    @Test
    void getJobMetricsReturns200WithMetrics() throws IOException, InterruptedException {
        server = new MiniHttpServer(0);
        server.start();
        int port = server.getLocalPort();
        HttpClient client = HttpClient.newHttpClient();

        String createBody = "{\"name\":\"metrics-job\",\"parameters\":{\"source\":\"kafka\"}}";
        HttpRequest create = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/api/v1/replay/jobs"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(createBody))
                .build();
        HttpResponse<String> createResponse = client.send(create, HttpResponse.BodyHandlers.ofString());
        assertEquals(201, createResponse.statusCode());
        String jobId = createResponse.body().replaceAll(".*\"job_id\":\"([^\"]+)\".*", "$1");

        HttpRequest metricsReq = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/api/v1/replay/jobs/" + jobId + "/metrics"))
                .GET()
                .build();
        HttpResponse<String> metricsResponse = client.send(metricsReq, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, metricsResponse.statusCode());
        assertTrue(metricsResponse.body().contains("\"job_id\":\"" + jobId + "\""));
        assertTrue(metricsResponse.body().contains("\"metrics\":"));
        assertTrue(metricsResponse.body().contains("events_per_second"));
        assertTrue(metricsResponse.body().contains("error_count"));
    }

    @Test
    void getJobStatusReturns404WhenJobNotExists() throws IOException, InterruptedException {
        server = new MiniHttpServer(0);
        server.start();
        int port = server.getLocalPort();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/api/v1/replay/jobs/nonexistent-id/status"))
                .GET()
                .build();
        HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(404, response.statusCode());
    }

    @Test
    void getJobMetricsReturns404WhenJobNotExists() throws IOException, InterruptedException {
        server = new MiniHttpServer(0);
        server.start();
        int port = server.getLocalPort();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/api/v1/replay/jobs/nonexistent-id/metrics"))
                .GET()
                .build();
        HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(404, response.statusCode());
    }
}
