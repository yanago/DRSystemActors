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
}
