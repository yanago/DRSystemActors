package com.example.replay.rest;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Minimal HTTP server using raw sockets. Serves GET /health with 200 OK for health checks.
 */
public final class MiniHttpServer implements Runnable, AutoCloseable {

    private static final String HEALTH_PATH = "/health";
    private static final String RESPONSE_OK = "OK";
    private static final String CRLF = "\r\n";

    private final int port;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile ServerSocket serverSocket;
    private final ExecutorService acceptor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "mini-http-acceptor");
        t.setDaemon(true);
        return t;
    });
    private final ExecutorService workers = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "mini-http-worker");
        t.setDaemon(true);
        return t;
    });

    public MiniHttpServer(int port) {
        this.port = port;
    }

    public void start() throws IOException {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        serverSocket = new ServerSocket(port);
        acceptor.submit(this::acceptLoop);
    }

    @Override
    public void run() {
        try {
            start();
        } catch (IOException e) {
            throw new RuntimeException("Failed to start MiniHttpServer on port " + port, e);
        }
    }

    private void acceptLoop() {
        while (running.get() && serverSocket != null && !serverSocket.isClosed()) {
            try {
                Socket client = serverSocket.accept();
                workers.submit(() -> handle(client));
            } catch (IOException e) {
                if (running.get()) {
                    // log and continue
                }
            }
        }
    }

    private void handle(Socket client) {
        try (Socket s = client;
             BufferedReader in = new BufferedReader(
                     new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
             OutputStream out = s.getOutputStream()) {

            String requestLine = in.readLine();
            if (requestLine == null) {
                sendResponse(out, 400, "Bad Request");
                return;
            }
            String[] parts = requestLine.split("\\s+");
            if (parts.length < 2) {
                sendResponse(out, 400, "Bad Request");
                return;
            }
            String method = parts[0];
            String path = parts[1];

            // consume remaining headers
            String line;
            while ((line = in.readLine()) != null && !line.isEmpty()) {
                // skip
            }

            if ("GET".equalsIgnoreCase(method) && HEALTH_PATH.equals(path)) {
                sendResponse(out, 200, RESPONSE_OK);
            } else {
                sendResponse(out, 404, "Not Found");
            }
        } catch (IOException e) {
            // connection error, ignore
        }
    }

    private void sendResponse(OutputStream out, int statusCode, String body) throws IOException {
        String status = statusCode == 200 ? "200 OK" : statusCode == 404 ? "404 Not Found" : "400 Bad Request";
        byte[] bodyBytes = body.getBytes(StandardCharsets.UTF_8);
        String headers =
                "HTTP/1.1 " + status + CRLF +
                        "Content-Type: text/plain; charset=UTF-8" + CRLF +
                        "Content-Length: " + bodyBytes.length + CRLF +
                        "Connection: close" + CRLF +
                        CRLF;
        out.write(headers.getBytes(StandardCharsets.UTF_8));
        out.write(bodyBytes);
        out.flush();
    }

    @Override
    public void close() {
        running.set(false);
        if (serverSocket != null) {
            try {
                serverSocket.close();
            } catch (IOException ignored) {
            }
            serverSocket = null;
        }
        acceptor.shutdown();
        workers.shutdown();
    }

    public boolean isRunning() {
        return running.get();
    }

    /** Configured port (may be 0 if random port was requested). */
    public int getPort() {
        return port;
    }

    /** Actual bound port (same as getPort() unless port was 0). */
    public int getLocalPort() {
        return serverSocket != null ? serverSocket.getLocalPort() : -1;
    }

    /**
     * Starts the server on port 8080. Usage: java ... MiniHttpServer
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 8080;
        MiniHttpServer server = new MiniHttpServer(port);
        server.start();
        System.out.println("Health check server listening on http://localhost:" + port + "/health");
        Runtime.getRuntime().addShutdownHook(new Thread(server::close));
        Thread.currentThread().join();
    }
}
