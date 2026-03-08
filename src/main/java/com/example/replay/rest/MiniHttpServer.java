package com.example.replay.rest;

import com.example.replay.storage.DataSourceConfig;
import com.example.replay.storage.InMemoryReplayJobRepository;
import com.example.replay.storage.PostgresReplayJobRepository;
import com.example.replay.storage.ReplayJobRepository;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.Optional;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Minimal HTTP server using raw sockets. Serves GET /health, POST/GET /api/v1/replay/jobs, GET /api/v1/replay/jobs/{id}.
 */
public final class MiniHttpServer implements Runnable, AutoCloseable {

    private static final String HEALTH_PATH = "/health";
    private static final String REPLAY_JOBS_PATH = "/api/v1/replay/jobs";
    private static final String REPLAY_JOBS_PATH_PREFIX = REPLAY_JOBS_PATH + "/";
    private static final String RESPONSE_OK = "OK";
    private static final String CRLF = "\r\n";
    private static final String CONTENT_LENGTH = "Content-Length";
    private static final String APPLICATION_JSON = "application/json; charset=UTF-8";

    private final int port;
    private final ReplayJobRepository jobRepository;
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
        this(port, new InMemoryReplayJobRepository());
    }

    /**
     * Creates a server with repository from env: use Postgres when REPLAY_JDBC_URL is set, else in-memory.
     */
    public static MiniHttpServer createWithConfiguredStorage(int port) {
        Optional<DataSource> ds = DataSourceConfig.createAndMigrate();
        ReplayJobRepository repo = ds.isPresent()
                ? new PostgresReplayJobRepository(ds.get())
                : new InMemoryReplayJobRepository();
        return new MiniHttpServer(port, repo);
    }

    public MiniHttpServer(int port, ReplayJobRepository jobRepository) {
        this.port = port;
        this.jobRepository = jobRepository;
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
             InputStream rawIn = s.getInputStream();
             OutputStream out = s.getOutputStream()) {

            String requestLine = readLine(rawIn);
            if (requestLine == null || requestLine.isEmpty()) {
                sendResponse(out, 400, "Bad Request", false);
                return;
            }
            String[] parts = requestLine.split("\\s+");
            if (parts.length < 2) {
                sendResponse(out, 400, "Bad Request", false);
                return;
            }
            String method = parts[0];
            String path = pathWithoutQuery(parts[1]);

            int contentLength = 0;
            String line;
            while ((line = readLine(rawIn)) != null && !line.isEmpty()) {
                if (line.toLowerCase().startsWith(CONTENT_LENGTH.toLowerCase() + ":")) {
                    String value = line.substring(line.indexOf(':') + 1).trim();
                    try {
                        contentLength = Integer.parseInt(value);
                    } catch (NumberFormatException ignored) {
                    }
                }
            }

            String body = null;
            if (contentLength > 0) {
                byte[] buf = new byte[contentLength];
                int read = 0;
                while (read < contentLength) {
                    int n = rawIn.read(buf, read, contentLength - read);
                    if (n <= 0) break;
                    read += n;
                }
                body = new String(buf, 0, read, StandardCharsets.UTF_8);
            }

            if ("GET".equalsIgnoreCase(method) && HEALTH_PATH.equals(path)) {
                sendResponse(out, 200, RESPONSE_OK, false);
                return;
            }
            if (REPLAY_JOBS_PATH.equals(path)) {
                if ("POST".equalsIgnoreCase(method)) {
                    ReplayJobHandler.HttpResponse apiResponse = ReplayJobHandler.handleCreateJob(jobRepository, body);
                    sendResponse(out, apiResponse.getStatusCode(), apiResponse.getBody(), true);
                    return;
                }
                if ("GET".equalsIgnoreCase(method)) {
                    ReplayJobHandler.HttpResponse apiResponse = ReplayJobHandler.handleListJobs(jobRepository);
                    sendResponse(out, apiResponse.getStatusCode(), apiResponse.getBody(), true);
                    return;
                }
            }
            if ("GET".equalsIgnoreCase(method) && path.startsWith(REPLAY_JOBS_PATH_PREFIX)) {
                String jobId = path.substring(REPLAY_JOBS_PATH_PREFIX.length()).trim();
                if (!jobId.isEmpty() && !jobId.contains("/")) {
                    ReplayJobHandler.HttpResponse apiResponse = ReplayJobHandler.handleGetJob(jobRepository, jobId);
                    sendResponse(out, apiResponse.getStatusCode(), apiResponse.getBody(), true);
                    return;
                }
            }
            sendResponse(out, 404, "Not Found", false);
        } catch (IOException e) {
            // connection error, ignore
        }
    }

    private static String pathWithoutQuery(String path) {
        int q = path.indexOf('?');
        return q < 0 ? path : path.substring(0, q);
    }

    private static String readLine(InputStream in) throws IOException {
        StringBuilder sb = new StringBuilder();
        int c;
        while ((c = in.read()) != -1) {
            if (c == '\n') break;
            if (c != '\r') sb.append((char) c);
        }
        return sb.toString();
    }

    private void sendResponse(OutputStream out, int statusCode, String body, boolean json) throws IOException {
        String status = statusLine(statusCode);
        byte[] bodyBytes = body.getBytes(StandardCharsets.UTF_8);
        String contentType = json ? APPLICATION_JSON : "text/plain; charset=UTF-8";
        String headers =
                "HTTP/1.1 " + status + CRLF +
                        "Content-Type: " + contentType + CRLF +
                        "Content-Length: " + bodyBytes.length + CRLF +
                        "Connection: close" + CRLF +
                        CRLF;
        out.write(headers.getBytes(StandardCharsets.UTF_8));
        out.write(bodyBytes);
        out.flush();
    }

    private static String statusLine(int code) {
        return switch (code) {
            case 200 -> "200 OK";
            case 201 -> "201 Created";
            case 400 -> "400 Bad Request";
            case 404 -> "404 Not Found";
            default -> code + " ";
        };
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
        MiniHttpServer server = createWithConfiguredStorage(port);
        server.start();
        System.out.println("Health check server listening on http://localhost:" + port + "/health");
        boolean usePostgres = System.getenv("REPLAY_JDBC_URL") != null && !System.getenv("REPLAY_JDBC_URL").isBlank();
        System.out.println("Job storage: " + (usePostgres ? "PostgreSQL" : "in-memory"));
        Runtime.getRuntime().addShutdownHook(new Thread(server::close));
        Thread.currentThread().join();
    }
}
