package com.example.replay.rest;

import com.example.replay.api.EventDestination;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * In-memory destination for tests: stores all received records.
 * Does not require a real HTTP server.
 */
public final class SimulatedRestDestination implements EventDestination {

    private final List<Object> received = new CopyOnWriteArrayList<>();

    @Override
    public void sendBatch(List<Object> records) {
        if (records != null) {
            received.addAll(records);
        }
    }

    @Override
    public void close() {
    }

    public List<Object> getReceived() {
        return new ArrayList<>(received);
    }

    public int getReceivedCount() {
        return received.size();
    }

    /**
     * Use when destination is "rest" and rest_url is missing or is a well-known simulation URL.
     */
    public static boolean isSimulationUrl(String url) {
        return url == null || url.isBlank()
                || "http://simulate".equals(url)
                || url.startsWith("http://localhost:0/");
    }
}
