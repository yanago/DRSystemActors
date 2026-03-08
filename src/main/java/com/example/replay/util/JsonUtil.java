package com.example.replay.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;

/**
 * JSON (de)serialization using Jackson. Thread-safe; uses a shared ObjectMapper
 * configured with Java 8 date/time support.
 */
public final class JsonUtil {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private JsonUtil() {
    }

    /**
     * Returns the shared ObjectMapper for custom use.
     */
    public static ObjectMapper mapper() {
        return MAPPER;
    }

    /**
     * Serializes value to JSON string.
     */
    public static String toJson(Object value) {
        if (value == null) {
            return "null";
        }
        try {
            return MAPPER.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Serializes value to JSON and write to the given output stream.
     */
    public static void toJson(OutputStream out, Object value) {
        try {
            MAPPER.writeValue(out, value);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Deserializes JSON string to the given class.
     */
    public static <T> T fromJson(String json, Class<T> type) {
        if (json == null || json.isBlank()) {
            return null;
        }
        try {
            return MAPPER.readValue(json, type);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Deserializes JSON string using a TypeReference (e.g. for generics).
     */
    public static <T> T fromJson(String json, TypeReference<T> typeRef) {
        if (json == null || json.isBlank()) {
            return null;
        }
        try {
            return MAPPER.readValue(json, typeRef);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Deserializes JSON from input stream to the given class.
     */
    public static <T> T fromJson(InputStream in, Class<T> type) {
        try {
            return MAPPER.readValue(in, type);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Deserializes JSON from input stream using a TypeReference.
     */
    public static <T> T fromJson(InputStream in, TypeReference<T> typeRef) {
        try {
            return MAPPER.readValue(in, typeRef);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
