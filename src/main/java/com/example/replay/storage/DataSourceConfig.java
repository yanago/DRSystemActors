package com.example.replay.storage;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Optional;

/**
 * Configures HikariCP connection pool and runs Flyway migrations.
 * Use REPLAY_JDBC_URL (and optionally REPLAY_JDBC_USER, REPLAY_JDBC_PASSWORD) for PostgreSQL.
 */
public final class DataSourceConfig {

    private static final String ENV_JDBC_URL = "REPLAY_JDBC_URL";
    private static final String ENV_JDBC_USER = "REPLAY_JDBC_USER";
    private static final String ENV_JDBC_PASSWORD = "REPLAY_JDBC_PASSWORD";
    private static final String ENV_JDBC_CONNECT_RETRIES = "REPLAY_JDBC_CONNECT_RETRIES";
    private static final String ENV_JDBC_CONNECT_DELAY_MS = "REPLAY_JDBC_CONNECT_DELAY_MS";
    private static final String MIGRATION_RESOURCE = "db/migration/V1__create_replay_jobs_table.sql";
    private static final int DEFAULT_CONNECT_RETRIES = 15;
    private static final long DEFAULT_CONNECT_DELAY_MS = 2000L;

    private DataSourceConfig() {
    }

    /**
     * Creates a pooled DataSource and runs Flyway migrations. Returns empty if REPLAY_JDBC_URL is not set.
     */
    public static Optional<DataSource> createAndMigrate() {
        String jdbcUrl = Optional.ofNullable(System.getenv(ENV_JDBC_URL))
                .or(() -> Optional.ofNullable(System.getProperty("replay.jdbc.url")))
                .orElse(null);
        if (jdbcUrl == null || jdbcUrl.isBlank()) {
            return Optional.empty();
        }

        String username = Optional.ofNullable(System.getenv(ENV_JDBC_USER))
                .or(() -> Optional.ofNullable(System.getProperty("replay.jdbc.user")))
                .orElse(null);
        String password = Optional.ofNullable(System.getenv(ENV_JDBC_PASSWORD))
                .or(() -> Optional.ofNullable(System.getProperty("replay.jdbc.password")))
                .orElse(null);
        int maxAttempts = readIntSetting(ENV_JDBC_CONNECT_RETRIES, "replay.jdbc.connect.retries", DEFAULT_CONNECT_RETRIES);
        long delayMs = readLongSetting(ENV_JDBC_CONNECT_DELAY_MS, "replay.jdbc.connect.delay.ms", DEFAULT_CONNECT_DELAY_MS);

        RuntimeException lastFailure = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                return Optional.of(createAndMigrate(jdbcUrl, username, password));
            } catch (RuntimeException e) {
                lastFailure = e;
                if (attempt == maxAttempts) {
                    break;
                }
                System.err.printf(
                        "Postgres connection attempt %d/%d failed: %s. Retrying in %d ms.%n",
                        attempt, maxAttempts, e.getMessage(), delayMs);
                sleep(delayMs);
            }
        }

        throw lastFailure != null ? lastFailure : new RuntimeException("Failed to initialize PostgreSQL datasource");
    }

    /**
     * Creates DataSource with the given URL (e.g. for tests). Runs migrations.
     */
    public static DataSource createAndMigrate(String jdbcUrl, String username, String password) {
        HikariDataSource ds = null;
        try {
            ds = new HikariDataSource(buildConfig(jdbcUrl, username, password, 1));

            migrateSchema(ds);

            return ds;
        } catch (RuntimeException e) {
            if (ds != null) {
                ds.close();
            }
            throw e;
        }
    }

    private static HikariConfig buildConfig(String jdbcUrl, String username, String password, int minimumIdle) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        if (username != null) {
            config.setUsername(username);
        }
        if (password != null) {
            config.setPassword(password);
        }
        config.setPoolName("replay-pool");
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(minimumIdle);
        config.setConnectionTimeout(5000);
        config.setInitializationFailTimeout(5000);
        return config;
    }

    private static void migrateSchema(HikariDataSource ds) {
        try {
            Flyway flyway = Flyway.configure()
                    .dataSource(ds)
                    .locations("classpath:db/migration")
                    .load();
            flyway.migrate();
        } catch (RuntimeException e) {
            if (!isUnsupportedDatabaseError(e)) {
                throw e;
            }
            System.err.println("Flyway does not recognize this PostgreSQL version; applying bundled schema SQL directly.");
            applyBundledSchema(ds);
        }
    }

    private static boolean isUnsupportedDatabaseError(RuntimeException e) {
        String message = e.getMessage();
        return message != null && message.contains("Unsupported Database");
    }

    private static void applyBundledSchema(DataSource ds) {
        String sql = readMigrationResource();
        try (Connection connection = ds.getConnection();
             Statement statement = connection.createStatement()) {
            for (String part : sql.split(";")) {
                String trimmed = stripSqlComments(part).trim();
                if (!trimmed.isEmpty()) {
                    statement.execute(trimmed);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to apply bundled PostgreSQL schema", e);
        }
    }

    private static String readMigrationResource() {
        try (InputStream input = DataSourceConfig.class.getClassLoader().getResourceAsStream(MIGRATION_RESOURCE)) {
            if (input == null) {
                throw new RuntimeException("Migration resource not found: " + MIGRATION_RESOURCE);
            }
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read migration resource: " + MIGRATION_RESOURCE, e);
        }
    }

    private static String stripSqlComments(String sql) {
        return Arrays.stream(sql.split("\\R"))
                .map(String::trim)
                .filter(line -> !line.startsWith("--"))
                .reduce("", (left, right) -> left.isEmpty() ? right : left + System.lineSeparator() + right);
    }

    private static int readIntSetting(String envName, String propertyName, int defaultValue) {
        String value = Optional.ofNullable(System.getenv(envName))
                .or(() -> Optional.ofNullable(System.getProperty(propertyName)))
                .orElse(null);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException ignored) {
            return defaultValue;
        }
    }

    private static long readLongSetting(String envName, String propertyName, long defaultValue) {
        String value = Optional.ofNullable(System.getenv(envName))
                .or(() -> Optional.ofNullable(System.getProperty(propertyName)))
                .orElse(null);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException ignored) {
            return defaultValue;
        }
    }

    private static void sleep(long delayMs) {
        try {
            Thread.sleep(delayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for PostgreSQL", e);
        }
    }
}
