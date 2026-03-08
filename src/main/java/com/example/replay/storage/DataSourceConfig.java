package com.example.replay.storage;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;

import javax.sql.DataSource;
import java.util.Optional;

/**
 * Configures HikariCP connection pool and runs Flyway migrations.
 * Use REPLAY_JDBC_URL (and optionally REPLAY_JDBC_USER, REPLAY_JDBC_PASSWORD) for PostgreSQL.
 */
public final class DataSourceConfig {

    private static final String ENV_JDBC_URL = "REPLAY_JDBC_URL";
    private static final String ENV_JDBC_USER = "REPLAY_JDBC_USER";
    private static final String ENV_JDBC_PASSWORD = "REPLAY_JDBC_PASSWORD";
    private static final String DEFAULT_JDBC_URL = "jdbc:postgresql://localhost:5432/replay";

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

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        Optional.ofNullable(System.getenv(ENV_JDBC_USER))
                .or(() -> Optional.ofNullable(System.getProperty("replay.jdbc.user")))
                .ifPresent(config::setUsername);
        Optional.ofNullable(System.getenv(ENV_JDBC_PASSWORD))
                .or(() -> Optional.ofNullable(System.getProperty("replay.jdbc.password")))
                .ifPresent(config::setPassword);
        config.setPoolName("replay-pool");
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(2);

        HikariDataSource ds = new HikariDataSource(config);

        Flyway flyway = Flyway.configure()
                .dataSource(ds)
                .locations("classpath:db/migration")
                .load();
        flyway.migrate();

        return Optional.of(ds);
    }

    /**
     * Creates DataSource with the given URL (e.g. for tests). Runs migrations.
     */
    public static DataSource createAndMigrate(String jdbcUrl, String username, String password) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        if (username != null) config.setUsername(username);
        if (password != null) config.setPassword(password);
        config.setPoolName("replay-pool");
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(1);

        HikariDataSource ds = new HikariDataSource(config);

        Flyway flyway = Flyway.configure()
                .dataSource(ds)
                .locations("classpath:db/migration")
                .load();
        flyway.migrate();

        return ds;
    }
}
