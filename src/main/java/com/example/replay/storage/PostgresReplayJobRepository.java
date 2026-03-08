package com.example.replay.storage;

import com.example.replay.model.ReplayJob;
import com.example.replay.util.JsonUtil;
import com.fasterxml.jackson.core.type.TypeReference;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * PostgreSQL implementation of {@link ReplayJobRepository} using JDBC and connection pooling.
 */
public final class PostgresReplayJobRepository implements ReplayJobRepository {

    private static final String TABLE = "replay_jobs";
    private static final TypeReference<Map<String, Object>> PARAMS_TYPE = new TypeReference<>() {};

    private final DataSource dataSource;

    public PostgresReplayJobRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public ReplayJob save(ReplayJob job) {
        String sql = """
            INSERT INTO replay_jobs (job_id, status, parameters, created_at, updated_at, message)
            VALUES (?, ?, ?::jsonb, ?, ?, ?)
            ON CONFLICT (job_id) DO UPDATE SET
                status = EXCLUDED.status,
                parameters = EXCLUDED.parameters,
                updated_at = EXCLUDED.updated_at,
                message = EXCLUDED.message
            """;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, job.getJobId());
            ps.setString(2, job.getStatus().name());
            ps.setString(3, JsonUtil.toJson(job.getParameters()));
            ps.setTimestamp(4, job.getCreatedAt() != null ? Timestamp.from(job.getCreatedAt()) : null);
            ps.setTimestamp(5, job.getUpdatedAt() != null ? Timestamp.from(job.getUpdatedAt()) : null);
            ps.setString(6, job.getMessage());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to save replay job: " + job.getJobId(), e);
        }
        return job;
    }

    @Override
    public Optional<ReplayJob> findById(String jobId) {
        String sql = "SELECT job_id, status, parameters, created_at, updated_at, message FROM " + TABLE + " WHERE job_id = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, jobId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(mapRow(rs));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to find replay job: " + jobId, e);
        }
        return Optional.empty();
    }

    @Override
    public List<ReplayJob> findAll() {
        String sql = "SELECT job_id, status, parameters, created_at, updated_at, message FROM " + TABLE + " ORDER BY created_at DESC";
        List<ReplayJob> list = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                list.add(mapRow(rs));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to list replay jobs", e);
        }
        return list;
    }

    private static ReplayJob mapRow(ResultSet rs) throws SQLException {
        String jobId = rs.getString("job_id");
        String statusStr = rs.getString("status");
        ReplayJob.ReplayJobStatus status = ReplayJob.ReplayJobStatus.valueOf(statusStr);
        String paramsJson = rs.getString("parameters");
        Map<String, Object> parameters = paramsJson != null && !paramsJson.isBlank()
                ? JsonUtil.fromJson(paramsJson, PARAMS_TYPE)
                : Map.of();
        if (parameters == null) {
            parameters = Map.of();
        }
        Instant createdAt = toInstant(rs.getTimestamp("created_at"));
        Instant updatedAt = toInstant(rs.getTimestamp("updated_at"));
        String message = rs.getString("message");
        return new ReplayJob(jobId, status, parameters, createdAt, updatedAt, message);
    }

    private static Instant toInstant(Timestamp ts) {
        return ts != null ? ts.toInstant() : null;
    }
}
