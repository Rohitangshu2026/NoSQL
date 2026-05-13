package com.etl.db;

import com.etl.core.ETLConfig;
import com.etl.core.PipelineResult;
import com.etl.core.ResultRow;
import com.mysql.cj.jdbc.Driver;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Map;
import java.util.Properties;

public class ResultLoader {
    public static void load(ETLConfig config, PipelineResult result) throws Exception {
        try (Connection conn = openConnection(config)) {
            conn.setAutoCommit(false);

            String insertRun = "INSERT INTO etl_runs " +
                    "(run_id, pipeline, query_name, batch_size, total_records, total_batches, avg_batch_size, malformed_count, runtime_ms) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
            try (PreparedStatement ps = conn.prepareStatement(insertRun)) {
                ps.setString(1, config.getRunId().toString());
                ps.setString(2, config.getPipelineName());
                ps.setString(3, config.getSelectedQuery());
                ps.setInt(4,    config.getBatchSize());
                ps.setLong(5,   result.getTotalRecords());
                ps.setInt(6,    result.getTotalBatches());
                ps.setDouble(7, result.avgBatchSize());
                ps.setLong(8,   result.getMalformedCount());
                ps.setLong(9,   result.getRuntimeMs());
                ps.executeUpdate();
            }

            // Per-batch metadata (best-effort: only filled in when the pipeline reports it).
            if (!result.getRecordsPerBatch().isEmpty()) {
                String insertBatch = "INSERT INTO etl_batches " +
                        "(run_id, batch_id, records_in_batch, malformed_in_batch) VALUES (?, ?, ?, ?)";
                try (PreparedStatement ps = conn.prepareStatement(insertBatch)) {
                    for (Map.Entry<Integer, Integer> e : result.getRecordsPerBatch().entrySet()) {
                        int batchId = e.getKey();
                        int records = e.getValue() == null ? 0 : e.getValue();
                        Integer mal = result.getMalformedPerBatch().get(batchId);
                        ps.setString(1, config.getRunId().toString());
                        ps.setInt(2, batchId);
                        ps.setInt(3, records);
                        ps.setInt(4, mal == null ? 0 : mal);
                        ps.addBatch();
                    }
                    ps.executeBatch();
                }
            }

            Timestamp executedAt = Timestamp.from(result.getFinishedAt());
            String insertResult = "INSERT INTO etl_results " +
                    "(pipeline, run_id, batch_id, executed_at, query_name, log_date, status_code, resource_path, log_hour, " +
                    " request_count, total_bytes, distinct_hosts, error_request_count, total_request_count, error_rate, distinct_error_hosts) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            try (PreparedStatement ps = conn.prepareStatement(insertResult)) {
                for (ResultRow row : result.getRows()) {
                    ps.setString(1, config.getPipelineName());
                    ps.setString(2, config.getRunId().toString());
                    ps.setInt(3, row.getBatchId());
                    ps.setTimestamp(4, executedAt);
                    ps.setString(5, row.getQueryName());

                    if (row.getLogDate() != null) ps.setDate(6, row.getLogDate()); else ps.setNull(6, Types.DATE);
                    if (row.getStatusCode() != null) ps.setInt(7, row.getStatusCode()); else ps.setNull(7, Types.INTEGER);
                    if (row.getResourcePath() != null) ps.setString(8, row.getResourcePath()); else ps.setNull(8, Types.VARCHAR);
                    if (row.getLogHour() != null) ps.setInt(9, row.getLogHour()); else ps.setNull(9, Types.INTEGER);
                    if (row.getRequestCount() != null) ps.setLong(10, row.getRequestCount()); else ps.setNull(10, Types.BIGINT);
                    if (row.getTotalBytes() != null) ps.setLong(11, row.getTotalBytes()); else ps.setNull(11, Types.BIGINT);
                    if (row.getDistinctHosts() != null) ps.setInt(12, row.getDistinctHosts()); else ps.setNull(12, Types.INTEGER);
                    if (row.getErrorRequestCount() != null) ps.setInt(13, row.getErrorRequestCount()); else ps.setNull(13, Types.INTEGER);
                    if (row.getTotalRequestCount() != null) ps.setInt(14, row.getTotalRequestCount()); else ps.setNull(14, Types.INTEGER);
                    if (row.getErrorRate() != null) ps.setDouble(15, row.getErrorRate()); else ps.setNull(15, Types.DECIMAL);
                    if (row.getDistinctErrorHosts() != null) ps.setInt(16, row.getDistinctErrorHosts()); else ps.setNull(16, Types.INTEGER);

                    ps.addBatch();
                }
                ps.executeBatch();
            }
            conn.commit();
        }
    }

    public static void updateExecutionMetadata(ETLConfig config, Timestamp executedAt, long runtimeMs) throws Exception {
        try (Connection conn = openConnection(config)) {
            conn.setAutoCommit(false);

            try (PreparedStatement ps = conn.prepareStatement(
                    "UPDATE etl_runs SET runtime_ms = ?, executed_at = ? WHERE run_id = ?")) {
                ps.setLong(1, runtimeMs);
                ps.setTimestamp(2, executedAt);
                ps.setString(3, config.getRunId().toString());
                ps.executeUpdate();
            }

            try (PreparedStatement ps = conn.prepareStatement(
                    "UPDATE etl_results SET executed_at = ? WHERE run_id = ?")) {
                ps.setTimestamp(1, executedAt);
                ps.setString(2, config.getRunId().toString());
                ps.executeUpdate();
            }

            conn.commit();
        }
    }

    private static Connection openConnection(ETLConfig config) throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", config.getDbUser());
        props.setProperty("password", config.getDbPassword());

        Connection conn = new Driver().connect(config.getDbUrl(), props);
        if (conn == null) {
            throw new SQLException("MySQL driver did not accept JDBC URL: " + config.getDbUrl());
        }
        return conn;
    }
}
