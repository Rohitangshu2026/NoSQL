package com.etl.db;

import com.etl.core.ETLConfig;
import com.etl.core.PipelineResult;
import com.etl.core.ResultRow;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Types;

public class ResultLoader {
    public static void load(ETLConfig config, PipelineResult result) throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        try (Connection conn = DriverManager.getConnection(config.getDbUrl(), config.getDbUser(), config.getDbPassword())) {
            conn.setAutoCommit(false);
            
            String insertRun = "INSERT INTO etl_runs (run_id, pipeline, batch_size, total_records, total_batches, malformed_count, runtime_ms) VALUES (?, ?, ?, ?, ?, ?, ?)";
            try (PreparedStatement ps = conn.prepareStatement(insertRun)) {
                ps.setString(1, config.getRunId().toString());
                ps.setString(2, config.getPipelineName());
                ps.setInt(3, config.getBatchSize());
                ps.setLong(4, result.getTotalRecords());
                ps.setInt(5, result.getTotalBatches());
                ps.setLong(6, result.getMalformedCount());
                ps.setLong(7, result.getRuntimeMs());
                ps.executeUpdate();
            }

            String insertResult = "INSERT INTO etl_results (run_id, batch_id, query_name, log_date, status_code, resource_path, log_hour, request_count, total_bytes, distinct_hosts, error_request_count, total_request_count, error_rate, distinct_error_hosts) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            try (PreparedStatement ps = conn.prepareStatement(insertResult)) {
                for (ResultRow row : result.getRows()) {
                    ps.setString(1, config.getRunId().toString());
                    ps.setInt(2, row.getBatchId());
                    ps.setString(3, row.getQueryName());

                    if (row.getLogDate() != null) ps.setDate(4, row.getLogDate()); else ps.setNull(4, Types.DATE);
                    if (row.getStatusCode() != null) ps.setInt(5, row.getStatusCode()); else ps.setNull(5, Types.INTEGER);
                    if (row.getResourcePath() != null) ps.setString(6, row.getResourcePath()); else ps.setNull(6, Types.VARCHAR);
                    if (row.getLogHour() != null) ps.setInt(7, row.getLogHour()); else ps.setNull(7, Types.INTEGER);
                    if (row.getRequestCount() != null) ps.setLong(8, row.getRequestCount()); else ps.setNull(8, Types.BIGINT);
                    if (row.getTotalBytes() != null) ps.setLong(9, row.getTotalBytes()); else ps.setNull(9, Types.BIGINT);
                    if (row.getDistinctHosts() != null) ps.setInt(10, row.getDistinctHosts()); else ps.setNull(10, Types.INTEGER);
                    if (row.getErrorRequestCount() != null) ps.setInt(11, row.getErrorRequestCount()); else ps.setNull(11, Types.INTEGER);
                    if (row.getTotalRequestCount() != null) ps.setInt(12, row.getTotalRequestCount()); else ps.setNull(12, Types.INTEGER);
                    if (row.getErrorRate() != null) ps.setDouble(13, row.getErrorRate()); else ps.setNull(13, Types.DECIMAL);
                    if (row.getDistinctErrorHosts() != null) ps.setInt(14, row.getDistinctErrorHosts()); else ps.setNull(14, Types.INTEGER);
                    
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            conn.commit();
        }
    }
}
