package com.etl.report;

import com.etl.core.ETLConfig;
import com.etl.core.PipelineResult;
import com.mysql.cj.jdbc.Driver;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class Reporter {
    private static final DateTimeFormatter TS_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z").withZone(ZoneId.systemDefault());

    public static void printReport(ETLConfig config, PipelineResult result) throws Exception {
        try (Connection conn = openConnection(config)) {
            printRunSummary(conn, config);
            for (String q : new String[]{"daily_traffic", "top_resources", "hourly_errors"}) {
                if (config.runsQuery(q)) {
                    printQueryResults(conn, config, q);
                }
            }
        }
    }

    private static void printRunSummary(Connection conn, ETLConfig config) throws Exception {
        String sql = "SELECT pipeline, batch_size, total_records, total_batches, malformed_count, runtime_ms, executed_at " +
                "FROM etl_runs WHERE run_id = ?";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, config.getRunId().toString());

            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalStateException("No stored run metadata found for run_id " + config.getRunId());
                }

                String pipeline = rs.getString("pipeline");
                int batchSize = rs.getInt("batch_size");
                long totalRecords = rs.getLong("total_records");
                int totalBatches = rs.getInt("total_batches");
                long malformedCount = rs.getLong("malformed_count");
                long runtimeMs = rs.getLong("runtime_ms");
                Timestamp executedAt = rs.getTimestamp("executed_at");
                double avgBatchSize = totalBatches == 0 ? 0.0 : (double) totalRecords / totalBatches;

                System.out.println("=================================================");
                System.out.println("ETL Run Report");
                System.out.println("=================================================");
                System.out.println("Pipeline: " + pipeline);
                System.out.println("Run ID: " + config.getRunId());
                System.out.println("Executed At: " + TS_FMT.format(executedAt.toInstant()));
                System.out.println("Runtime: " + runtimeMs + " ms (" + String.format("%.2f", runtimeMs / 1000.0) + " seconds)");
                System.out.println("Batch Size: " + batchSize);
                System.out.println("Total Records: " + totalRecords);
                System.out.println("Malformed Records: " + malformedCount);
                System.out.println("Total Batches: " + totalBatches);
                System.out.println("Average Batch Size: " + String.format("%.2f", avgBatchSize));
                System.out.println("=================================================\n");
            }
        }
    }

    private static void printQueryResults(Connection conn, ETLConfig config, String queryName) throws Exception {
        String sql = "SELECT batch_id, log_date, status_code, resource_path, log_hour, request_count, total_bytes, " +
                "distinct_hosts, error_request_count, total_request_count, error_rate, distinct_error_hosts " +
                "FROM etl_results WHERE run_id = ? AND query_name = ? ORDER BY batch_id, id";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, config.getRunId().toString());
            ps.setString(2, queryName);

            try (ResultSet rs = ps.executeQuery()) {
                System.out.println("Query Results: " + queryName);
                System.out.println("-------------------------------------------------");

                if ("daily_traffic".equals(queryName)) {
                    System.out.println("BatchId\tLogDate\tStatusCode\tRequestCount\tTotalBytes");
                    while (rs.next()) {
                        System.out.println(rs.getInt("batch_id") + "\t" +
                                rs.getDate("log_date") + "\t" +
                                rs.getInt("status_code") + "\t" +
                                rs.getLong("request_count") + "\t" +
                                rs.getLong("total_bytes"));
                    }
                } else if ("top_resources".equals(queryName)) {
                    System.out.println("BatchId\tResourcePath\tRequestCount\tTotalBytes\tDistinctHosts");
                    while (rs.next()) {
                        System.out.println(rs.getInt("batch_id") + "\t" +
                                rs.getString("resource_path") + "\t" +
                                rs.getLong("request_count") + "\t" +
                                rs.getLong("total_bytes") + "\t" +
                                rs.getInt("distinct_hosts"));
                    }
                } else if ("hourly_errors".equals(queryName)) {
                    System.out.println("BatchId\tLogDate\tLogHour\tErrorRequests\tTotalRequests\tErrorRate\tDistinctErrorHosts");
                    while (rs.next()) {
                        System.out.println(rs.getInt("batch_id") + "\t" +
                                rs.getDate("log_date") + "\t" +
                                rs.getInt("log_hour") + "\t" +
                                rs.getInt("error_request_count") + "\t" +
                                rs.getInt("total_request_count") + "\t" +
                                String.format("%.4f", rs.getDouble("error_rate")) + "\t" +
                                rs.getInt("distinct_error_hosts"));
                    }
                }

                System.out.println();
            }
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
