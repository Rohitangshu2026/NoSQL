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

/**
 * Console reporter — reads the freshly-loaded run from MySQL and prints
 *   1) run-level metadata (pipeline, query, batch counts, runtime, etc.)
 *   2) per-batch row counts from etl_batches
 *   3) per-batch query results from etl_results
 *   4) a rolled-up global view computed in SQL over etl_results
 */
public class Reporter {
    private static final DateTimeFormatter TS_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z").withZone(ZoneId.systemDefault());

    public static void printReport(ETLConfig config, PipelineResult result) throws Exception {
        try (Connection conn = openConnection(config)) {
            printRunSummary(conn, config);
            printBatchSummary(conn, config);

            for (String q : new String[]{"daily_traffic", "top_resources", "hourly_errors"}) {
                if (!config.runsQuery(q)) continue;
                printPerBatchResults(conn, config, q);
                printGlobalRollup(conn, config, q);
            }
        }
    }

    private static void printRunSummary(Connection conn, ETLConfig config) throws Exception {
        String sql = "SELECT pipeline, query_name, batch_size, total_records, total_batches, " +
                "       avg_batch_size, malformed_count, runtime_ms, executed_at " +
                "FROM etl_runs WHERE run_id = ?";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, config.getRunId().toString());

            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalStateException("No stored run metadata for run_id " + config.getRunId());
                }
                String pipeline      = rs.getString("pipeline");
                String queryName     = rs.getString("query_name");
                int    batchSize     = rs.getInt("batch_size");
                long   totalRecords  = rs.getLong("total_records");
                int    totalBatches  = rs.getInt("total_batches");
                double avgBatch      = rs.getDouble("avg_batch_size");
                long   malformed     = rs.getLong("malformed_count");
                long   runtimeMs     = rs.getLong("runtime_ms");
                Timestamp executedAt = rs.getTimestamp("executed_at");

                System.out.println("=================================================");
                System.out.println("ETL Run Report");
                System.out.println("=================================================");
                System.out.println("Pipeline           : " + pipeline);
                System.out.println("Query              : " + queryName);
                System.out.println("Run ID             : " + config.getRunId());
                System.out.println("Executed At        : " + TS_FMT.format(executedAt.toInstant()));
                System.out.println("Runtime            : " + runtimeMs + " ms (" +
                        String.format("%.2f", runtimeMs / 1000.0) + " s)");
                System.out.println("Batch Size         : " + batchSize);
                System.out.println("Total Records      : " + totalRecords);
                System.out.println("Malformed Records  : " + malformed);
                System.out.println("Total Batches      : " + totalBatches);
                System.out.println("Average Batch Size : " + String.format("%.2f", avgBatch));
                System.out.println("=================================================\n");
            }
        }
    }

    private static void printBatchSummary(Connection conn, ETLConfig config) throws Exception {
        String sql = "SELECT batch_id, records_in_batch, malformed_in_batch " +
                "FROM etl_batches WHERE run_id = ? ORDER BY batch_id";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, config.getRunId().toString());
            try (ResultSet rs = ps.executeQuery()) {
                boolean any = false;
                StringBuilder body = new StringBuilder();
                body.append("BatchId\tRecords\tMalformed\n");
                while (rs.next()) {
                    any = true;
                    body.append(rs.getInt("batch_id")).append('\t')
                        .append(rs.getInt("records_in_batch")).append('\t')
                        .append(rs.getInt("malformed_in_batch")).append('\n');
                }
                if (any) {
                    System.out.println("Batch Summary");
                    System.out.println("-------------------------------------------------");
                    System.out.print(body);
                    System.out.println();
                }
            }
        }
    }

    private static void printPerBatchResults(Connection conn, ETLConfig config, String queryName) throws Exception {
        System.out.println("Per-Batch Results: " + queryName);
        System.out.println("-------------------------------------------------");

        String sql;
        if ("daily_traffic".equals(queryName)) {
            sql = "SELECT batch_id, log_date, status_code, request_count, total_bytes " +
                  "FROM etl_results WHERE run_id = ? AND query_name = ? " +
                  "ORDER BY batch_id, log_date, status_code";
        } else if ("top_resources".equals(queryName)) {
            sql = "SELECT batch_id, resource_path, request_count, total_bytes, distinct_hosts " +
                  "FROM etl_results WHERE run_id = ? AND query_name = ? " +
                  "ORDER BY batch_id, request_count DESC";
        } else {
            sql = "SELECT batch_id, log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts " +
                  "FROM etl_results WHERE run_id = ? AND query_name = ? " +
                  "ORDER BY batch_id, log_date, log_hour";
        }

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, config.getRunId().toString());
            ps.setString(2, queryName);
            try (ResultSet rs = ps.executeQuery()) {
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
                } else {
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

    /**
     * Compute the rolled-up "global" view by SUM-ing per-batch results.
     * For top_resources the global rollup is an approximation since per-batch
     * results only carry the top 20 of each batch — a resource that ranks 21st
     * in every batch would be missed. That caveat is printed below the table.
     */
    private static void printGlobalRollup(Connection conn, ETLConfig config, String queryName) throws Exception {
        System.out.println("Global Rollup: " + queryName);
        System.out.println("-------------------------------------------------");

        String sql;
        if ("daily_traffic".equals(queryName)) {
            sql = "SELECT log_date, status_code, SUM(request_count) AS rc, SUM(total_bytes) AS tb " +
                  "FROM etl_results WHERE run_id = ? AND query_name = ? " +
                  "GROUP BY log_date, status_code ORDER BY log_date, status_code";
        } else if ("top_resources".equals(queryName)) {
            sql = "SELECT resource_path, SUM(request_count) AS rc, SUM(total_bytes) AS tb, " +
                  "       SUM(distinct_hosts) AS dh " +
                  "FROM etl_results WHERE run_id = ? AND query_name = ? " +
                  "GROUP BY resource_path ORDER BY rc DESC LIMIT 20";
        } else {
            sql = "SELECT log_date, log_hour, SUM(error_request_count) AS erc, SUM(total_request_count) AS trc, " +
                  "       SUM(distinct_error_hosts) AS deh " +
                  "FROM etl_results WHERE run_id = ? AND query_name = ? " +
                  "GROUP BY log_date, log_hour ORDER BY log_date, log_hour";
        }

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, config.getRunId().toString());
            ps.setString(2, queryName);
            try (ResultSet rs = ps.executeQuery()) {
                if ("daily_traffic".equals(queryName)) {
                    System.out.println("LogDate\tStatusCode\tRequestCount\tTotalBytes");
                    while (rs.next()) {
                        System.out.println(rs.getDate("log_date") + "\t" +
                                rs.getInt("status_code") + "\t" +
                                rs.getLong("rc") + "\t" +
                                rs.getLong("tb"));
                    }
                } else if ("top_resources".equals(queryName)) {
                    System.out.println("ResourcePath\tRequestCount\tTotalBytes\tSumDistinctHosts");
                    while (rs.next()) {
                        System.out.println(rs.getString("resource_path") + "\t" +
                                rs.getLong("rc") + "\t" +
                                rs.getLong("tb") + "\t" +
                                rs.getLong("dh"));
                    }
                    System.out.println("(note: rollup is approximate — only per-batch top-20s are SUMmed)");
                } else {
                    System.out.println("LogDate\tLogHour\tErrorRequests\tTotalRequests\tErrorRate\tSumDistinctErrorHosts");
                    while (rs.next()) {
                        long erc = rs.getLong("erc");
                        long trc = rs.getLong("trc");
                        double rate = trc == 0 ? 0.0 : (double) erc / (double) trc;
                        System.out.println(rs.getDate("log_date") + "\t" +
                                rs.getInt("log_hour") + "\t" +
                                erc + "\t" +
                                trc + "\t" +
                                String.format("%.4f", rate) + "\t" +
                                rs.getLong("deh"));
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
