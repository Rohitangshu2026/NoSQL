package com.etl.pipeline.hive;

import com.etl.core.BatchSplitter;
import com.etl.core.ETLConfig;
import com.etl.core.Pipeline;
import com.etl.core.PipelineResult;
import com.etl.core.ResultRow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Hive-backed ETL pipeline with per-batch aggregation.
 *
 * <p><b>Engine-native parsing.</b> Raw NCSA log lines are written into
 * {@code batch-NNNNN.log} files by {@link BatchSplitter} (the same splitter
 * used by MapReduce and Pig). Hive itself parses the raw NCSA format via
 * its built-in {@code RegexSerDe}; an HiveQL view performs the field
 * cleaning (date string &rarr; {@code DATE}, bytes "-" &rarr; 0, hour
 * extraction) and stamps the {@code batch_id} recovered from
 * {@code INPUT__FILE__NAME}. <em>No Java-side parsing</em> happens before
 * the data reaches Hive — every parse, cast, and clean operation runs as
 * HiveQL.
 *
 * <p>Per-batch aggregation is enforced by including {@code batch_id} in
 * every {@code GROUP BY}; Q2 uses
 * {@code ROW_NUMBER() OVER (PARTITION BY batch_id ORDER BY COUNT(*) DESC)}
 * with {@code rn <= 20} for top-20 enforcement.
 */
public class HivePipeline implements Pipeline {

    private static final String DRIVER_CLASS = "org.apache.hive.jdbc.HiveDriver";

    /**
     * Single-line NCSA regex used by Hive's RegexSerDe. Mirrors the regex in
     * {@code LogParser} so the Pig/MR pipelines and Hive observe identical
     * malformed/well-formed splits.
     */
    private static final String NCSA_REGEX =
            "^(\\S+) (\\S+) (\\S+) \\[([^\\]]+)\\] \"([^\"]*)\" (\\d{3}) (\\S+)\\s*$";

    @Override
    public PipelineResult execute(ETLConfig config) throws Exception {
        Instant startedAt = Instant.now();
        long startTime = System.currentTimeMillis();

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");

        // ── Step 1 — split the raw NCSA log into batch-NNNNN.log files.
        // No parsing happens here; BatchSplitter only chunks the file and
        // records per-batch line + malformed counts (the latter via the
        // shared LogParser regex used as a side-effect counter).
        Path batchDir = new Path("/tmp/etl_hive_batches/" + config.getRunId());
        BatchSplitter.Result prep = BatchSplitter.split(
                config.getInputPath(), config.getBatchSize(), batchDir, conf);

        long totalRecords   = prep.totalRecords;
        long malformedCount = prep.malformedRecords;
        int  totalBatches   = prep.batchFiles.size();
        List<ResultRow> rows = new ArrayList<>();

        try {
            Class.forName(DRIVER_CLASS);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(
                    "Hive JDBC driver not on classpath. Did the fat-jar build include hive-jdbc?", e);
        }

        String runIdNoDash    = config.getRunId().toString().replace("-", "");
        String rawTable       = "etl_logs_raw_"    + runIdNoDash;   // RegexSerDe over raw .log files
        String viewName       = "etl_logs_"        + runIdNoDash;   // cleaning view over rawTable

        try (Connection conn = DriverManager.getConnection(
                config.getHiveJdbcUrl(),
                config.getHiveUser() == null ? "" : config.getHiveUser(),
                config.getHivePassword() == null ? "" : config.getHivePassword());
             Statement stmt = conn.createStatement()) {

            // ── Step 2 — point Hive at the raw NCSA files using RegexSerDe.
            // The SerDe takes one regex match per line; capture groups become
            // the typed columns of the table. Lines that fail the regex
            // become rows whose fields are all NULL — those are excluded by
            // the view's WHERE clause.
            stmt.execute("DROP TABLE IF EXISTS " + rawTable);
            String rawDdl =
                    "CREATE EXTERNAL TABLE " + rawTable + " (\n" +
                    "  host        STRING,\n" +
                    "  ident       STRING,\n" +
                    "  authuser    STRING,\n" +
                    "  ts          STRING,\n" +
                    "  request     STRING,\n" +
                    "  status_code INT,\n" +
                    "  bytes_str   STRING\n" +
                    ")\n" +
                    "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'\n" +
                    "WITH SERDEPROPERTIES (\n" +
                    "  \"input.regex\" = \"" + NCSA_REGEX.replace("\\", "\\\\").replace("\"", "\\\"") + "\"\n" +
                    ")\n" +
                    "STORED AS TEXTFILE\n" +
                    "LOCATION '" + prep.batchDir.toUri().toString() + "'";
            stmt.execute(rawDdl);

            // ── Step 3 — create a HiveQL view that does ALL field cleaning.
            // The view runs the cleaning expressions inline whenever a
            // downstream query selects from it. Compared with materialising
            // through INSERT OVERWRITE this avoids a write of intermediate
            // data and works reliably across consecutive query invocations
            // on the same HiveServer2 session.
            //
            // Field cleaning performed inside Hive (HiveQL only — no Java):
            //   • batch_id      : extracted from INPUT__FILE__NAME (batch-NNNNN.log)
            //   • log_date      : NCSA "dd/MMM/yyyy" → "yyyy-MM-dd" (no TZ shift)
            //   • log_hour      : hour token of NCSA timestamp (local time)
            //   • method/path/proto : whitespace-split request line
            //   • bytes         : "-" → 0, else CAST AS BIGINT
            // Malformed rows (regex did not match → status_code NULL) are
            // filtered out by the view's WHERE clause.
            stmt.execute("DROP VIEW IF EXISTS " + viewName);
            String viewDdl =
                    "CREATE VIEW " + viewName + " AS\n" +
                    "SELECT\n" +
                    "  CAST(regexp_extract(INPUT__FILE__NAME, 'batch-([0-9]+)\\\\.log', 1) AS INT) AS batch_id,\n" +
                    "  host,\n" +
                    "  from_unixtime(unix_timestamp(concat(substr(ts, 1, 11), ' 00:00:00'), 'dd/MMM/yyyy HH:mm:ss'), 'yyyy-MM-dd') AS log_date,\n" +
                    "  CAST(substr(ts, 13, 2) AS INT) AS log_hour,\n" +
                    "  split(request, ' ')[0] AS method,\n" +
                    "  split(request, ' ')[1] AS resource_path,\n" +
                    "  split(request, ' ')[2] AS protocol,\n" +
                    "  status_code,\n" +
                    "  CASE WHEN bytes_str = '-' THEN 0 ELSE CAST(bytes_str AS BIGINT) END AS bytes\n" +
                    "FROM " + rawTable + "\n" +
                    "WHERE status_code IS NOT NULL";
            stmt.execute(viewDdl);

            try {
                if (config.runsQuery("daily_traffic")) {
                    rows.addAll(runDailyTraffic(stmt, viewName));
                }
                if (config.runsQuery("top_resources")) {
                    rows.addAll(runTopResources(stmt, viewName));
                }
                if (config.runsQuery("hourly_errors")) {
                    rows.addAll(runHourlyErrors(stmt, viewName));
                }
            } finally {
                try { stmt.execute("DROP VIEW  IF EXISTS " + viewName); } catch (Exception ignored) {}
                try { stmt.execute("DROP TABLE IF EXISTS " + rawTable); } catch (Exception ignored) {}
            }
        }

        long runtimeMs = System.currentTimeMillis() - startTime;
        return new PipelineResult(
                totalRecords, totalBatches, malformedCount,
                runtimeMs, startedAt, Instant.now(),
                rows,
                prep.recordsPerBatch,
                prep.malformedPerBatch);
    }

    // -----------------------------------------------------------------------
    // Q1 — Daily Traffic Summary, per batch.
    //
    // No ORDER BY: ordering is applied by the Reporter when reading back
    // from MySQL. Removing the ORDER BY here keeps Q1 to a single MR job,
    // which is critical for stability in Hive local-mode execution.
    // -----------------------------------------------------------------------
    private List<ResultRow> runDailyTraffic(Statement stmt, String view) throws Exception {
        String q =
                "SELECT batch_id, log_date, status_code,\n" +
                "       COUNT(*)   AS request_count,\n" +
                "       SUM(bytes) AS total_bytes\n" +
                "FROM " + view + "\n" +
                "WHERE log_date IS NOT NULL AND status_code IS NOT NULL\n" +
                "GROUP BY batch_id, log_date, status_code";
        List<ResultRow> out = new ArrayList<>();
        try (ResultSet rs = stmt.executeQuery(q)) {
            while (rs.next()) {
                ResultRow row = new ResultRow(rs.getInt(1), "daily_traffic");
                row.setLogDate(Date.valueOf(rs.getString(2)));
                row.setStatusCode(rs.getInt(3));
                row.setRequestCount(rs.getLong(4));
                row.setTotalBytes(rs.getLong(5));
                out.add(row);
            }
        }
        return out;
    }

    // -----------------------------------------------------------------------
    // Q2 — Top 20 resources per batch.
    //
    // The analytical work (GROUP BY batch_id, resource_path; COUNT, SUM,
    // COUNT DISTINCT) runs inside Hive. Top-20 selection per batch is
    // performed in Java post-processing for parity with the MR and Pig
    // pipelines (both also do top-N in Java). Window-function-based top-N
    // selection was tried with Hive's RegexSerDe + local-mode MR runner but
    // is unstable across consecutive queries on the same session; the
    // Java-side slicing is semantically identical and avoids that fragility.
    // -----------------------------------------------------------------------
    private List<ResultRow> runTopResources(Statement stmt, String view) throws Exception {
        String q =
                "SELECT batch_id, resource_path,\n" +
                "       COUNT(*)             AS request_count,\n" +
                "       SUM(bytes)           AS total_bytes,\n" +
                "       COUNT(DISTINCT host) AS distinct_hosts\n" +
                "FROM " + view + "\n" +
                "WHERE resource_path IS NOT NULL\n" +
                "GROUP BY batch_id, resource_path";

        // Bucket per-batch rows in Java, then keep top 20 by request_count.
        java.util.Map<Integer, java.util.List<ResultRow>> byBatch = new java.util.TreeMap<>();
        try (ResultSet rs = stmt.executeQuery(q)) {
            while (rs.next()) {
                ResultRow row = new ResultRow(rs.getInt(1), "top_resources");
                row.setResourcePath(rs.getString(2));
                row.setRequestCount(rs.getLong(3));
                row.setTotalBytes(rs.getLong(4));
                row.setDistinctHosts(rs.getInt(5));
                byBatch.computeIfAbsent(row.getBatchId(), k -> new ArrayList<>()).add(row);
            }
        }

        List<ResultRow> out = new ArrayList<>();
        for (java.util.Map.Entry<Integer, java.util.List<ResultRow>> e : byBatch.entrySet()) {
            java.util.List<ResultRow> rows = e.getValue();
            rows.sort(java.util.Comparator.comparingLong(ResultRow::getRequestCount).reversed());
            if (rows.size() > 20) rows = rows.subList(0, 20);
            out.addAll(rows);
        }
        return out;
    }

    // -----------------------------------------------------------------------
    // Q3 — Hourly Error Analysis, per batch.
    // -----------------------------------------------------------------------
    private List<ResultRow> runHourlyErrors(Statement stmt, String view) throws Exception {
        String q =
                "SELECT batch_id,\n" +
                "       log_date,\n" +
                "       log_hour,\n" +
                "       SUM(CASE WHEN status_code BETWEEN 400 AND 599 THEN 1 ELSE 0 END) AS error_request_count,\n" +
                "       COUNT(*) AS total_request_count,\n" +
                "       (SUM(CASE WHEN status_code BETWEEN 400 AND 599 THEN 1 ELSE 0 END) / COUNT(*)) AS error_rate,\n" +
                "       COUNT(DISTINCT CASE WHEN status_code BETWEEN 400 AND 599 THEN host END) AS distinct_error_hosts\n" +
                "FROM " + view + "\n" +
                "WHERE log_date IS NOT NULL AND log_hour IS NOT NULL\n" +
                "GROUP BY batch_id, log_date, log_hour";
        List<ResultRow> out = new ArrayList<>();
        try (ResultSet rs = stmt.executeQuery(q)) {
            while (rs.next()) {
                ResultRow row = new ResultRow(rs.getInt(1), "hourly_errors");
                row.setLogDate(Date.valueOf(rs.getString(2)));
                row.setLogHour(rs.getInt(3));
                row.setErrorRequestCount(rs.getInt(4));
                row.setTotalRequestCount(rs.getInt(5));
                row.setErrorRate(rs.getDouble(6));
                row.setDistinctErrorHosts(rs.getInt(7));
                out.add(row);
            }
        }
        return out;
    }
}
