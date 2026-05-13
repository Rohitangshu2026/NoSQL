package com.etl.pipeline.hive;

import com.etl.core.ETLConfig;
import com.etl.core.LogParser;
import com.etl.core.LogRecord;
import com.etl.core.Pipeline;
import com.etl.core.PipelineResult;
import com.etl.core.ResultRow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * Hive-backed ETL pipeline.
 *
 * Data processing happens entirely through HiveQL executed against a running
 * HiveServer2 (default jdbc:hive2://localhost:10000/default). The pipeline:
 *   1. Parses raw NASA logs into TSV-formatted batch files (batchSize lines each).
 *   2. Creates a per-run external Hive table over the batch directory.
 *   3. Runs the three mandatory queries as HiveQL aggregations.
 *   4. Drops the external table on completion.
 */
public class HivePipeline implements Pipeline {

    private static final String DRIVER_CLASS = "org.apache.hive.jdbc.HiveDriver";

    @Override
    public PipelineResult execute(ETLConfig config) throws Exception {
        Instant startedAt = Instant.now();
        long startTime = System.currentTimeMillis();

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");

        // 1. Parse raw logs into TSV batches that Hive can LOAD.
        BatchPrep prep = prepareBatches(
                config.getInputPath(), config.getBatchSize(),
                config.getRunId().toString(), conf);

        long totalRecords = prep.totalRecords;
        long malformedCount = prep.malformedRecords;
        int  totalBatches = prep.batchFiles.size();
        List<ResultRow> rows = new ArrayList<>();

        // 2. Open a HiveServer2 JDBC connection.
        try {
            Class.forName(DRIVER_CLASS);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(
                    "Hive JDBC driver not on classpath. Did the fat-jar build include hive-jdbc?", e);
        }

        String tableName = "etl_logs_" + config.getRunId().toString().replace("-", "");

        try (Connection conn = DriverManager.getConnection(
                config.getHiveJdbcUrl(),
                config.getHiveUser() == null ? "" : config.getHiveUser(),
                config.getHivePassword() == null ? "" : config.getHivePassword());
             Statement stmt = conn.createStatement()) {

            stmt.execute("DROP TABLE IF EXISTS " + tableName);
            String ddl =
                    "CREATE EXTERNAL TABLE " + tableName + " (\n" +
                    "  host          STRING,\n" +
                    "  log_date      STRING,\n" +
                    "  log_hour      INT,\n" +
                    "  method        STRING,\n" +
                    "  resource_path STRING,\n" +
                    "  protocol      STRING,\n" +
                    "  status_code   INT,\n" +
                    "  bytes         BIGINT\n" +
                    ")\n" +
                    "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'\n" +
                    "STORED AS TEXTFILE\n" +
                    "LOCATION '" + prep.batchDir.toUri().toString() + "'";
            stmt.execute(ddl);

            // Run the requested queries.
            try {
                if (config.runsQuery("daily_traffic")) {
                    rows.addAll(runDailyTraffic(stmt, tableName));
                }
                if (config.runsQuery("top_resources")) {
                    rows.addAll(runTopResources(stmt, tableName));
                }
                if (config.runsQuery("hourly_errors")) {
                    rows.addAll(runHourlyErrors(stmt, tableName));
                }
            } finally {
                try { stmt.execute("DROP TABLE IF EXISTS " + tableName); } catch (Exception ignored) {}
            }
        }

        long runtimeMs = System.currentTimeMillis() - startTime;
        return new PipelineResult(totalRecords, totalBatches, malformedCount,
                runtimeMs, startedAt, Instant.now(), rows);
    }

    // -----------------------------------------------------------------------
    // Query 1 — Daily Traffic Summary
    // -----------------------------------------------------------------------
    private List<ResultRow> runDailyTraffic(Statement stmt, String table) throws Exception {
        String q =
                "SELECT log_date, status_code, COUNT(*) AS request_count, SUM(bytes) AS total_bytes\n" +
                "FROM " + table + "\n" +
                "WHERE log_date IS NOT NULL AND status_code IS NOT NULL\n" +
                "GROUP BY log_date, status_code\n" +
                "ORDER BY log_date, status_code";
        List<ResultRow> out = new ArrayList<>();
        try (ResultSet rs = stmt.executeQuery(q)) {
            while (rs.next()) {
                ResultRow row = new ResultRow(1, "daily_traffic");
                row.setLogDate(Date.valueOf(rs.getString(1)));
                row.setStatusCode(rs.getInt(2));
                row.setRequestCount(rs.getLong(3));
                row.setTotalBytes(rs.getLong(4));
                out.add(row);
            }
        }
        return out;
    }

    // -----------------------------------------------------------------------
    // Query 2 — Top 20 Requested Resources
    // -----------------------------------------------------------------------
    private List<ResultRow> runTopResources(Statement stmt, String table) throws Exception {
        String q =
                "SELECT resource_path, COUNT(*) AS request_count, SUM(bytes) AS total_bytes, " +
                "       COUNT(DISTINCT host) AS distinct_hosts\n" +
                "FROM " + table + "\n" +
                "WHERE resource_path IS NOT NULL\n" +
                "GROUP BY resource_path\n" +
                "ORDER BY request_count DESC\n" +
                "LIMIT 20";
        List<ResultRow> out = new ArrayList<>();
        try (ResultSet rs = stmt.executeQuery(q)) {
            while (rs.next()) {
                ResultRow row = new ResultRow(1, "top_resources");
                row.setResourcePath(rs.getString(1));
                row.setRequestCount(rs.getLong(2));
                row.setTotalBytes(rs.getLong(3));
                row.setDistinctHosts(rs.getInt(4));
                out.add(row);
            }
        }
        return out;
    }

    // -----------------------------------------------------------------------
    // Query 3 — Hourly Error Analysis
    // -----------------------------------------------------------------------
    private List<ResultRow> runHourlyErrors(Statement stmt, String table) throws Exception {
        String q =
                "SELECT log_date,\n" +
                "       log_hour,\n" +
                "       SUM(CASE WHEN status_code BETWEEN 400 AND 599 THEN 1 ELSE 0 END) AS error_request_count,\n" +
                "       COUNT(*) AS total_request_count,\n" +
                "       (SUM(CASE WHEN status_code BETWEEN 400 AND 599 THEN 1 ELSE 0 END) / COUNT(*)) AS error_rate,\n" +
                "       COUNT(DISTINCT CASE WHEN status_code BETWEEN 400 AND 599 THEN host END) AS distinct_error_hosts\n" +
                "FROM " + table + "\n" +
                "WHERE log_date IS NOT NULL AND log_hour IS NOT NULL\n" +
                "GROUP BY log_date, log_hour\n" +
                "ORDER BY log_date, log_hour";
        List<ResultRow> out = new ArrayList<>();
        try (ResultSet rs = stmt.executeQuery(q)) {
            while (rs.next()) {
                ResultRow row = new ResultRow(1, "hourly_errors");
                row.setLogDate(Date.valueOf(rs.getString(1)));
                row.setLogHour(rs.getInt(2));
                row.setErrorRequestCount(rs.getInt(3));
                row.setTotalRequestCount(rs.getInt(4));
                row.setErrorRate(rs.getDouble(5));
                row.setDistinctErrorHosts(rs.getInt(6));
                out.add(row);
            }
        }
        return out;
    }

    // -----------------------------------------------------------------------
    // Read raw logs, parse with the shared LogParser, write parsed records to
    // tab-separated batch files of size <= batchSize lines each.
    // -----------------------------------------------------------------------
    private BatchPrep prepareBatches(String inputPath, int batchSize, String runId,
                                     Configuration conf) throws Exception {
        Path rootPath = new Path(inputPath);
        FileSystem inputFs = rootPath.getFileSystem(conf);
        FileSystem localFs = FileSystem.getLocal(conf);
        Path batchDir = new Path("/tmp/etl_hive_batches/" + runId);

        if (localFs.exists(batchDir)) localFs.delete(batchDir, true);
        localFs.mkdirs(batchDir);

        List<Path> inputFiles = collectInputFiles(inputFs, rootPath);
        List<Path> batchFiles = new ArrayList<>();

        long total = 0;
        long malformed = 0;
        int batchNumber = 1;
        int currentBatchSize = 0;
        BufferedWriter writer = null;

        try {
            for (Path inputFile : inputFiles) {
                try (BufferedReader br = new BufferedReader(
                        new InputStreamReader(inputFs.open(inputFile)))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        total++;
                        Optional<LogRecord> parsed = LogParser.parse(line);
                        if (!parsed.isPresent()) {
                            malformed++;
                            continue;
                        }
                        LogRecord r = parsed.get();

                        if (writer == null || currentBatchSize >= batchSize) {
                            if (writer != null) writer.close();
                            Path bf = new Path(batchDir,
                                    String.format("batch-%05d.tsv", batchNumber));
                            FSDataOutputStream out = localFs.create(bf, true);
                            writer = new BufferedWriter(new OutputStreamWriter(out));
                            batchFiles.add(bf);
                            batchNumber++;
                            currentBatchSize = 0;
                        }
                        writer.write(serialize(r));
                        writer.newLine();
                        currentBatchSize++;
                    }
                }
            }
        } finally {
            if (writer != null) writer.close();
        }

        return new BatchPrep(batchDir, batchFiles, total, malformed);
    }

    private static String serialize(LogRecord r) {
        StringBuilder sb = new StringBuilder(96);
        sb.append(safe(r.getHost())).append('\t')
          .append(r.getLogDate()).append('\t')
          .append(r.getLogHour()).append('\t')
          .append(safe(r.getMethod())).append('\t')
          .append(safe(r.getResourcePath())).append('\t')
          .append(safe(r.getProtocol())).append('\t')
          .append(r.getStatusCode()).append('\t')
          .append(r.getBytes());
        return sb.toString();
    }

    private static String safe(String s) {
        if (s == null) return "";
        // Strip tabs/newlines so the TSV layout stays one record per line.
        return s.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ');
    }

    private List<Path> collectInputFiles(FileSystem fs, Path rootPath) throws Exception {
        List<Path> inputFiles = new ArrayList<>();
        if (fs.isFile(rootPath)) {
            inputFiles.add(rootPath);
            return inputFiles;
        }
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(rootPath, true);
        while (files.hasNext()) {
            LocatedFileStatus file = files.next();
            if (!file.isFile()) continue;
            String name = file.getPath().getName();
            if (name.startsWith("_") || name.startsWith(".")) continue;
            inputFiles.add(file.getPath());
        }
        Collections.sort(inputFiles, Comparator.comparing(Path::toString));
        return inputFiles;
    }

    private static final class BatchPrep {
        final Path batchDir;
        final List<Path> batchFiles;
        final long totalRecords;
        final long malformedRecords;

        BatchPrep(Path batchDir, List<Path> batchFiles, long totalRecords, long malformedRecords) {
            this.batchDir = batchDir;
            this.batchFiles = batchFiles;
            this.totalRecords = totalRecords;
            this.malformedRecords = malformedRecords;
        }
    }
}
