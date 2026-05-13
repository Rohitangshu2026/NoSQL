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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Hive-backed ETL pipeline (per-batch aggregation).
 *
 * NCSA logs are parsed in Java into TSV files of batchSize records each;
 * every record carries the batch id it belongs to as its first column. An
 * external Hive table is then created over the batch directory and the three
 * mandatory queries are executed as HiveQL aggregations that always
 * {@code GROUP BY batch_id} first.
 */
public class HivePipeline implements Pipeline {

    private static final String DRIVER_CLASS = "org.apache.hive.jdbc.HiveDriver";

    @Override
    public PipelineResult execute(ETLConfig config) throws Exception {
        Instant startedAt = Instant.now();
        long startTime = System.currentTimeMillis();

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");

        BatchPrep prep = prepareBatches(
                config.getInputPath(), config.getBatchSize(),
                config.getRunId().toString(), conf);

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

        String tableName = "etl_logs_" + config.getRunId().toString().replace("-", "");

        try (Connection conn = DriverManager.getConnection(
                config.getHiveJdbcUrl(),
                config.getHiveUser() == null ? "" : config.getHiveUser(),
                config.getHivePassword() == null ? "" : config.getHivePassword());
             Statement stmt = conn.createStatement()) {

            stmt.execute("DROP TABLE IF EXISTS " + tableName);
            String ddl =
                    "CREATE EXTERNAL TABLE " + tableName + " (\n" +
                    "  batch_id      INT,\n" +
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
        return new PipelineResult(
                totalRecords, totalBatches, malformedCount,
                runtimeMs, startedAt, Instant.now(),
                rows,
                prep.recordsPerBatch,
                prep.malformedPerBatch);
    }

    // -----------------------------------------------------------------------
    // Q1 — Daily Traffic Summary, per batch.
    // -----------------------------------------------------------------------
    private List<ResultRow> runDailyTraffic(Statement stmt, String table) throws Exception {
        String q =
                "SELECT batch_id, log_date, status_code, COUNT(*) AS request_count, SUM(bytes) AS total_bytes\n" +
                "FROM " + table + "\n" +
                "WHERE log_date IS NOT NULL AND status_code IS NOT NULL\n" +
                "GROUP BY batch_id, log_date, status_code\n" +
                "ORDER BY batch_id, log_date, status_code";
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
    // Q2 — Top 20 resources per batch via ROW_NUMBER() PARTITION BY batch_id.
    // -----------------------------------------------------------------------
    private List<ResultRow> runTopResources(Statement stmt, String table) throws Exception {
        String q =
                "SELECT batch_id, resource_path, request_count, total_bytes, distinct_hosts FROM (\n" +
                "  SELECT batch_id, resource_path,\n" +
                "         COUNT(*)            AS request_count,\n" +
                "         SUM(bytes)          AS total_bytes,\n" +
                "         COUNT(DISTINCT host) AS distinct_hosts,\n" +
                "         ROW_NUMBER() OVER (PARTITION BY batch_id ORDER BY COUNT(*) DESC) AS rn\n" +
                "  FROM " + table + "\n" +
                "  WHERE resource_path IS NOT NULL\n" +
                "  GROUP BY batch_id, resource_path\n" +
                ") t WHERE rn <= 20\n" +
                "ORDER BY batch_id, request_count DESC";
        List<ResultRow> out = new ArrayList<>();
        try (ResultSet rs = stmt.executeQuery(q)) {
            while (rs.next()) {
                ResultRow row = new ResultRow(rs.getInt(1), "top_resources");
                row.setResourcePath(rs.getString(2));
                row.setRequestCount(rs.getLong(3));
                row.setTotalBytes(rs.getLong(4));
                row.setDistinctHosts(rs.getInt(5));
                out.add(row);
            }
        }
        return out;
    }

    // -----------------------------------------------------------------------
    // Q3 — Hourly Error Analysis, per batch.
    // -----------------------------------------------------------------------
    private List<ResultRow> runHourlyErrors(Statement stmt, String table) throws Exception {
        String q =
                "SELECT batch_id,\n" +
                "       log_date,\n" +
                "       log_hour,\n" +
                "       SUM(CASE WHEN status_code BETWEEN 400 AND 599 THEN 1 ELSE 0 END) AS error_request_count,\n" +
                "       COUNT(*) AS total_request_count,\n" +
                "       (SUM(CASE WHEN status_code BETWEEN 400 AND 599 THEN 1 ELSE 0 END) / COUNT(*)) AS error_rate,\n" +
                "       COUNT(DISTINCT CASE WHEN status_code BETWEEN 400 AND 599 THEN host END) AS distinct_error_hosts\n" +
                "FROM " + table + "\n" +
                "WHERE log_date IS NOT NULL AND log_hour IS NOT NULL\n" +
                "GROUP BY batch_id, log_date, log_hour\n" +
                "ORDER BY batch_id, log_date, log_hour";
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

    // -----------------------------------------------------------------------
    // Read raw NCSA logs, parse with the shared LogParser, write parsed records
    // to tab-separated batch files of size <= batchSize lines each. Each TSV
    // row is prefixed with the batch_id so HiveQL can GROUP BY it directly.
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
        Map<Integer, Integer> recordsPerBatch   = new LinkedHashMap<>();
        Map<Integer, Integer> malformedPerBatch = new LinkedHashMap<>();

        long total = 0;
        long malformed = 0;
        int batchId = 0;
        int currentBatchRecords = 0;
        int currentBatchMalformed = 0;
        BufferedWriter writer = null;

        try {
            for (Path inputFile : inputFiles) {
                try (BufferedReader br = new BufferedReader(
                        new InputStreamReader(inputFs.open(inputFile)))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        // Rotate batch when the input-line count for the
                        // current batch hits batchSize, OR if no batch is open.
                        if (writer == null || currentBatchRecords == batchSize) {
                            if (writer != null) {
                                writer.close();
                                recordsPerBatch.put(batchId, currentBatchRecords);
                                malformedPerBatch.put(batchId, currentBatchMalformed);
                            }
                            batchId++;
                            currentBatchRecords = 0;
                            currentBatchMalformed = 0;
                            Path bf = new Path(batchDir,
                                    String.format("batch-%05d.tsv", batchId));
                            FSDataOutputStream out = localFs.create(bf, true);
                            writer = new BufferedWriter(new OutputStreamWriter(out));
                            batchFiles.add(bf);
                        }

                        total++;
                        currentBatchRecords++;

                        Optional<LogRecord> parsed = LogParser.parse(line);
                        if (!parsed.isPresent()) {
                            malformed++;
                            currentBatchMalformed++;
                            continue;   // malformed lines are not written to TSV.
                        }
                        writer.write(serialize(batchId, parsed.get()));
                        writer.newLine();
                    }
                }
            }
            if (writer != null) {
                recordsPerBatch.put(batchId, currentBatchRecords);
                malformedPerBatch.put(batchId, currentBatchMalformed);
            }
        } finally {
            if (writer != null) writer.close();
        }

        return new BatchPrep(batchDir, batchFiles, total, malformed,
                recordsPerBatch, malformedPerBatch);
    }

    private static String serialize(int batchId, LogRecord r) {
        StringBuilder sb = new StringBuilder(96);
        sb.append(batchId).append('\t')
          .append(safe(r.getHost())).append('\t')
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
        final Map<Integer, Integer> recordsPerBatch;
        final Map<Integer, Integer> malformedPerBatch;

        BatchPrep(Path batchDir, List<Path> batchFiles, long totalRecords, long malformedRecords,
                  Map<Integer, Integer> recordsPerBatch, Map<Integer, Integer> malformedPerBatch) {
            this.batchDir          = batchDir;
            this.batchFiles        = batchFiles;
            this.totalRecords      = totalRecords;
            this.malformedRecords  = malformedRecords;
            this.recordsPerBatch   = recordsPerBatch;
            this.malformedPerBatch = malformedPerBatch;
        }
    }
}
