package com.etl.pipeline.mongodb;

import com.etl.core.ETLConfig;
import com.etl.core.LogParser;
import com.etl.core.LogRecord;
import com.etl.core.Pipeline;
import com.etl.core.PipelineResult;
import com.etl.core.ResultRow;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import org.bson.Document;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Date;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * MongoDB-backed ETL pipeline with per-batch aggregation.
 *
 *   1. NCSA log lines are parsed in Java and bulk-inserted into a per-run
 *      collection. Every document carries the batch_id it belonged to.
 *   2. The three mandatory queries are expressed as MongoDB aggregation
 *      pipelines that GROUP BY batch_id first, so each result row corresponds
 *      to a single (batch_id, grouping key) pair.
 *   3. The temporary collection is dropped on completion.
 */
public class MongoDBPipeline implements Pipeline {

    @Override
    public PipelineResult execute(ETLConfig config) throws Exception {
        Instant startedAt = Instant.now();
        long startTime = System.currentTimeMillis();

        String runId = config.getRunId().toString();
        String collectionName = "etl_logs_" + runId.replace("-", "");

        long totalRecords = 0;
        long malformedCount = 0;
        int  batchCount = 0;
        List<ResultRow> rows = new ArrayList<>();

        Map<Integer, Integer> recordsPerBatch   = new LinkedHashMap<>();
        Map<Integer, Integer> malformedPerBatch = new LinkedHashMap<>();

        try (MongoClient client = MongoClients.create(config.getMongoUri())) {
            MongoDatabase db = client.getDatabase(config.getMongoDatabase());
            MongoCollection<Document> coll = db.getCollection(collectionName);

            coll.drop();
            coll.createIndex(Indexes.ascending("batch_id"), new IndexOptions().background(false));
            coll.createIndex(Indexes.ascending("batch_id", "log_date", "status_code"));
            coll.createIndex(Indexes.ascending("batch_id", "resource_path"));
            coll.createIndex(Indexes.ascending("batch_id", "log_date", "log_hour"));

            // --- ETL: stream raw input, parse, batched-insert with batch_id --
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "file:///");
            Path rootPath = new Path(config.getInputPath());
            FileSystem fs = rootPath.getFileSystem(conf);

            List<Path> inputFiles = collectInputFiles(fs, rootPath);
            int batchSize = config.getBatchSize();
            int currentBatchId = 0;
            int currentBatchRecords = 0;
            int currentBatchMalformed = 0;
            List<Document> buffer = new ArrayList<>(batchSize);

            for (Path file : inputFiles) {
                try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(file)))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        // A new batch starts at the first record AND every time
                        // we've filled batchSize lines of *input* (parsed or not).
                        if (currentBatchRecords == 0 && currentBatchId == 0) {
                            currentBatchId = 1;
                        } else if (currentBatchRecords == batchSize) {
                            // Flush current batch into Mongo before starting the next.
                            if (!buffer.isEmpty()) {
                                coll.insertMany(buffer);
                                buffer.clear();
                            }
                            recordsPerBatch.put(currentBatchId, currentBatchRecords);
                            malformedPerBatch.put(currentBatchId, currentBatchMalformed);
                            batchCount++;
                            currentBatchId++;
                            currentBatchRecords = 0;
                            currentBatchMalformed = 0;
                        }

                        totalRecords++;
                        currentBatchRecords++;

                        Optional<LogRecord> parsed = LogParser.parse(line);
                        if (!parsed.isPresent()) {
                            malformedCount++;
                            currentBatchMalformed++;
                            continue;
                        }
                        LogRecord r = parsed.get();
                        Document doc = new Document()
                                .append("batch_id",      currentBatchId)
                                .append("host",          r.getHost())
                                .append("log_date",      r.getLogDate())
                                .append("log_hour",      r.getLogHour())
                                .append("method",        r.getMethod())
                                .append("resource_path", r.getResourcePath())
                                .append("protocol",      r.getProtocol())
                                .append("status_code",   r.getStatusCode())
                                .append("bytes",         r.getBytes());
                        buffer.add(doc);
                    }
                }
            }
            // Flush remaining records as the final batch.
            if (currentBatchRecords > 0) {
                if (!buffer.isEmpty()) {
                    coll.insertMany(buffer);
                    buffer.clear();
                }
                recordsPerBatch.put(currentBatchId, currentBatchRecords);
                malformedPerBatch.put(currentBatchId, currentBatchMalformed);
                batchCount++;
            }

            System.out.println("[MongoDBPipeline] Loaded " + (totalRecords - malformedCount)
                    + " documents into " + collectionName + " across " + batchCount + " batches.");

            // --- Aggregations -----------------------------------------------
            try {
                if (config.runsQuery("daily_traffic")) {
                    rows.addAll(runDailyTraffic(coll));
                }
                if (config.runsQuery("top_resources")) {
                    rows.addAll(runTopResources(coll));
                }
                if (config.runsQuery("hourly_errors")) {
                    rows.addAll(runHourlyErrors(coll));
                }
            } finally {
                coll.drop();
            }
        }

        long runtimeMs = System.currentTimeMillis() - startTime;
        return new PipelineResult(
                totalRecords, batchCount, malformedCount,
                runtimeMs, startedAt, Instant.now(),
                rows,
                recordsPerBatch,
                malformedPerBatch);
    }

    // -----------------------------------------------------------------------
    // Q1: Daily traffic summary (per batch)
    // -----------------------------------------------------------------------
    private List<ResultRow> runDailyTraffic(MongoCollection<Document> coll) {
        List<Document> pipeline = Arrays.asList(
                new Document("$group", new Document()
                        .append("_id", new Document()
                                .append("batch_id",    "$batch_id")
                                .append("log_date",    "$log_date")
                                .append("status_code", "$status_code"))
                        .append("request_count", new Document("$sum", 1))
                        .append("total_bytes",   new Document("$sum", "$bytes"))),
                new Document("$sort", new Document("_id.batch_id", 1)
                        .append("_id.log_date", 1)
                        .append("_id.status_code", 1))
        );

        List<ResultRow> out = new ArrayList<>();
        for (Document d : coll.aggregate(pipeline).allowDiskUse(true)) {
            Document key = d.get("_id", Document.class);
            ResultRow row = new ResultRow(key.getInteger("batch_id"), "daily_traffic");
            row.setLogDate(Date.valueOf(key.getString("log_date")));
            row.setStatusCode(key.getInteger("status_code"));
            row.setRequestCount(toLong(d.get("request_count")));
            row.setTotalBytes(toLong(d.get("total_bytes")));
            out.add(row);
        }
        return out;
    }

    // -----------------------------------------------------------------------
    // Q2: Top requested resources (top 20 per batch)
    //
    // Implemented as a two-phase aggregation: first $group by
    // (batch_id, resource_path), then $sort + $group-into-array + $slice 20
    // to keep only the top 20 per batch in a single pipeline.
    // -----------------------------------------------------------------------
    private List<ResultRow> runTopResources(MongoCollection<Document> coll) {
        List<Document> pipeline = Arrays.asList(
                new Document("$group", new Document()
                        .append("_id", new Document()
                                .append("batch_id",      "$batch_id")
                                .append("resource_path", "$resource_path"))
                        .append("request_count", new Document("$sum", 1))
                        .append("total_bytes",   new Document("$sum", "$bytes"))
                        .append("hosts",         new Document("$addToSet", "$host"))),
                new Document("$project", new Document()
                        .append("batch_id",      "$_id.batch_id")
                        .append("resource_path", "$_id.resource_path")
                        .append("request_count", 1)
                        .append("total_bytes",   1)
                        .append("distinct_hosts", new Document("$size", "$hosts"))),
                new Document("$sort", new Document("batch_id", 1).append("request_count", -1)),
                new Document("$group", new Document()
                        .append("_id", "$batch_id")
                        .append("top", new Document("$push", new Document()
                                .append("resource_path",  "$resource_path")
                                .append("request_count",  "$request_count")
                                .append("total_bytes",    "$total_bytes")
                                .append("distinct_hosts", "$distinct_hosts")))),
                new Document("$project", new Document()
                        .append("top", new Document("$slice", Arrays.asList("$top", 20)))),
                new Document("$sort", new Document("_id", 1))
        );

        List<ResultRow> out = new ArrayList<>();
        for (Document batchDoc : coll.aggregate(pipeline).allowDiskUse(true)) {
            int batchId = toInt(batchDoc.get("_id"));
            @SuppressWarnings("unchecked")
            List<Document> top = (List<Document>) batchDoc.get("top");
            if (top == null) continue;
            for (Document d : top) {
                ResultRow row = new ResultRow(batchId, "top_resources");
                row.setResourcePath(d.getString("resource_path"));
                row.setRequestCount(toLong(d.get("request_count")));
                row.setTotalBytes(toLong(d.get("total_bytes")));
                row.setDistinctHosts(toInt(d.get("distinct_hosts")));
                out.add(row);
            }
        }
        return out;
    }

    // -----------------------------------------------------------------------
    // Q3: Hourly error analysis (per batch)
    // -----------------------------------------------------------------------
    private List<ResultRow> runHourlyErrors(MongoCollection<Document> coll) {
        Document isErrorCond = new Document("$cond", Arrays.asList(
                new Document("$and", Arrays.asList(
                        new Document("$gte", Arrays.asList("$status_code", 400)),
                        new Document("$lte", Arrays.asList("$status_code", 599)))),
                1, 0));

        List<Document> pipeline = Arrays.asList(
                new Document("$group", new Document()
                        .append("_id", new Document()
                                .append("batch_id", "$batch_id")
                                .append("log_date", "$log_date")
                                .append("log_hour", "$log_hour"))
                        .append("total_request_count", new Document("$sum", 1))
                        .append("error_request_count", new Document("$sum", isErrorCond))
                        .append("error_hosts", new Document("$addToSet", new Document("$cond",
                                Arrays.asList(
                                        new Document("$and", Arrays.asList(
                                                new Document("$gte", Arrays.asList("$status_code", 400)),
                                                new Document("$lte", Arrays.asList("$status_code", 599)))),
                                        "$host",
                                        "$$REMOVE"))))),
                new Document("$project", new Document()
                        .append("total_request_count", 1)
                        .append("error_request_count", 1)
                        .append("error_rate", new Document("$cond", Arrays.asList(
                                new Document("$eq", Arrays.asList("$total_request_count", 0)),
                                0.0,
                                new Document("$divide", Arrays.asList(
                                        "$error_request_count", "$total_request_count")))))
                        .append("distinct_error_hosts", new Document("$size", "$error_hosts"))),
                new Document("$sort", new Document("_id.batch_id", 1)
                        .append("_id.log_date", 1)
                        .append("_id.log_hour", 1))
        );

        List<ResultRow> out = new ArrayList<>();
        for (Document d : coll.aggregate(pipeline).allowDiskUse(true)) {
            Document key = d.get("_id", Document.class);
            ResultRow row = new ResultRow(key.getInteger("batch_id"), "hourly_errors");
            row.setLogDate(Date.valueOf(key.getString("log_date")));
            row.setLogHour(key.getInteger("log_hour"));
            row.setErrorRequestCount(toInt(d.get("error_request_count")));
            row.setTotalRequestCount(toInt(d.get("total_request_count")));
            row.setErrorRate(toDouble(d.get("error_rate")));
            row.setDistinctErrorHosts(toInt(d.get("distinct_error_hosts")));
            out.add(row);
        }
        return out;
    }

    // ---- helpers ----------------------------------------------------------
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

    private static long toLong(Object o) {
        if (o == null) return 0L;
        if (o instanceof Number) return ((Number) o).longValue();
        return Long.parseLong(o.toString());
    }

    private static int toInt(Object o) {
        if (o == null) return 0;
        if (o instanceof Number) return ((Number) o).intValue();
        return Integer.parseInt(o.toString());
    }

    private static double toDouble(Object o) {
        if (o == null) return 0.0;
        if (o instanceof Number) return ((Number) o).doubleValue();
        return Double.parseDouble(o.toString());
    }
}
