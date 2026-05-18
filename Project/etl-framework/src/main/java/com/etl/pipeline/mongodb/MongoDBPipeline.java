package com.etl.pipeline.mongodb;

import com.etl.core.ETLConfig;
import com.etl.core.Pipeline;
import com.etl.core.PipelineResult;
import com.etl.core.ResultRow;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
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

/**
 * MongoDB-backed ETL pipeline with engine-native parsing and per-batch
 * aggregation.
 *
 * <p><b>Engine-native parsing.</b> Raw NCSA log lines are inserted into a
 * per-run MongoDB collection as documents of shape
 * {@code {batch_id, line}} — <em>no parsing happens in Java</em>. Every
 * analytical aggregation pipeline begins with a shared "parse stage" of
 * MongoDB aggregation operators:
 * <ul>
 *   <li>{@code $regexFind} — runs the NCSA regex inside MongoDB</li>
 *   <li>{@code $match} — drops malformed lines (regex did not match)</li>
 *   <li>{@code $addFields} — extracts capture groups, casts status_code,
 *       parses the timestamp via {@code $dateFromString}, computes
 *       {@code log_date}, {@code log_hour}, {@code resource_path}, and
 *       {@code bytes} (with "-" &rarr; 0)</li>
 * </ul>
 *
 * <p>Subsequent stages run the per-batch group/aggregation work specific to
 * each of Q1/Q2/Q3. The collection is dropped after all queries complete.
 *
 * <p>Per-batch aggregation is enforced by every {@code $group _id} including
 * {@code batch_id}; Q2 uses
 * {@code $sort + $group $push + $project $slice 20} for top-20 enforcement.
 */
public class MongoDBPipeline implements Pipeline {

    /**
     * NCSA regex used inside MongoDB's {@code $regexFind} stage. Mirrors the
     * Java {@code LogParser} regex so the four pipelines agree on what counts
     * as a well-formed line.
     */
    private static final String NCSA_REGEX =
            "^(\\S+) (\\S+) (\\S+) \\[([^\\]]+)\\] \"([^\"]*)\" (\\d{3}) (\\S+)\\s*$";

    @Override
    public PipelineResult execute(ETLConfig config) throws Exception {
        Instant startedAt = Instant.now();
        long startTime = System.currentTimeMillis();

        String runId = config.getRunId().toString();
        String collectionName = "etl_logs_" + runId.replace("-", "");

        long totalRecords   = 0;
        int  batchCount     = 0;
        List<ResultRow> rows = new ArrayList<>();

        Map<Integer, Integer> recordsPerBatch   = new LinkedHashMap<>();
        Map<Integer, Integer> malformedPerBatch = new LinkedHashMap<>();   // computed post-load

        try (MongoClient client = MongoClients.create(config.getMongoUri())) {
            MongoDatabase db = client.getDatabase(config.getMongoDatabase());
            MongoCollection<Document> coll = db.getCollection(collectionName);

            coll.drop();
            coll.createIndex(Indexes.ascending("batch_id"), new IndexOptions().background(false));

            // ── Step 1 — bulk-insert raw NCSA lines as {batch_id, line} docs.
            // The only Java work is BufferedReader.readLine() and insertMany().
            // No regex matching, date parsing, or field extraction happens
            // here — every parse step runs server-side inside MongoDB.
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "file:///");
            Path rootPath = new Path(config.getInputPath());
            FileSystem fs = rootPath.getFileSystem(conf);

            List<Path> inputFiles = collectInputFiles(fs, rootPath);
            int batchSize = config.getBatchSize();
            int currentBatchId = 1;
            int currentBatchRecords = 0;
            List<Document> buffer = new ArrayList<>(batchSize);

            for (Path file : inputFiles) {
                try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(file)))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        if (currentBatchRecords == batchSize) {
                            // Flush before starting next batch.
                            if (!buffer.isEmpty()) {
                                coll.insertMany(buffer);
                                buffer.clear();
                            }
                            recordsPerBatch.put(currentBatchId, currentBatchRecords);
                            batchCount++;
                            currentBatchId++;
                            currentBatchRecords = 0;
                        }

                        totalRecords++;
                        currentBatchRecords++;

                        buffer.add(new Document()
                                .append("batch_id", currentBatchId)
                                .append("line",     line));
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
                batchCount++;
            }

            // ── Step 2 — count malformed lines via $regexFind on the
            //              raw {batch_id, line} collection. This count is
            //              also computed entirely server-side.
            long malformedCount = countMalformed(coll);
            for (Integer bid : recordsPerBatch.keySet()) {
                malformedPerBatch.put(bid, countMalformedForBatch(coll, bid));
            }

            System.out.println("[MongoDBPipeline] Inserted " + totalRecords
                    + " raw lines into " + collectionName
                    + " across " + batchCount + " batches"
                    + " (malformed = " + malformedCount + ", detected server-side).");

            // Once the data is in, build indexes that will accelerate the
            // grouping aggregations after the parse stage.
            coll.createIndex(Indexes.ascending("batch_id"));

            // ── Step 3 — run each analytical aggregation pipeline. Every
            //              pipeline prepends parseStages() so parsing happens
            //              inside MongoDB before grouping/summing.
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

            long runtimeMs = System.currentTimeMillis() - startTime;
            return new PipelineResult(
                    totalRecords, batchCount, malformedCount,
                    runtimeMs, startedAt, Instant.now(),
                    rows,
                    recordsPerBatch,
                    malformedPerBatch);
        }
    }

    // -----------------------------------------------------------------------
    // Shared parse stages — prepended to every analytical aggregation.
    // Runs the NCSA regex inside MongoDB, drops malformed lines, extracts
    // capture groups, parses the timestamp, and projects the typed fields
    // used by Q1/Q2/Q3 ($host, $log_date, $log_hour, $resource_path,
    // $status_code, $bytes).
    // -----------------------------------------------------------------------
    private static List<Document> parseStages() {
        List<Document> stages = new ArrayList<>();

        // $regexFind on the raw line — returns null if no match.
        stages.add(new Document("$addFields", new Document("p",
                new Document("$regexFind",
                        new Document("input", "$line")
                                .append("regex", NCSA_REGEX)))));

        // Filter out malformed lines (regex didn't match).
        stages.add(new Document("$match", new Document("p", new Document("$ne", null))));

        // Pull capture groups into typed fields. Capture group order:
        //   captures[0] = host
        //   captures[1] = ident   (unused, "-")
        //   captures[2] = authuser (unused, "-")
        //   captures[3] = ts      (e.g. "01/Jul/1995:00:00:01 -0400")
        //   captures[4] = request (e.g. "GET /history/apollo/ HTTP/1.0")
        //   captures[5] = status_code (3-digit string)
        //   captures[6] = bytes_str ("-" for none, else digits)
        stages.add(new Document("$addFields", new Document()
                .append("host",        new Document("$arrayElemAt", Arrays.asList("$p.captures", 0)))
                .append("ts",          new Document("$arrayElemAt", Arrays.asList("$p.captures", 3)))
                .append("request",     new Document("$arrayElemAt", Arrays.asList("$p.captures", 4)))
                .append("status_code", new Document("$toInt",
                        new Document("$arrayElemAt", Arrays.asList("$p.captures", 5))))
                .append("bytes_str",   new Document("$arrayElemAt", Arrays.asList("$p.captures", 6)))));

        // Parse the date portion only ("01/Jul/1995") — no timezone
        // conversion. Mirrors the LogParser / Hive RegexSerDe behaviour of
        // using the NCSA local-time date as-is.
        // log_hour: bytes 12-13 of "01/Jul/1995:00:00:01 -0400" = "00".
        // resource_path: 2nd token of the request line.
        // bytes: "-" → 0L, else $toLong.
        stages.add(new Document("$addFields", new Document()
                .append("parsed_dt", new Document("$dateFromString",
                        new Document("dateString",
                                new Document("$substr", Arrays.asList("$ts", 0, 11)))
                                .append("format", "%d/%b/%Y")))
                .append("log_hour", new Document("$toInt",
                        new Document("$substr", Arrays.asList("$ts", 12, 2))))
                .append("resource_path", new Document("$arrayElemAt", Arrays.asList(
                        new Document("$split", Arrays.asList("$request", " ")), 1)))
                .append("bytes", new Document("$cond", Arrays.asList(
                        new Document("$eq", Arrays.asList("$bytes_str", "-")),
                        0L,
                        new Document("$toLong", "$bytes_str"))))));

        // Derive log_date (yyyy-MM-dd string) from the parsed date.
        stages.add(new Document("$addFields", new Document()
                .append("log_date", new Document("$dateToString",
                        new Document("date", "$parsed_dt").append("format", "%Y-%m-%d")))));

        return stages;
    }

    // -----------------------------------------------------------------------
    // Server-side malformed-line counting via $regexFind.
    // -----------------------------------------------------------------------
    private static long countMalformed(MongoCollection<Document> coll) {
        List<Document> pipeline = Arrays.asList(
                new Document("$addFields", new Document("matched",
                        new Document("$regexFind",
                                new Document("input", "$line")
                                        .append("regex", NCSA_REGEX)))),
                new Document("$match", new Document("matched", null)),
                new Document("$count", "n"));
        long n = 0;
        for (Document d : coll.aggregate(pipeline).allowDiskUse(true)) {
            n = d.getInteger("n", 0);
        }
        return n;
    }

    private static int countMalformedForBatch(MongoCollection<Document> coll, int batchId) {
        List<Document> pipeline = Arrays.asList(
                new Document("$match", new Document("batch_id", batchId)),
                new Document("$addFields", new Document("matched",
                        new Document("$regexFind",
                                new Document("input", "$line")
                                        .append("regex", NCSA_REGEX)))),
                new Document("$match", new Document("matched", null)),
                new Document("$count", "n"));
        for (Document d : coll.aggregate(pipeline).allowDiskUse(true)) {
            return d.getInteger("n", 0);
        }
        return 0;
    }

    // -----------------------------------------------------------------------
    // Q1: Daily traffic summary (per batch)
    // -----------------------------------------------------------------------
    private List<ResultRow> runDailyTraffic(MongoCollection<Document> coll) {
        List<Document> pipeline = new ArrayList<>(parseStages());
        pipeline.add(new Document("$group", new Document()
                .append("_id", new Document()
                        .append("batch_id",    "$batch_id")
                        .append("log_date",    "$log_date")
                        .append("status_code", "$status_code"))
                .append("request_count", new Document("$sum", 1))
                .append("total_bytes",   new Document("$sum", "$bytes"))));
        pipeline.add(new Document("$sort", new Document("_id.batch_id", 1)
                .append("_id.log_date", 1)
                .append("_id.status_code", 1)));

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
    // Phase 1: parse, group by (batch_id, resource_path), aggregate counts.
    // Phase 2: sort within each batch, $push into an array, $slice the top 20.
    // -----------------------------------------------------------------------
    private List<ResultRow> runTopResources(MongoCollection<Document> coll) {
        List<Document> pipeline = new ArrayList<>(parseStages());
        pipeline.add(new Document("$group", new Document()
                .append("_id", new Document()
                        .append("batch_id",      "$batch_id")
                        .append("resource_path", "$resource_path"))
                .append("request_count", new Document("$sum", 1))
                .append("total_bytes",   new Document("$sum", "$bytes"))
                .append("hosts",         new Document("$addToSet", "$host"))));
        pipeline.add(new Document("$project", new Document()
                .append("batch_id",      "$_id.batch_id")
                .append("resource_path", "$_id.resource_path")
                .append("request_count", 1)
                .append("total_bytes",   1)
                .append("distinct_hosts", new Document("$size", "$hosts"))));
        pipeline.add(new Document("$sort", new Document("batch_id", 1).append("request_count", -1)));
        pipeline.add(new Document("$group", new Document()
                .append("_id", "$batch_id")
                .append("top", new Document("$push", new Document()
                        .append("resource_path",  "$resource_path")
                        .append("request_count",  "$request_count")
                        .append("total_bytes",    "$total_bytes")
                        .append("distinct_hosts", "$distinct_hosts")))));
        pipeline.add(new Document("$project", new Document()
                .append("top", new Document("$slice", Arrays.asList("$top", 20)))));
        pipeline.add(new Document("$sort", new Document("_id", 1)));

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
    //
    // Conditional $sum counts errors without a $match prefilter; distinct
    // error hosts are accumulated into a $addToSet that conditionally emits
    // $$REMOVE for non-error rows.
    // -----------------------------------------------------------------------
    private List<ResultRow> runHourlyErrors(MongoCollection<Document> coll) {
        Document isErrorOne = new Document("$cond", Arrays.asList(
                new Document("$and", Arrays.asList(
                        new Document("$gte", Arrays.asList("$status_code", 400)),
                        new Document("$lte", Arrays.asList("$status_code", 599)))),
                1, 0));

        Document errorHostOrRemove = new Document("$cond", Arrays.asList(
                new Document("$and", Arrays.asList(
                        new Document("$gte", Arrays.asList("$status_code", 400)),
                        new Document("$lte", Arrays.asList("$status_code", 599)))),
                "$host",
                "$$REMOVE"));

        List<Document> pipeline = new ArrayList<>(parseStages());
        pipeline.add(new Document("$group", new Document()
                .append("_id", new Document()
                        .append("batch_id", "$batch_id")
                        .append("log_date", "$log_date")
                        .append("log_hour", "$log_hour"))
                .append("total_request_count", new Document("$sum", 1))
                .append("error_request_count", new Document("$sum", isErrorOne))
                .append("error_hosts",         new Document("$addToSet", errorHostOrRemove))));
        pipeline.add(new Document("$project", new Document()
                .append("total_request_count", 1)
                .append("error_request_count", 1)
                .append("error_rate", new Document("$cond", Arrays.asList(
                        new Document("$eq", Arrays.asList("$total_request_count", 0)),
                        0.0,
                        new Document("$divide", Arrays.asList(
                                "$error_request_count", "$total_request_count")))))
                .append("distinct_error_hosts", new Document("$size", "$error_hosts"))));
        pipeline.add(new Document("$sort", new Document("_id.batch_id", 1)
                .append("_id.log_date", 1)
                .append("_id.log_hour", 1)));

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
