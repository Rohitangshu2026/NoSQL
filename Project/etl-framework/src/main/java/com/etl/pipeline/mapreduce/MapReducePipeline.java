package com.etl.pipeline.mapreduce;

import com.etl.core.BatchSplitter;
import com.etl.core.ETLConfig;
import com.etl.core.Pipeline;
import com.etl.core.PipelineResult;
import com.etl.core.ResultRow;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Date;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class MapReducePipeline implements Pipeline {

    @Override
    public PipelineResult execute(ETLConfig config) throws Exception {
        Instant startedAt = Instant.now();
        long startTime = System.currentTimeMillis();

        // Hadoop will resolve splits off this Configuration; force local FS so we can
        // read the batch files we just wrote under /tmp.
        Configuration baseConf = new Configuration();
        baseConf.set("fs.defaultFS", "file:///");

        // Pre-split the raw NCSA log into batch-NNNNN.log files. The LogMapper
        // recovers the batch id from the file name, so MR ends up emitting
        // per-batch grouping keys.
        Path batchDir = new Path("/tmp/etl_mr_batches/" + config.getRunId());
        BatchSplitter.Result prep = BatchSplitter.split(
                config.getInputPath(), config.getBatchSize(), batchDir, baseConf);

        long totalRecords   = prep.totalRecords;
        long malformedCount = prep.malformedRecords;
        int  totalBatches   = prep.batchFiles.size();
        List<ResultRow> rows = new ArrayList<>();

        String[] queries = {"daily_traffic", "top_resources", "hourly_errors"};

        for (String query : queries) {
            if (!config.runsQuery(query)) continue;

            Configuration conf = new Configuration(baseConf);
            conf.set("etl.query.name", query);

            Job job = Job.getInstance(conf, "ETL Job: " + query);
            job.setJarByClass(MapReducePipeline.class);

            job.setMapperClass(LogMapper.class);
            job.setReducerClass(QueryReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            Path outPath = new Path("/tmp/etl_out/" + config.getRunId() + "/" + query);

            FileInputFormat.addInputPath(job, batchDir);
            FileOutputFormat.setOutputPath(job, outPath);

            boolean success = job.waitForCompletion(true);
            if (!success) {
                throw new Exception("Job " + query + " failed.");
            }

            rows.addAll(readResults(conf, outPath, query));
        }

        rows = postProcessTopResources(rows);

        long runtimeMs = System.currentTimeMillis() - startTime;
        return new PipelineResult(
                totalRecords, totalBatches, malformedCount,
                runtimeMs, startedAt, Instant.now(),
                rows,
                prep.recordsPerBatch,
                prep.malformedPerBatch);
    }

    private List<ResultRow> readResults(Configuration conf, Path outPath, String queryName) throws Exception {
        List<ResultRow> results = new ArrayList<>();
        FileSystem fs = FileSystem.get(conf);
        org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus> iter =
                fs.listFiles(outPath, true);

        while (iter.hasNext()) {
            org.apache.hadoop.fs.LocatedFileStatus fileStatus = iter.next();
            if (!fileStatus.getPath().getName().startsWith("part-r-")) continue;

            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(fileStatus.getPath())))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split("\\t");
                    String[] keyTokens = parts[0].split("\\|");
                    int batchId = Integer.parseInt(keyTokens[0]);

                    ResultRow row = new ResultRow(batchId, queryName);

                    if ("daily_traffic".equals(queryName)) {
                        // key: batchId | log_date | status_code
                        row.setLogDate(Date.valueOf(keyTokens[1]));
                        row.setStatusCode(Integer.parseInt(keyTokens[2]));
                        row.setRequestCount(Long.parseLong(parts[1]));
                        row.setTotalBytes(Long.parseLong(parts[2]));

                    } else if ("top_resources".equals(queryName)) {
                        // key: batchId | resource_path  (path itself may contain '|', so reconstruct)
                        String resourcePath = parts[0].substring(parts[0].indexOf('|') + 1);
                        row.setResourcePath(resourcePath);
                        row.setRequestCount(Long.parseLong(parts[1]));
                        row.setTotalBytes(Long.parseLong(parts[2]));
                        row.setDistinctHosts(Integer.parseInt(parts[3]));

                    } else if ("hourly_errors".equals(queryName)) {
                        // key: batchId | log_date | log_hour
                        row.setLogDate(Date.valueOf(keyTokens[1]));
                        row.setLogHour(Integer.parseInt(keyTokens[2]));
                        row.setErrorRequestCount(Integer.parseInt(parts[1]));
                        row.setTotalRequestCount(Integer.parseInt(parts[2]));
                        row.setErrorRate(Double.parseDouble(parts[3]));
                        row.setDistinctErrorHosts(Integer.parseInt(parts[4]));
                    }
                    results.add(row);
                }
            }
        }
        return results;
    }

    /** Keep the top 20 resources per batch. */
    private List<ResultRow> postProcessTopResources(List<ResultRow> allRows) {
        List<ResultRow> finalRows = new ArrayList<>();
        Map<Integer, List<ResultRow>> topByBatch = new TreeMap<>();

        for (ResultRow r : allRows) {
            if ("top_resources".equals(r.getQueryName())) {
                topByBatch.computeIfAbsent(r.getBatchId(), k -> new ArrayList<>()).add(r);
            } else {
                finalRows.add(r);
            }
        }

        for (Map.Entry<Integer, List<ResultRow>> e : topByBatch.entrySet()) {
            List<ResultRow> perBatch = e.getValue();
            perBatch.sort(Comparator.comparing(ResultRow::getRequestCount).reversed());
            finalRows.addAll(perBatch.stream().limit(20).collect(Collectors.toList()));
        }
        return finalRows;
    }
}
