package com.etl.pipeline.mapreduce;

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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.time.Instant;
import java.util.stream.Collectors;

public class MapReducePipeline implements Pipeline {

    @Override
    public PipelineResult execute(ETLConfig config) throws Exception {
        Instant startedAt = Instant.now();
        long startTime = System.currentTimeMillis();
        long totalRecords = 0;
        long malformedCount = 0;
        List<ResultRow> rows = new ArrayList<>();
        int maxBatches = 0;

        String[] queries = {"daily_traffic", "top_resources", "hourly_errors"};

        for (String query : queries) {
            if (!config.runsQuery(query)) continue;
            Configuration conf = new Configuration();
            conf.set("etl.query.name", query);

            Job job = Job.getInstance(conf, "ETL Job: " + query);
            job.setJarByClass(MapReducePipeline.class);

            job.setMapperClass(LogMapper.class);
            job.setReducerClass(QueryReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            Path inPath = new Path(config.getInputPath());
            Path outPath = new Path("/tmp/etl_out/" + config.getRunId() + "/" + query);

            FileInputFormat.addInputPath(job, inPath);
            FileOutputFormat.setOutputPath(job, outPath);

            boolean success = job.waitForCompletion(true);
            if (!success) {
                throw new Exception("Job " + query + " failed.");
            }

            // Counter values are the same across all three queries (same input, same mapper).
            // Capture them once from whichever query ran first.
            if (totalRecords == 0) {
                totalRecords = job.getCounters().findCounter(LogMapper.Counters.TOTAL_RECORDS).getValue();
                malformedCount = job.getCounters().findCounter(LogMapper.Counters.MALFORMED_RECORDS).getValue();
            }

            // MapReduce doesn't strictly provide a built-in count of total distinct input splits across all jobs easily in a variable,
            // but the number of maps is generally the number of batches.
            int batches = job.getConfiguration().getInt("mapreduce.job.maps", 1);
            if (batches > maxBatches) {
                maxBatches = batches;
            }

            rows.addAll(readResults(conf, outPath, query));
        }

        // Post-process top_resources to keep only top 20 per batch
        rows = postProcessTopResources(rows);

        long runtimeMs = System.currentTimeMillis() - startTime;
        return new PipelineResult(totalRecords, maxBatches, malformedCount, runtimeMs, startedAt, Instant.now(), rows);
    }

    private List<ResultRow> readResults(Configuration conf, Path outPath, String queryName) throws Exception {
        List<ResultRow> results = new ArrayList<>();
        FileSystem fs = FileSystem.get(conf);
        org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus> fileStatusListIterator = fs.listFiles(outPath, true);

        while (fileStatusListIterator.hasNext()) {
            org.apache.hadoop.fs.LocatedFileStatus fileStatus = fileStatusListIterator.next();
            if (fileStatus.getPath().getName().startsWith("part-r-")) {
                try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(fileStatus.getPath())))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] parts = line.split("\\t");
                        String keyPart = parts[0];
                        String[] keyTokens = keyPart.split("\\|");
                        int batchId = 1;

                        ResultRow row = new ResultRow(batchId, queryName);

                        if ("daily_traffic".equals(queryName)) {
                            row.setLogDate(Date.valueOf(keyTokens[0]));
                            row.setStatusCode(Integer.parseInt(keyTokens[1]));
                            row.setRequestCount(Long.parseLong(parts[1]));
                            row.setTotalBytes(Long.parseLong(parts[2]));

                        } else if ("top_resources".equals(queryName)) {
                            row.setResourcePath(keyTokens[0]);
                            row.setRequestCount(Long.parseLong(parts[1]));
                            row.setTotalBytes(Long.parseLong(parts[2]));
                            row.setDistinctHosts(Integer.parseInt(parts[3]));

                        } else if ("hourly_errors".equals(queryName)) {
                            row.setLogDate(Date.valueOf(keyTokens[0]));
                            row.setLogHour(Integer.parseInt(keyTokens[1]));
                            row.setErrorRequestCount(Integer.parseInt(parts[1]));
                            row.setTotalRequestCount(Integer.parseInt(parts[2]));
                            row.setErrorRate(Double.parseDouble(parts[3]));
                            row.setDistinctErrorHosts(Integer.parseInt(parts[4]));
                        }
                        results.add(row);
                    }
                }
            }
        }
        return results;
    }

    private List<ResultRow> postProcessTopResources(List<ResultRow> allRows) {
        List<ResultRow> finalRows = new ArrayList<>();

        List<ResultRow> topResRows = new ArrayList<>();
        for (ResultRow r : allRows) {
            if ("top_resources".equals(r.getQueryName())) {
                topResRows.add(r);
            } else {
                finalRows.add(r);
            }
        }

        topResRows.sort(Comparator.comparing(ResultRow::getRequestCount).reversed());
        finalRows.addAll(topResRows.stream().limit(20).collect(Collectors.toList()));

        return finalRows;
    }
}