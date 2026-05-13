package com.etl.pipeline.pig;

import com.etl.core.BatchSplitter;
import com.etl.core.ETLConfig;
import com.etl.core.Pipeline;
import com.etl.core.PipelineResult;
import com.etl.core.ResultRow;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;

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

/**
 * Pig-backed ETL pipeline (per-batch aggregation).
 *
 * The raw NASA log is first split into batch-NNNNN.log files of batchSize
 * lines each via {@link BatchSplitter}. Each batch file is then fed through
 * the three .pig scripts independently, so every aggregation runs *inside*
 * one batch. Java stamps the recovered batch_id onto every ResultRow.
 */
public class PigPipeline implements Pipeline {

    @Override
    public PipelineResult execute(ETLConfig config) throws Exception {
        Instant startedAt = Instant.now();
        long startTime = System.currentTimeMillis();

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");

        Path batchDir = new Path("/tmp/etl_pig_batches/" + config.getRunId());
        BatchSplitter.Result prep = BatchSplitter.split(
                config.getInputPath(), config.getBatchSize(), batchDir, conf);

        long totalRecords   = prep.totalRecords;
        int  totalBatches   = prep.batchFiles.size();
        long malformedCount = prep.malformedRecords;

        FileSystem fs = FileSystem.get(conf);

        String[] allQueries = {"daily_traffic", "top_resources", "hourly_errors"};
        List<String> queries = new ArrayList<>();
        for (String q : allQueries) if (config.runsQuery(q)) queries.add(q);

        List<ResultRow> rows = new ArrayList<>();

        for (Path batchFile : prep.batchFiles) {
            int batchId = BatchSplitter.batchIdFromFilename(batchFile.getName());

            for (String query : queries) {
                String outPath = "/tmp/etl_out_pig/" + config.getRunId()
                        + "/" + query + "/batch-" + batchId;
                Path outputPath = new Path(outPath);

                if (fs.exists(outputPath)) fs.delete(outputPath, true);

                Map<String, String> params = new HashMap<>();
                params.put("INPUT",  batchFile.toString());
                params.put("OUTPUT", outPath);

                String scriptPath = "pig/" + query + ".pig";

                // A fresh PigServer per script — alias state would otherwise
                // leak between consecutive registerScript() calls.
                PigServer pigServer = new PigServer(ExecType.LOCAL);
                try {
                    pigServer.registerScript(scriptPath, params);
                } finally {
                    pigServer.shutdown();
                }

                if (!fs.exists(outputPath)) {
                    throw new IllegalStateException(
                            "Pig query '" + query + "' produced no output at " + outPath);
                }

                rows.addAll(readResults(conf, outputPath, query, batchId));
            }
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

    private List<ResultRow> readResults(Configuration conf, Path outPath,
                                        String queryName, int batchId) throws Exception {
        List<ResultRow> results = new ArrayList<>();
        FileSystem fs = FileSystem.get(conf);

        org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus> iter =
                fs.listFiles(outPath, true);

        while (iter.hasNext()) {
            org.apache.hadoop.fs.LocatedFileStatus fileStatus = iter.next();
            if (!fileStatus.getPath().getName().startsWith("part-")) continue;

            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(fs.open(fileStatus.getPath())))) {

                String line;
                while ((line = br.readLine()) != null) {
                    if (line.trim().isEmpty()) continue;
                    String[] parts = line.split("\\t");

                    // Fallback for whitespace-delimited Pig output (never apply
                    // to top_resources — resource paths could contain spaces).
                    if (parts.length == 1 && !line.contains("\t") && !"top_resources".equals(queryName)) {
                        parts = line.trim().split("\\s+");
                    }

                    ResultRow row = new ResultRow(batchId, queryName);

                    if ("daily_traffic".equals(queryName)) {
                        row.setLogDate(Date.valueOf(parts[0].trim()));
                        row.setStatusCode(Integer.parseInt(parts[1].trim()));
                        row.setRequestCount(Long.parseLong(parts[2].trim()));
                        row.setTotalBytes(Long.parseLong(parts[3].trim()));

                    } else if ("top_resources".equals(queryName)) {
                        row.setResourcePath(parts[0].trim());
                        row.setRequestCount(Long.parseLong(parts[1].trim()));
                        row.setTotalBytes(Long.parseLong(parts[2].trim()));
                        row.setDistinctHosts(Integer.parseInt(parts[3].trim()));

                    } else if ("hourly_errors".equals(queryName)) {
                        row.setLogDate(Date.valueOf(parts[0].trim()));
                        row.setLogHour(Integer.parseInt(parts[1].trim()));
                        row.setErrorRequestCount(Integer.parseInt(parts[2].trim()));
                        row.setTotalRequestCount(Integer.parseInt(parts[3].trim()));
                        row.setErrorRate(Double.parseDouble(parts[4].trim()));
                        row.setDistinctErrorHosts(Integer.parseInt(parts[5].trim()));
                    }
                    results.add(row);
                }
            }
        }
        return results;
    }

    /** Keep top 20 resources per batch. */
    private List<ResultRow> postProcessTopResources(List<ResultRow> allRows) {
        List<ResultRow> finalRows = new ArrayList<>();
        Map<Integer, List<ResultRow>> topByBatch = new TreeMap<>();

        for (ResultRow row : allRows) {
            if ("top_resources".equals(row.getQueryName())) {
                topByBatch.computeIfAbsent(row.getBatchId(), k -> new ArrayList<>()).add(row);
            } else {
                finalRows.add(row);
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
