package com.etl.pipeline.pig;

import com.etl.core.ETLConfig;
import com.etl.core.LogParser;
import com.etl.core.Pipeline;
import com.etl.core.PipelineResult;
import com.etl.core.ResultRow;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;

import java.time.Instant;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PigPipeline implements Pipeline {

    // -------------------------------------------------------------------------
    // FIX 1 — ExecType.LOCAL, not MAPREDUCE
    //
    // ExecType.MAPREDUCE triggers MRPigStatsUtil.accumulateStats() after each
    // job, which calls MRJobStats.getTaskReports(). That method requires
    // org.python.google.common.collect.Lists — a class from Jython's shaded
    // Guava that is NOT on the classpath when Pig is embedded in a fat JAR.
    //
    //   NoClassDefFoundError: org/python/google/common/collect/Lists
    //
    // ExecType.LOCAL skips MRJobStats entirely; the stats code path that
    // needs Jython's Guava is never reached. The original Phase 1 code used
    // LOCAL for exactly this reason.
    //
    // FIX 2 — conf.set("fs.defaultFS", "file:///")
    //
    // Without this, FileSystem.get(conf) resolves to whatever Hadoop's default
    // site XML says, which may be an HDFS URI. In LOCAL exec mode all Pig
    // output lands under the local filesystem, so the reader must also use
    // the local FS. Setting fs.defaultFS to file:/// makes both sides
    // consistent regardless of the Hadoop configuration present on the machine.
    //
    // FIX 3 — try-with-resources for PigServer
    //
    // PigServer implements Closeable. Leaving it open leaks a MiniMR cluster
    // handle in LOCAL mode and prevents clean JVM shutdown.
    // -------------------------------------------------------------------------

    @Override
    public PipelineResult execute(ETLConfig config) throws Exception {
        Instant startedAt = Instant.now();
        long startTime = System.currentTimeMillis();

        // Shared local-FS configuration used for batch writing and output reading.
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");   // FIX 2

        BatchPrepResult batchPrep = prepareBatches(
                config.getInputPath(), config.getBatchSize(),
                config.getRunId().toString(), conf);

        long totalRecords = batchPrep.totalRecords;
        int  totalBatches = batchPrep.batchFiles.size();
        long malformedCount = batchPrep.malformedRecords;
        List<ResultRow> rows = new ArrayList<>();

        String[] queries = {"daily_traffic", "top_resources", "hourly_errors"};

        // FIX 4 — A single PigServer accumulates alias state across registerScript() calls.
        // When top_resources.pig runs after daily_traffic.pig on the same instance,
        // Pig still holds the previous script's alias graph in memory. The alias
        // "top_resources" conflicts with the leftover state, causing:
        //   "Undefined alias: top_resources" / "Multiple values found for OUTPUT"
        // Fix: create and shut down a fresh PigServer for every script invocation.
        FileSystem fs = FileSystem.get(conf);   // resolves to local FS (file:///)

        String fullInput = config.getInputPath();  // FULL dataset

        for (String query : queries) {

            String outPath = "/tmp/etl_out_pig/" + config.getRunId() + "/" + query;
            Path outputPath = new Path(outPath);

            if (fs.exists(outputPath)) {
                fs.delete(outputPath, true);
            }

            Map<String, String> params = new HashMap<>();
            params.put("INPUT", fullInput);   // 🔥 key change
            params.put("OUTPUT", outPath);

            String scriptPath = "pig/" + query + ".pig";

            PigServer pigServer = new PigServer(ExecType.LOCAL);
            try {
                pigServer.registerScript(scriptPath, params);
            } finally {
                pigServer.shutdown();
            }

            if (!fs.exists(outputPath)) {
                throw new IllegalStateException(
                        "Pig query '" + query + "' finished without creating output at " + outPath);
            }

            // 🔥 single batch_id since global execution
            rows.addAll(readResults(conf, outputPath, query, 1));
        }

        rows = postProcessTopResources(rows);

        if (rows.isEmpty()) {
            throw new RuntimeException("Pig produced 0 result rows — check script output paths and UDF registration.");
        }

        long runtimeMs = System.currentTimeMillis() - startTime;
        return new PipelineResult(totalRecords, totalBatches, malformedCount, runtimeMs, startedAt, Instant.now(), rows);
    }

    // -------------------------------------------------------------------------
    // readResults — no logic changes needed; parsing was already correct.
    //
    // The Pig scripts use PigStorage('\t'), so line.split("\\t") is right.
    // The \s+ fallback is kept as a safety net but must NOT be used on
    // top_resources lines since resource paths can theoretically contain spaces.
    // The column indices have been verified against the UDF tuple layout:
    //
    //   UDF index: 0=host 1=logDate 2=logHour 3=method 4=resourcePath
    //              5=protocol 6=statusCode 7=bytes
    //
    //   daily_traffic  output: logDate | statusCode | requestCount | totalBytes
    //   top_resources  output: resourcePath | requestCount | totalBytes | distinctHosts
    //   hourly_errors  output: logDate | logHour | errorCount | totalCount | errorRate | distinctErrHosts
    //
    // All column positions match parts[0..N] exactly.
    // -------------------------------------------------------------------------
    private List<ResultRow> readResults(Configuration conf, Path outPath,
                                        String queryName, int batchId) throws Exception {
        List<ResultRow> results = new ArrayList<>();
        FileSystem fs = FileSystem.get(conf);   // local FS — matches Pig LOCAL output location

        org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus> iter =
                fs.listFiles(outPath, true);

        while (iter.hasNext()) {
            org.apache.hadoop.fs.LocatedFileStatus fileStatus = iter.next();
            if (!fileStatus.getPath().getName().startsWith("part-")) continue;

            System.out.println("[PigPipeline] Reading: " + fileStatus.getPath());

            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(fs.open(fileStatus.getPath())))) {

                String line;
                while ((line = br.readLine()) != null) {
                    if (line.trim().isEmpty()) continue;

                    // Primary split: tab (matches PigStorage('\t') in all three scripts)
                    String[] parts = line.split("\\t");

                    // Safety fallback: only for non-top_resources queries where fields
                    // cannot contain spaces. Never apply to top_resources — a resource
                    // path like "/shuttle/countdown/ HTTP" would be split incorrectly.
                    if (parts.length == 1 && !line.contains("\t") && !"top_resources".equals(queryName)) {
                        parts = line.trim().split("\\s+");
                    }

                    ResultRow row = new ResultRow(batchId, queryName);

                    try {
                        if ("daily_traffic".equals(queryName)) {
                            if (parts.length < 4) throw new RuntimeException(
                                    "daily_traffic: expected 4 columns, got " + parts.length + " | line: " + line);
                            row.setLogDate(Date.valueOf(parts[0].trim()));
                            row.setStatusCode(Integer.parseInt(parts[1].trim()));
                            row.setRequestCount(Long.parseLong(parts[2].trim()));
                            row.setTotalBytes(Long.parseLong(parts[3].trim()));

                        } else if ("top_resources".equals(queryName)) {
                            if (parts.length < 4) throw new RuntimeException(
                                    "top_resources: expected 4 columns, got " + parts.length + " | line: " + line);
                            row.setResourcePath(parts[0].trim());
                            row.setRequestCount(Long.parseLong(parts[1].trim()));
                            row.setTotalBytes(Long.parseLong(parts[2].trim()));
                            row.setDistinctHosts(Integer.parseInt(parts[3].trim()));

                        } else if ("hourly_errors".equals(queryName)) {
                            if (parts.length < 6) throw new RuntimeException(
                                    "hourly_errors: expected 6 columns, got " + parts.length + " | line: " + line);
                            row.setLogDate(Date.valueOf(parts[0].trim()));
                            row.setLogHour(Integer.parseInt(parts[1].trim()));
                            row.setErrorRequestCount(Integer.parseInt(parts[2].trim()));
                            row.setTotalRequestCount(Integer.parseInt(parts[3].trim()));
                            row.setErrorRate(Double.parseDouble(parts[4].trim()));
                            row.setDistinctErrorHosts(Integer.parseInt(parts[5].trim()));
                        }

                        results.add(row);

                    } catch (Exception e) {
                        System.err.println("[PigPipeline] Parse error in " + queryName + ": " + line);
                        e.printStackTrace();
                        throw new RuntimeException("Aborting: parsing failure in " + queryName, e);
                    }
                }
            }
            System.out.println("[PigPipeline] Rows parsed from " + fileStatus.getPath().getName() + ": " + results.size());
        }
        return results;
    }

    private List<ResultRow> postProcessTopResources(List<ResultRow> allRows) {
        List<ResultRow> finalRows      = new ArrayList<>();
        List<ResultRow> topResourceRows = new ArrayList<>();

        for (ResultRow row : allRows) {
            if ("top_resources".equals(row.getQueryName())) {
                topResourceRows.add(row);
            } else {
                finalRows.add(row);
            }
        }

        topResourceRows.stream()
                .collect(Collectors.groupingBy(ResultRow::getBatchId))
                .forEach((batchId, batchRows) -> {
                    batchRows.sort(Comparator.comparing(ResultRow::getRequestCount).reversed());
                    finalRows.addAll(batchRows.stream().limit(20).collect(Collectors.toList()));
                });

        return finalRows;
    }

    // -------------------------------------------------------------------------
    // prepareBatches — passes the shared conf so localFs is consistent.
    //
    // The original code called FileSystem.getLocal(conf) for writing but
    // new Configuration() (potentially HDFS) for reading output. Both now
    // use the same conf with fs.defaultFS=file:///, so input batch files
    // written here are readable by the same FileSystem.get(conf) in readResults.
    // -------------------------------------------------------------------------
    private BatchPrepResult prepareBatches(String inputPath, int batchSize,
                                           String runId, Configuration conf) throws Exception {
        Path     rootPath = new Path(inputPath);
        FileSystem inputFs = rootPath.getFileSystem(conf);   // honours file:/// or hdfs://
        FileSystem localFs = FileSystem.getLocal(conf);
        Path      batchDir = new Path("/tmp/etl_batches/" + runId);

        if (localFs.exists(batchDir)) localFs.delete(batchDir, true);
        localFs.mkdirs(batchDir);

        List<Path> inputFiles = collectInputFiles(inputFs, rootPath);
        List<Path> batchFiles = new ArrayList<>();

        long total    = 0;
        long malformed = 0;
        int  batchNumber       = 1;
        int  currentBatchSize  = 0;
        BufferedWriter writer  = null;

        try {
            for (Path inputFile : inputFiles) {
                try (BufferedReader br = new BufferedReader(
                        new InputStreamReader(inputFs.open(inputFile)))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        if (writer == null || currentBatchSize == batchSize) {
                            if (writer != null) writer.close();
                            Path batchFile = new Path(batchDir,
                                    String.format("batch-%05d.log", batchNumber));
                            FSDataOutputStream out = localFs.create(batchFile, true);
                            writer = new BufferedWriter(new OutputStreamWriter(out));
                            batchFiles.add(batchFile);
                            batchNumber++;
                            currentBatchSize = 0;
                        }
                        writer.write(line);
                        writer.newLine();
                        currentBatchSize++;
                        total++;
                        if (!LogParser.parse(line).isPresent()) malformed++;
                    }
                }
            }
        } finally {
            if (writer != null) writer.close();
        }

        return new BatchPrepResult(batchFiles, total, malformed);
    }

    private List<Path> collectInputFiles(FileSystem fs, Path rootPath) throws Exception {
        List<Path> inputFiles = new ArrayList<>();
        if (fs.isFile(rootPath)) {
            inputFiles.add(rootPath);
            return inputFiles;
        }
        org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus> files =
                fs.listFiles(rootPath, true);
        while (files.hasNext()) {
            org.apache.hadoop.fs.LocatedFileStatus file = files.next();
            if (!file.isFile()) continue;
            String name = file.getPath().getName();
            if (name.startsWith("_") || name.startsWith(".")) continue;
            inputFiles.add(file.getPath());
        }
        Collections.sort(inputFiles, Comparator.comparing(Path::toString));
        return inputFiles;
    }

    private static final class BatchPrepResult {
        final List<Path> batchFiles;
        final long totalRecords;
        final long malformedRecords;

        BatchPrepResult(List<Path> batchFiles, long totalRecords, long malformedRecords) {
            this.batchFiles     = batchFiles;
            this.totalRecords   = totalRecords;
            this.malformedRecords = malformedRecords;
        }
    }
}