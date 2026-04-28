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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.sql.Date;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PigPipeline implements Pipeline {

    @Override
    public PipelineResult execute(ETLConfig config) throws Exception {
        Instant startedAt = Instant.now();
        long startTime = System.currentTimeMillis();
        BatchPrepResult batchPrep = prepareBatches(config.getInputPath(), config.getBatchSize(), config.getRunId().toString());
        long totalRecords = batchPrep.totalRecords;
        int totalBatches = batchPrep.batchFiles.size();
        long malformedCount = batchPrep.malformedRecords;
        List<ResultRow> rows = new ArrayList<>();

        String[] queries = {"daily_traffic", "top_resources", "hourly_errors"};

        PigServer pigServer = new PigServer(ExecType.MAPREDUCE);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        for (int i = 0; i < batchPrep.batchFiles.size(); i++) {
            int batchId = i + 1;
            String batchInput = batchPrep.batchFiles.get(i).toString();

            for (String query : queries) {
                String outPath = "/tmp/etl_out/" + config.getRunId() + "/batch-" + batchId + "/" + query;
                Path outputPath = new Path(outPath);
                if (fs.exists(outputPath)) {
                    fs.delete(outputPath, true);
                }

                Map<String, String> params = new HashMap<>();
                params.put("INPUT", batchInput);
                params.put("OUTPUT", outPath);

                String scriptPath = "pig/" + query + ".pig";

                pigServer.registerScript(scriptPath, params);

                if (!fs.exists(outputPath)) {
                    throw new IllegalStateException("Pig query '" + query + "' finished without creating output at " + outPath);
                }
                rows.addAll(readResults(conf, outputPath, query, batchId));
            }
        }

        rows = postProcessTopResources(rows);

        Instant finishedAt = Instant.now();
        long runtimeMs = System.currentTimeMillis() - startTime;
        return new PipelineResult(totalRecords, totalBatches, malformedCount, runtimeMs, startedAt, finishedAt, rows);
    }

    private List<ResultRow> readResults(Configuration conf, Path outPath, String queryName, int batchId) throws Exception {
        List<ResultRow> results = new ArrayList<>();
        FileSystem fs = FileSystem.get(conf);
        org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus> fileStatusListIterator = fs.listFiles(outPath, true);

        while (fileStatusListIterator.hasNext()) {
            org.apache.hadoop.fs.LocatedFileStatus fileStatus = fileStatusListIterator.next();
            if (fileStatus.getPath().getName().startsWith("part-")) {
                try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(fileStatus.getPath())))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] parts = line.split("\\t");

                        ResultRow row = new ResultRow(batchId, queryName);

                        if ("daily_traffic".equals(queryName)) {
                            row.setLogDate(Date.valueOf(parts[0]));
                            row.setStatusCode(Integer.parseInt(parts[1]));
                            row.setRequestCount(Long.parseLong(parts[2]));
                            row.setTotalBytes(Long.parseLong(parts[3]));

                        } else if ("top_resources".equals(queryName)) {
                            row.setResourcePath(parts[0]);
                            row.setRequestCount(Long.parseLong(parts[1]));
                            row.setTotalBytes(Long.parseLong(parts[2]));
                            row.setDistinctHosts(Integer.parseInt(parts[3]));

                        } else if ("hourly_errors".equals(queryName)) {
                            row.setLogDate(Date.valueOf(parts[0]));
                            row.setLogHour(Integer.parseInt(parts[1]));
                            row.setErrorRequestCount(Integer.parseInt(parts[2]));
                            row.setTotalRequestCount(Integer.parseInt(parts[3]));
                            row.setErrorRate(Double.parseDouble(parts[4]));
                            row.setDistinctErrorHosts(Integer.parseInt(parts[5]));
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

    private BatchPrepResult prepareBatches(String inputPath, int batchSize, String runId) throws Exception {
        Configuration conf = new Configuration();
        Path rootPath = new Path(inputPath);
        FileSystem fs = rootPath.getFileSystem(conf);
        FileSystem localFs = FileSystem.getLocal(conf);
        Path batchDir = new Path("/tmp/etl_batches/" + runId);

        if (localFs.exists(batchDir)) {
            localFs.delete(batchDir, true);
        }
        localFs.mkdirs(batchDir);

        List<Path> inputFiles = collectInputFiles(fs, rootPath);
        List<Path> batchFiles = new ArrayList<>();

        long total = 0;
        long malformed = 0;
        int batchNumber = 1;
        int currentBatchSize = 0;
        BufferedWriter writer = null;

        try {
            for (Path inputFile : inputFiles) {
                try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(inputFile)))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        if (writer == null || currentBatchSize == batchSize) {
                            if (writer != null) {
                                writer.close();
                            }
                            Path batchFile = new Path(batchDir, String.format("batch-%05d.log", batchNumber));
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

                        if (!LogParser.parse(line).isPresent()) {
                            malformed++;
                        }
                    }
                }
            }
        } finally {
            if (writer != null) {
                writer.close();
            }
        }

        return new BatchPrepResult(batchFiles, total, malformed);
    }

    private List<Path> collectInputFiles(FileSystem fs, Path rootPath) throws Exception {
        List<Path> inputFiles = new ArrayList<>();

        if (fs.isFile(rootPath)) {
            inputFiles.add(rootPath);
            return inputFiles;
        }

        org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus> files = fs.listFiles(rootPath, true);
        while (files.hasNext()) {
            org.apache.hadoop.fs.LocatedFileStatus file = files.next();
            if (!file.isFile()) {
                continue;
            }

            String name = file.getPath().getName();
            if (name.startsWith("_") || name.startsWith(".")) {
                continue;
            }

            inputFiles.add(file.getPath());
        }

        Collections.sort(inputFiles, Comparator.comparing(Path::toString));
        return inputFiles;
    }

    private static final class BatchPrepResult {
        private final List<Path> batchFiles;
        private final long totalRecords;
        private final long malformedRecords;

        private BatchPrepResult(List<Path> batchFiles, long totalRecords, long malformedRecords) {
            this.batchFiles = batchFiles;
            this.totalRecords = totalRecords;
            this.malformedRecords = malformedRecords;
        }
    }
}
