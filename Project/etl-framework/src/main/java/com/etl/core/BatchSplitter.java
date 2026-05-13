package com.etl.core;

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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Splits raw NASA log input into batch files of at most {@code batchSize}
 * lines each. The batch files are named {@code batch-00001.log},
 * {@code batch-00002.log}, ... so downstream pipelines (MR, Pig) can recover
 * the {@code batch_id} from the filename.
 *
 * Per-batch record and malformed counts are tracked using
 * {@link LogParser#parse(String)} as a side-effect of streaming through the
 * input, so the metadata is exact even when the pipeline itself does its
 * own parsing.
 */
public final class BatchSplitter {

    public static final String BATCH_FILE_PREFIX = "batch-";
    public static final String BATCH_FILE_SUFFIX = ".log";

    /** Pattern used to recover the batch id from a file path. */
    public static int batchIdFromFilename(String fileName) {
        int start = fileName.indexOf(BATCH_FILE_PREFIX);
        if (start < 0) return 0;
        start += BATCH_FILE_PREFIX.length();
        int end = fileName.indexOf('.', start);
        if (end < 0) end = fileName.length();
        try {
            return Integer.parseInt(fileName.substring(start, end));
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    public static final class Result {
        public final Path batchDir;
        public final List<Path> batchFiles;
        public final long totalRecords;
        public final long malformedRecords;
        public final Map<Integer, Integer> recordsPerBatch;
        public final Map<Integer, Integer> malformedPerBatch;

        Result(Path batchDir, List<Path> batchFiles, long totalRecords, long malformedRecords,
               Map<Integer, Integer> recordsPerBatch, Map<Integer, Integer> malformedPerBatch) {
            this.batchDir         = batchDir;
            this.batchFiles       = batchFiles;
            this.totalRecords     = totalRecords;
            this.malformedRecords = malformedRecords;
            this.recordsPerBatch  = recordsPerBatch;
            this.malformedPerBatch = malformedPerBatch;
        }
    }

    /**
     * @param inputPath  root path (single file or directory of raw NCSA log files).
     * @param batchSize  max number of input log lines per batch file.
     * @param batchDir   directory to write batch-NNNNN.log files into.
     * @param conf       Hadoop configuration (input FS is resolved off this).
     */
    public static Result split(String inputPath, int batchSize, Path batchDir,
                               Configuration conf) throws Exception {
        Path rootPath = new Path(inputPath);
        FileSystem inputFs = rootPath.getFileSystem(conf);
        FileSystem localFs = FileSystem.getLocal(conf);

        if (localFs.exists(batchDir)) localFs.delete(batchDir, true);
        localFs.mkdirs(batchDir);

        List<Path> inputFiles = collectInputFiles(inputFs, rootPath);
        List<Path> batchFiles = new ArrayList<>();
        Map<Integer, Integer> recordsPerBatch = new LinkedHashMap<>();
        Map<Integer, Integer> malformedPerBatch = new LinkedHashMap<>();

        long total = 0;
        long malformed = 0;
        int  batchNumber = 0;
        int  currentBatchSize = 0;
        int  currentBatchMalformed = 0;
        BufferedWriter writer = null;

        try {
            for (Path inputFile : inputFiles) {
                try (BufferedReader br = new BufferedReader(
                        new InputStreamReader(inputFs.open(inputFile)))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        if (writer == null || currentBatchSize == batchSize) {
                            if (writer != null) {
                                writer.close();
                                recordsPerBatch.put(batchNumber, currentBatchSize);
                                malformedPerBatch.put(batchNumber, currentBatchMalformed);
                            }
                            batchNumber++;
                            currentBatchSize = 0;
                            currentBatchMalformed = 0;
                            Path bf = new Path(batchDir,
                                    String.format("%s%05d%s",
                                            BATCH_FILE_PREFIX, batchNumber, BATCH_FILE_SUFFIX));
                            FSDataOutputStream out = localFs.create(bf, true);
                            writer = new BufferedWriter(new OutputStreamWriter(out));
                            batchFiles.add(bf);
                        }
                        writer.write(line);
                        writer.newLine();
                        currentBatchSize++;
                        total++;
                        if (!LogParser.parse(line).isPresent()) {
                            malformed++;
                            currentBatchMalformed++;
                        }
                    }
                }
            }
            // Flush metadata for the final (partial) batch.
            if (writer != null) {
                recordsPerBatch.put(batchNumber, currentBatchSize);
                malformedPerBatch.put(batchNumber, currentBatchMalformed);
            }
        } finally {
            if (writer != null) writer.close();
        }

        return new Result(batchDir, batchFiles, total, malformed,
                recordsPerBatch, malformedPerBatch);
    }

    private static List<Path> collectInputFiles(FileSystem fs, Path rootPath) throws Exception {
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

    private BatchSplitter() {}
}
