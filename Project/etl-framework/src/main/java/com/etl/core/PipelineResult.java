package com.etl.core;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class PipelineResult {
    private final long totalRecords;
    private final int totalBatches;
    private final long malformedCount;
    private final long runtimeMs;
    private final Instant startedAt;
    private final Instant finishedAt;
    private final List<ResultRow> rows;
    /** batchId -> records in that batch */
    private final Map<Integer, Integer> recordsPerBatch;
    /** batchId -> malformed lines counted in that batch */
    private final Map<Integer, Integer> malformedPerBatch;

    public PipelineResult(long totalRecords, int totalBatches, long malformedCount,
                          long runtimeMs, Instant startedAt, Instant finishedAt,
                          List<ResultRow> rows) {
        this(totalRecords, totalBatches, malformedCount, runtimeMs, startedAt, finishedAt,
                rows, Collections.emptyMap(), Collections.emptyMap());
    }

    public PipelineResult(long totalRecords, int totalBatches, long malformedCount,
                          long runtimeMs, Instant startedAt, Instant finishedAt,
                          List<ResultRow> rows,
                          Map<Integer, Integer> recordsPerBatch,
                          Map<Integer, Integer> malformedPerBatch) {
        this.totalRecords      = totalRecords;
        this.totalBatches      = totalBatches;
        this.malformedCount    = malformedCount;
        this.runtimeMs         = runtimeMs;
        this.startedAt         = startedAt;
        this.finishedAt        = finishedAt;
        this.rows              = rows;
        this.recordsPerBatch   = new TreeMap<>(recordsPerBatch);
        this.malformedPerBatch = new TreeMap<>(malformedPerBatch);
    }

    public long getTotalRecords()   { return totalRecords; }
    public int  getTotalBatches()   { return totalBatches; }
    public long getMalformedCount() { return malformedCount; }
    public long getRuntimeMs()      { return runtimeMs; }
    public Instant getStartedAt()   { return startedAt; }
    public Instant getFinishedAt()  { return finishedAt; }
    public List<ResultRow> getRows(){ return rows; }

    public Map<Integer, Integer> getRecordsPerBatch()   { return recordsPerBatch; }
    public Map<Integer, Integer> getMalformedPerBatch() { return malformedPerBatch; }

    public double avgBatchSize() {
        if (totalBatches == 0) return 0.0;
        return (double) totalRecords / totalBatches;
    }
}
