package com.etl.core;

import java.time.Instant;
import java.util.List;

public class PipelineResult {
    private final long totalRecords;
    private final int totalBatches;
    private final long malformedCount;
    private final long runtimeMs;
    private final Instant startedAt;
    private final Instant finishedAt;
    private final List<ResultRow> rows;

    public PipelineResult(long totalRecords, int totalBatches, long malformedCount,
                          long runtimeMs, Instant startedAt, Instant finishedAt,
                          List<ResultRow> rows) {
        this.totalRecords  = totalRecords;
        this.totalBatches  = totalBatches;
        this.malformedCount = malformedCount;
        this.runtimeMs     = runtimeMs;
        this.startedAt     = startedAt;
        this.finishedAt    = finishedAt;
        this.rows          = rows;
    }

    public long getTotalRecords()   { return totalRecords; }
    public int getTotalBatches()    { return totalBatches; }
    public long getMalformedCount() { return malformedCount; }
    public long getRuntimeMs()      { return runtimeMs; }
    public Instant getStartedAt()   { return startedAt; }
    public Instant getFinishedAt()  { return finishedAt; }
    public List<ResultRow> getRows(){ return rows; }

    public double avgBatchSize() {
        if (totalBatches == 0) return 0.0;
        return (double) totalRecords / totalBatches;
    }
}