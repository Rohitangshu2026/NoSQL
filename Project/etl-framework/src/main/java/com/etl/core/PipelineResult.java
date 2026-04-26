package com.etl.core;

import java.util.List;

public class PipelineResult {
    private final long totalRecords;
    private final int totalBatches;
    private final long malformedCount;
    private final long runtimeMs;
    private final List<ResultRow> rows;

    public PipelineResult(long totalRecords, int totalBatches, long malformedCount, long runtimeMs, List<ResultRow> rows) {
        this.totalRecords = totalRecords;
        this.totalBatches = totalBatches;
        this.malformedCount = malformedCount;
        this.runtimeMs = runtimeMs;
        this.rows = rows;
    }

    public long getTotalRecords() { return totalRecords; }
    public int getTotalBatches() { return totalBatches; }
    public long getMalformedCount() { return malformedCount; }
    public long getRuntimeMs() { return runtimeMs; }
    public List<ResultRow> getRows() { return rows; }

    public double avgBatchSize() {
        if (totalBatches == 0) return 0.0;
        return (double) totalRecords / totalBatches;
    }
}
