package com.etl.core;

public class ResultRow {
    private int batchId;
    private String queryName;
    private java.sql.Date logDate;
    private Integer statusCode;
    private String resourcePath;
    private Integer logHour;
    private Long requestCount;
    private Long totalBytes;
    private Integer distinctHosts;
    private Integer errorRequestCount;
    private Integer totalRequestCount;
    private Double errorRate;
    private Integer distinctErrorHosts;

    public ResultRow(int batchId, String queryName) {
        this.batchId = batchId;
        this.queryName = queryName;
    }

    // Getters and Setters
    public int getBatchId() { return batchId; }
    public void setBatchId(int batchId) { this.batchId = batchId; }

    public String getQueryName() { return queryName; }
    public void setQueryName(String queryName) { this.queryName = queryName; }

    public java.sql.Date getLogDate() { return logDate; }
    public void setLogDate(java.sql.Date logDate) { this.logDate = logDate; }

    public Integer getStatusCode() { return statusCode; }
    public void setStatusCode(Integer statusCode) { this.statusCode = statusCode; }

    public String getResourcePath() { return resourcePath; }
    public void setResourcePath(String resourcePath) { this.resourcePath = resourcePath; }

    public Integer getLogHour() { return logHour; }
    public void setLogHour(Integer logHour) { this.logHour = logHour; }

    public Long getRequestCount() { return requestCount; }
    public void setRequestCount(Long requestCount) { this.requestCount = requestCount; }

    public Long getTotalBytes() { return totalBytes; }
    public void setTotalBytes(Long totalBytes) { this.totalBytes = totalBytes; }

    public Integer getDistinctHosts() { return distinctHosts; }
    public void setDistinctHosts(Integer distinctHosts) { this.distinctHosts = distinctHosts; }

    public Integer getErrorRequestCount() { return errorRequestCount; }
    public void setErrorRequestCount(Integer errorRequestCount) { this.errorRequestCount = errorRequestCount; }

    public Integer getTotalRequestCount() { return totalRequestCount; }
    public void setTotalRequestCount(Integer totalRequestCount) { this.totalRequestCount = totalRequestCount; }

    public Double getErrorRate() { return errorRate; }
    public void setErrorRate(Double errorRate) { this.errorRate = errorRate; }

    public Integer getDistinctErrorHosts() { return distinctErrorHosts; }
    public void setDistinctErrorHosts(Integer distinctErrorHosts) { this.distinctErrorHosts = distinctErrorHosts; }
}
