package com.etl.core;

public class LogRecord {
    private final String host;
    private final String logDate;
    private final int logHour;
    private final String method;
    private final String resourcePath;
    private final String protocol;
    private final int statusCode;
    private final long bytes;

    public LogRecord(String host, String logDate, int logHour, String method, String resourcePath, String protocol, int statusCode, long bytes) {
        this.host = host;
        this.logDate = logDate;
        this.logHour = logHour;
        this.method = method;
        this.resourcePath = resourcePath;
        this.protocol = protocol;
        this.statusCode = statusCode;
        this.bytes = bytes;
    }

    public String getHost() { return host; }
    public String getLogDate() { return logDate; }
    public int getLogHour() { return logHour; }
    public String getMethod() { return method; }
    public String getResourcePath() { return resourcePath; }
    public String getProtocol() { return protocol; }
    public int getStatusCode() { return statusCode; }
    public long getBytes() { return bytes; }
}
