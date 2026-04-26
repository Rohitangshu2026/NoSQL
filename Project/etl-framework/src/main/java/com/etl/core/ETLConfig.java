package com.etl.core;

import java.util.UUID;

public class ETLConfig {
    private final String pipelineName;
    private final String inputPath;
    private final int batchSize;
    private final UUID runId;
    private final String dbUrl;
    private final String dbUser;
    private final String dbPassword;

    public ETLConfig(String pipelineName, String inputPath, int batchSize, UUID runId, String dbUrl, String dbUser, String dbPassword) {
        this.pipelineName = pipelineName;
        this.inputPath = inputPath;
        this.batchSize = batchSize;
        this.runId = runId;
        this.dbUrl = dbUrl;
        this.dbUser = dbUser;
        this.dbPassword = dbPassword;
    }

    public String getPipelineName() { return pipelineName; }
    public String getInputPath() { return inputPath; }
    public int getBatchSize() { return batchSize; }
    public UUID getRunId() { return runId; }
    public String getDbUrl() { return dbUrl; }
    public String getDbUser() { return dbUser; }
    public String getDbPassword() { return dbPassword; }
}
