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
    private final String selectedQuery;   // "daily_traffic" | "top_resources" | "hourly_errors" | "all"
    private final String mongoUri;        // used by MongoDBPipeline
    private final String mongoDatabase;   // used by MongoDBPipeline
    private final String hiveJdbcUrl;     // used by HivePipeline
    private final String hiveUser;        // used by HivePipeline (optional)
    private final String hivePassword;    // used by HivePipeline (optional)

    public ETLConfig(String pipelineName, String inputPath, int batchSize, UUID runId,
                     String dbUrl, String dbUser, String dbPassword,
                     String selectedQuery,
                     String mongoUri, String mongoDatabase,
                     String hiveJdbcUrl, String hiveUser, String hivePassword) {
        this.pipelineName  = pipelineName;
        this.inputPath     = inputPath;
        this.batchSize     = batchSize;
        this.runId         = runId;
        this.dbUrl         = dbUrl;
        this.dbUser        = dbUser;
        this.dbPassword    = dbPassword;
        this.selectedQuery = (selectedQuery == null || selectedQuery.isEmpty()) ? "all" : selectedQuery;
        this.mongoUri      = mongoUri;
        this.mongoDatabase = mongoDatabase;
        this.hiveJdbcUrl   = hiveJdbcUrl;
        this.hiveUser      = hiveUser;
        this.hivePassword  = hivePassword;
    }

    public String getPipelineName()  { return pipelineName; }
    public String getInputPath()     { return inputPath; }
    public int    getBatchSize()     { return batchSize; }
    public UUID   getRunId()         { return runId; }
    public String getDbUrl()         { return dbUrl; }
    public String getDbUser()        { return dbUser; }
    public String getDbPassword()    { return dbPassword; }
    public String getSelectedQuery() { return selectedQuery; }
    public String getMongoUri()      { return mongoUri; }
    public String getMongoDatabase() { return mongoDatabase; }
    public String getHiveJdbcUrl()   { return hiveJdbcUrl; }
    public String getHiveUser()      { return hiveUser; }
    public String getHivePassword()  { return hivePassword; }

    public boolean runsQuery(String queryName) {
        return "all".equalsIgnoreCase(selectedQuery) || selectedQuery.equalsIgnoreCase(queryName);
    }
}
