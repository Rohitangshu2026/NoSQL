REGISTER 'target/etl-framework-1.0.jar';

DEFINE LogParser com.etl.pipeline.pig.LogParserUDF();

-- Load raw log lines
raw = LOAD '$INPUT' USING TextLoader() AS (line:chararray);

-- Parse logs
parsed = FOREACH raw GENERATE LogParser(line) AS parsed_tuple;

-- Keep only valid parsed rows
valid = FILTER parsed BY parsed_tuple IS NOT NULL;

-- Extract fields + FIX bytes ("-" → 0)
flattened = FOREACH valid GENERATE
    parsed_tuple.$0 AS host:chararray,
    parsed_tuple.$4 AS resource_path:chararray,
    (parsed_tuple.$7 == '-' ? 0L : (long)parsed_tuple.$7) AS bytes:long;

-- Remove null resource paths (important for consistency)
cleaned = FILTER flattened BY resource_path IS NOT NULL;

-- Global aggregation
grouped = GROUP cleaned BY resource_path;

top_resources = FOREACH grouped {
    unique_hosts = DISTINCT cleaned.host;
    GENERATE
        group AS resource_path,
        COUNT(cleaned) AS request_count,
        SUM(cleaned.bytes) AS total_bytes,
        COUNT(unique_hosts) AS distinct_hosts;
};

-- ORDER BY intentionally omitted due to Pig 0.17 + Java 11 issue.
-- Sorting + top-20 selection is handled in Java (PigPipeline).

STORE top_resources INTO '$OUTPUT' USING PigStorage('\t');