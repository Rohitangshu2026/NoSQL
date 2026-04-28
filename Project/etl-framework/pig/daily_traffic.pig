REGISTER 'target/etl-framework-1.0.jar';

DEFINE LogParser com.etl.pipeline.pig.LogParserUDF();

-- Load raw log lines
raw = LOAD '$INPUT' USING TextLoader() AS (line:chararray);

-- Parse logs using UDF
parsed = FOREACH raw GENERATE LogParser(line) AS parsed_tuple;

-- Keep only successfully parsed rows
valid = FILTER parsed BY parsed_tuple IS NOT NULL;

-- Extract fields + FIX bytes ("-" → 0)
flattened = FOREACH valid GENERATE
    parsed_tuple.$1 AS log_date:chararray,
    parsed_tuple.$6 AS status_code:int,
    (parsed_tuple.$7 == '-' ? 0L : (long)parsed_tuple.$7) AS bytes:long;

-- Remove any null keys (safety)
cleaned = FILTER flattened BY log_date IS NOT NULL AND status_code IS NOT NULL;

-- Global aggregation
grouped = GROUP cleaned BY (log_date, status_code);

daily_traffic = FOREACH grouped GENERATE
    group.log_date AS log_date,
    group.status_code AS status_code,
    COUNT(cleaned) AS request_count,
    SUM(cleaned.bytes) AS total_bytes;

-- Store output
STORE daily_traffic INTO '$OUTPUT' USING PigStorage('\t');