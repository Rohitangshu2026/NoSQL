REGISTER 'target/etl-framework-1.0.jar';

DEFINE LogParser com.etl.pipeline.pig.LogParserUDF();

-- Load raw log lines
raw = LOAD '$INPUT' USING TextLoader() AS (line:chararray);

-- Parse logs using UDF
parsed = FOREACH raw GENERATE LogParser(line) AS parsed_tuple;

-- Keep only successfully parsed rows
valid = FILTER parsed BY parsed_tuple IS NOT NULL;

-- Extract required fields
flattened = FOREACH valid GENERATE
    parsed_tuple.$0 AS host:chararray,
    parsed_tuple.$1 AS log_date:chararray,
    parsed_tuple.$2 AS log_hour:int,
    parsed_tuple.$6 AS status_code:int;

-- Remove null keys (important for consistency with MR)
cleaned = FILTER flattened BY log_date IS NOT NULL AND log_hour IS NOT NULL;

-- GLOBAL grouping (matches MapReduce key: date|hour)
grouped = GROUP cleaned BY (log_date, log_hour);

hourly_errors = FOREACH grouped {

    -- total requests in this group
    total = COUNT(cleaned);

    -- error requests (400–599 inclusive)
    errors = FILTER cleaned BY status_code >= 400 AND status_code <= 599;

    -- distinct error hosts
    error_hosts = DISTINCT errors.host;

    GENERATE
        group.log_date AS log_date,
        group.log_hour AS log_hour,
        COUNT(errors) AS error_request_count,
        total AS total_request_count,
        (total == 0 ? 0.0 : (double)COUNT(errors) / (double)total) AS error_rate,
        COUNT(error_hosts) AS distinct_error_hosts;
};

-- Store output
STORE hourly_errors INTO '$OUTPUT' USING PigStorage('\t');