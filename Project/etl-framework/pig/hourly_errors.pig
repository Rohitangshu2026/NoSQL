REGISTER 'target/etl-framework-1.0.jar';
DEFINE LogParser com.etl.pipeline.pig.LogParserUDF();
raw = LOAD '$INPUT' USING TextLoader() AS (line:chararray);
parsed = FOREACH raw GENERATE LogParser(line) AS parsed_tuple;
valid = FILTER parsed BY parsed_tuple IS NOT NULL;
flattened = FOREACH valid GENERATE
    parsed_tuple.$0 AS host:chararray,
    parsed_tuple.$1 AS log_date:chararray,
    parsed_tuple.$2 AS log_hour:int,
    parsed_tuple.$6 AS status_code:int;
grouped = GROUP flattened BY (log_date, log_hour);
hourly_errors = FOREACH grouped {
    errors = FILTER flattened BY status_code >= 400 AND status_code <= 599;
    error_hosts = DISTINCT errors.host;
    GENERATE
        group.log_date AS log_date,
        group.log_hour AS log_hour,
        COUNT(errors) AS error_request_count,
        COUNT(flattened) AS total_request_count,
        (double)COUNT(errors) / (double)COUNT(flattened) AS error_rate,
        COUNT(error_hosts) AS distinct_error_hosts;
};
STORE hourly_errors INTO '$OUTPUT' USING PigStorage('\t');
