REGISTER 'target/etl-framework-1.0.jar';
DEFINE LogParser com.etl.pipeline.pig.LogParserUDF();
raw = LOAD '$INPUT' USING TextLoader() AS (line:chararray);
parsed = FOREACH raw GENERATE LogParser(line) AS parsed_tuple;
valid = FILTER parsed BY parsed_tuple IS NOT NULL;
flattened = FOREACH valid GENERATE
    parsed_tuple.$1 AS log_date:chararray,
    parsed_tuple.$6 AS status_code:int,
    parsed_tuple.$7 AS bytes:long;
grouped = GROUP flattened BY (log_date, status_code);
daily_traffic = FOREACH grouped GENERATE
    group.log_date AS log_date,
    group.status_code AS status_code,
    COUNT(flattened) AS request_count,
    SUM(flattened.bytes) AS total_bytes;
STORE daily_traffic INTO '$OUTPUT' USING PigStorage('\t');
