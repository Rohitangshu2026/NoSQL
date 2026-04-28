REGISTER 'target/etl-framework-1.0.jar';
DEFINE LogParser com.etl.pipeline.pig.LogParserUDF();
raw = LOAD '$INPUT' USING TextLoader() AS (line:chararray);
parsed = FOREACH raw GENERATE LogParser(line) AS parsed_tuple;
valid = FILTER parsed BY parsed_tuple IS NOT NULL;
flattened = FOREACH valid GENERATE
    parsed_tuple.$0 AS host:chararray,
    parsed_tuple.$4 AS resource_path:chararray,
    parsed_tuple.$7 AS bytes:long;
grouped = GROUP flattened BY resource_path;
resources = FOREACH grouped {
    unique_hosts = DISTINCT flattened.host;
    GENERATE
        group AS resource_path,
        COUNT(flattened) AS request_count,
        SUM(flattened.bytes) AS total_bytes,
        COUNT(unique_hosts) AS distinct_hosts;
};

STORE top_resources INTO '$OUTPUT' USING PigStorage('\t');
