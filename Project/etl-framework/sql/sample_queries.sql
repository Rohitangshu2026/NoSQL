-- Query 1: Daily Traffic
SELECT log_date, status_code, request_count, total_bytes
FROM etl_results
WHERE query_name = 'daily_traffic'
ORDER BY log_date, status_code;

-- Query 2: Top Resources
SELECT resource_path, request_count, total_bytes, distinct_hosts
FROM etl_results
WHERE query_name = 'top_resources'
ORDER BY request_count DESC
LIMIT 20;

-- Query 3: Hourly Errors
SELECT log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts
FROM etl_results
WHERE query_name = 'hourly_errors'
ORDER BY log_date, log_hour;
