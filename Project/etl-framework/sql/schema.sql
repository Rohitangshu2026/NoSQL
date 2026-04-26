DROP TABLE IF EXISTS etl_results;
DROP TABLE IF EXISTS etl_runs;

CREATE TABLE etl_runs (
  run_id VARCHAR(36) PRIMARY KEY,
  pipeline VARCHAR(20) NOT NULL,
  batch_size INT NOT NULL,
  total_records INT NOT NULL,
  total_batches INT NOT NULL,
  malformed_count INT NOT NULL DEFAULT 0,
  runtime_ms BIGINT NOT NULL,
  executed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE etl_results (
  id INT AUTO_INCREMENT PRIMARY KEY,
  run_id VARCHAR(36) NOT NULL,
  batch_id INT NOT NULL,
  query_name VARCHAR(30) NOT NULL,
  log_date DATE DEFAULT NULL,
  status_code INT DEFAULT NULL,
  resource_path VARCHAR(512) DEFAULT NULL,
  log_hour INT DEFAULT NULL,
  request_count BIGINT DEFAULT NULL,
  total_bytes BIGINT DEFAULT NULL,
  distinct_hosts INT DEFAULT NULL,
  error_request_count INT DEFAULT NULL,
  total_request_count INT DEFAULT NULL,
  error_rate DECIMAL(7,4) DEFAULT NULL,
  distinct_error_hosts INT DEFAULT NULL,
  FOREIGN KEY (run_id) REFERENCES etl_runs(run_id),
  INDEX idx_run_query (run_id, query_name),
  INDEX idx_run_batch (run_id, batch_id)
);
