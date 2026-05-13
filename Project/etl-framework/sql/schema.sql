DROP TABLE IF EXISTS etl_results;
DROP TABLE IF EXISTS etl_batches;
DROP TABLE IF EXISTS etl_runs;

CREATE TABLE etl_runs (
  run_id VARCHAR(36) PRIMARY KEY,
  pipeline VARCHAR(20) NOT NULL,
  query_name VARCHAR(30) NOT NULL DEFAULT 'all',
  batch_size INT NOT NULL,
  total_records BIGINT NOT NULL,
  total_batches INT NOT NULL,
  avg_batch_size DECIMAL(15,2) NOT NULL DEFAULT 0,
  malformed_count BIGINT NOT NULL DEFAULT 0,
  runtime_ms BIGINT NOT NULL,
  executed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- One row per batch produced during an ETL run.
CREATE TABLE etl_batches (
  run_id VARCHAR(36) NOT NULL,
  batch_id INT NOT NULL,
  records_in_batch INT NOT NULL,
  malformed_in_batch INT NOT NULL DEFAULT 0,
  PRIMARY KEY (run_id, batch_id),
  FOREIGN KEY (run_id) REFERENCES etl_runs(run_id)
);

CREATE TABLE etl_results (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  pipeline VARCHAR(20) NOT NULL,
  run_id VARCHAR(36) NOT NULL,
  batch_id INT NOT NULL,
  executed_at TIMESTAMP NOT NULL,
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
  INDEX idx_run_query       (run_id, query_name),
  INDEX idx_run_query_batch (run_id, query_name, batch_id),
  INDEX idx_run_batch       (run_id, batch_id)
);
