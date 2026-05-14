# Video Demo Script — NASA Log ETL Framework

**Target length:** 12–15 minutes
**Recording setup:**
- All group members on camera (webcams ON, names visible).
- Three terminal windows side-by-side (or split panes):
  - **T1 — Driver:** runs the pipelines.
  - **T2 — MySQL:** keeps a `mysql` session open to inspect tables live.
  - **T3 — Tail:** optional, for `tail -f` of any log if needed.
- Browser tab with `README.md` open on GitHub (for the rendered mermaid diagrams).
- Editor open at the relevant source files (`ETLRunner.java`, `BatchSplitter.java`, `MapReducePipeline.java`, etc.).

---

## Pre-demo checklist (do this BEFORE you hit record)

```bash
# Working directory
cd ~/Developer/NoSQL/Project/etl-framework

# 1. All services up?
nc -z localhost 3306  && echo "MySQL OK"
nc -z localhost 27017 && echo "MongoDB OK"
nc -z localhost 10000 && echo "HiveServer2 OK"
which hadoop && hadoop version | head -1

# 2. Jar built?
ls -lh target/etl-framework-1.0.jar 2>/dev/null || mvn -q -DskipTests package

# 3. Schema fresh?
export MYSQL_PWD='Etl@12345'
mysql -uetl etldb < sql/schema.sql
mysql -uetl etldb -e "SHOW TABLES;"

# 4. Demo input prepared?
mkdir -p /tmp/demo
head -50000 nasa/logs/NASA_access_log_Jul95 > /tmp/demo/sample.log
wc -l /tmp/demo/sample.log     # should print 50000
```

If anything in the checklist fails, fix it before recording.

---

## The Script

> **Tip:** read the narration lines aloud naturally — don't read verbatim. Commands inside the boxes are the literal shell input.

---

### Section 1 — Introduction *(≈ 1 minute)*

**On camera:** all members briefly introduce themselves by name and which part of the project each one worked on.

**One member** says:

> "This is our end-semester project for DAS 839 — a multi-pipeline ETL and reporting framework for NASA HTTP web server logs. The same three queries run across four different execution backends — MapReduce, Apache Pig, MongoDB, and Apache Hive — and we compare them. Today we'll show the CLI, run all four pipelines, walk through MySQL to verify the results, and end with a side-by-side comparison."

---

### Section 2 — Architecture walkthrough *(≈ 2 minutes)*

Switch to the **rendered README on GitHub**.

> "The framework has three layers — a controller layer that owns the CLI and lifecycle, an execution layer where the four `Pipeline` implementations sit behind one interface, and an output layer where a shared `ResultLoader` and `Reporter` handle MySQL writes for every pipeline."

Scroll through:
1. **Class diagram** — point to `Pipeline` interface, the four implementations, the shared `BatchSplitter` / `LogParser` / `ResultRow`.
2. **Execution flow** — point to the batching layer (each backend has its own), the aggregation grain ("group by batch_id and key — same in every backend"), the load layer writing three tables.
3. **MR sequence diagram** — point to `BatchSplitter.split` writing batch-NNNNN.log files, the `FileSplit.getPath().getName() → batch_id` recovery in `LogMapper.setup()`.
4. **Mongo sequence diagram** — point to the `insertMany` batching with `batch_id` tags, and the `$group _id: {batch_id, …}` aggregation grain.
5. **Hive sequence diagram** — point to the `batch_id` as TSV column 0 and the `ROW_NUMBER() OVER (PARTITION BY batch_id …)` for per-batch top-20.

> "Notice the **same** ETL steps in every backend — only the engine differs. That's the fair-comparison guarantee."

Quick switch to the editor — open `src/main/java/com/etl/core/BatchSplitter.java`, scroll through `split()`. Then open `src/main/java/com/etl/pipeline/mapreduce/LogMapper.java`, point to the `setup()` method that pulls `batch_id` from the filename.

---

### Section 3 — Sanity check: clean MySQL, fresh build *(≈ 1 minute)*

**Terminal T2** (keep this open the whole demo):

```bash
export MYSQL_PWD='Etl@12345'
mysql -uetl etldb
```

In the MySQL prompt:

```sql
SELECT COUNT(*) FROM etl_runs;
SELECT COUNT(*) FROM etl_batches;
SELECT COUNT(*) FROM etl_results;
```

Should all be 0 (after schema reload). If not, exit and run:

```bash
mysql -uetl etldb < sql/schema.sql
```

**Terminal T1:**

```bash
export MYSQL_PWD='Etl@12345'
ls -lh target/etl-framework-1.0.jar
```

> "The fat jar is 236 MB — it bundles Hadoop, Pig, Mongo driver, and the Hive JDBC client. One jar, four backends."

---

### Section 4 — Run 1: MapReduce + Q1 (Daily Traffic) *(≈ 2 minutes)*

**Terminal T1:**

```bash
./scripts/run.sh \
  --pipeline mapreduce \
  --input file:///tmp/demo/sample.log \
  --batch-size 10000 \
  --query q1
```

While it runs, narrate:

> "50,000 log lines, batch size 10,000 — so we expect 5 batches. The MapReduce pipeline first chops the raw NCSA logs into batch-00001.log through batch-00005.log files. Each batch is fed as a separate input split, and the mapper recovers the batch id from the file name. The reducer key is `(batch_id, log_date, status_code)`, so MR aggregates **per batch**."

When the run finishes, **point to the printed report** — Pipeline, Query, Total Records, Total Batches, Avg Batch Size, Runtime.

Then **inspect MySQL** in T2:

```sql
-- The run we just did
SELECT pipeline, query_name, total_records, total_batches, avg_batch_size, runtime_ms
FROM etl_runs ORDER BY executed_at DESC LIMIT 1;

-- Per-batch row counts
SELECT * FROM etl_batches
WHERE run_id = (SELECT run_id FROM etl_runs ORDER BY executed_at DESC LIMIT 1);

-- Per-batch aggregates
SELECT batch_id, log_date, status_code, request_count, total_bytes
FROM etl_results
WHERE run_id = (SELECT run_id FROM etl_runs ORDER BY executed_at DESC LIMIT 1)
ORDER BY batch_id, status_code;
```

> "Notice every result row carries a real `batch_id` — not a placeholder. `SUM(records_in_batch)` equals `total_records`, batches are 1 through 5, and each `(batch_id, status_code)` combination is its own row."

---

### Section 5 — Run 2: Pig + Q2 (Top Resources) *(≈ 2 minutes)*

**Terminal T1:**

```bash
./scripts/run.sh \
  --pipeline pig \
  --input file:///tmp/demo/sample.log \
  --batch-size 10000 \
  --query q2
```

Narrate while it runs:

> "Pig uses the same `BatchSplitter` to chop the input. Then for each batch file we register and run `pig/top_resources.pig` against just that batch — the Pig Latin itself doesn't know about batching, it just aggregates over its input. Java tags every output row with the batch id it came from, and keeps the top 20 per batch."

When done, in T2:

```sql
-- The Pig run
SELECT pipeline, query_name, total_records, total_batches, avg_batch_size, runtime_ms
FROM etl_runs ORDER BY executed_at DESC LIMIT 1;

-- Top 5 resources from each batch (Q2)
SELECT batch_id, resource_path, request_count, distinct_hosts
FROM etl_results
WHERE run_id = (SELECT run_id FROM etl_runs ORDER BY executed_at DESC LIMIT 1)
  AND query_name = 'top_resources'
ORDER BY batch_id, request_count DESC
LIMIT 25;
```

> "Each batch has its own top-20. The global rollup in the report uses SUM across batches to approximate the global top 20."

---

### Section 6 — Run 3: MongoDB + Q3 (Hourly Errors) *(≈ 2 minutes)*

**Terminal T1:**

```bash
./scripts/run.sh \
  --pipeline mongodb \
  --input file:///tmp/demo/sample.log \
  --batch-size 10000 \
  --query q3
```

Narrate:

> "MongoDB is different — instead of writing batch files, we parse in Java and bulk-insert into a per-run collection. Every document is tagged with the batch id of its slice. Q3 is a single `aggregate()` call that groups on `(batch_id, log_date, log_hour)` and uses `$cond` to count only the 4xx/5xx records."

When done, in T2:

```sql
-- The Mongo run
SELECT pipeline, query_name, total_records, total_batches, avg_batch_size, runtime_ms
FROM etl_runs ORDER BY executed_at DESC LIMIT 1;

-- Per-batch hourly error analysis
SELECT batch_id, log_date, log_hour, error_request_count, total_request_count,
       error_rate, distinct_error_hosts
FROM etl_results
WHERE run_id = (SELECT run_id FROM etl_runs ORDER BY executed_at DESC LIMIT 1)
  AND query_name = 'hourly_errors'
ORDER BY batch_id, log_hour;
```

Optional — show the per-run Mongo collection got cleaned up:

```bash
# In T1
mongosh --quiet etl_logs --eval 'db.getCollectionNames().filter(n => n.startsWith("etl_logs_"))'
```

> "Empty — the pipeline drops its temp collection on exit."

---

### Section 7 — Run 4: Hive + all queries *(≈ 2 minutes)*

**Terminal T1:**

```bash
./scripts/run.sh \
  --pipeline hive \
  --input file:///tmp/demo/sample.log \
  --batch-size 10000 \
  --query all
```

Narrate:

> "Hive runs all three queries this time. The pipeline parses logs into TSV files with batch_id as column 0, creates an external Hive table, and then HiveQL groups by batch_id everywhere. For top-20-per-batch we use `ROW_NUMBER() OVER (PARTITION BY batch_id ORDER BY COUNT(*) DESC)` — the window function puts the per-batch selection entirely on the Hive side."

When done, in T2:

```sql
-- The Hive run
SELECT pipeline, query_name, total_records, total_batches, avg_batch_size, runtime_ms
FROM etl_runs ORDER BY executed_at DESC LIMIT 1;

-- Result row count broken down by query
SELECT query_name, COUNT(*) AS rows_stored
FROM etl_results
WHERE run_id = (SELECT run_id FROM etl_runs ORDER BY executed_at DESC LIMIT 1)
GROUP BY query_name;
```

> "All three queries populated in one run — 12 daily_traffic rows, 100 top_resources rows (5 batches × 20), and however many distinct (date, hour) buckets the hourly_errors picked up."

---

### Section 8 — Comparison: same answer, different engines *(≈ 2 minutes)*

This is the **money slide** — it proves equivalence.

In T2:

```sql
-- All four runs side by side
SELECT pipeline, query_name, total_records, total_batches,
       avg_batch_size, runtime_ms
FROM etl_runs
WHERE executed_at >= (NOW() - INTERVAL 30 MINUTE)
ORDER BY executed_at;
```

> "Same 50,000 records, same 5 batches, same average batch size. Runtime is the interesting axis — that's the systems comparison."

Now show the **numerical answers** agree. For Q1 (daily_traffic) we ran it on MR; but we can compute the global rollup of any pipeline's data:

```sql
-- Q1 daily_traffic global rollup — should match what MR computed
SELECT log_date, status_code,
       SUM(request_count) AS total_requests,
       SUM(total_bytes)   AS total_bytes
FROM etl_results
WHERE run_id = (SELECT run_id FROM etl_runs
                WHERE pipeline = 'hive' AND query_name = 'all'
                ORDER BY executed_at DESC LIMIT 1)
  AND query_name = 'daily_traffic'
GROUP BY log_date, status_code
ORDER BY log_date, status_code;
```

```sql
-- And the same query against the MR run
SELECT log_date, status_code,
       SUM(request_count) AS total_requests,
       SUM(total_bytes)   AS total_bytes
FROM etl_results
WHERE run_id = (SELECT run_id FROM etl_runs
                WHERE pipeline = 'mapreduce' AND query_name = 'daily_traffic'
                ORDER BY executed_at DESC LIMIT 1)
  AND query_name = 'daily_traffic'
GROUP BY log_date, status_code
ORDER BY log_date, status_code;
```

> "Identical numbers. That's pipeline equivalence — the same problem definition, the same answers, expressed in four different paradigms."

---

### Section 9 — MySQL schema tour *(≈ 1 minute)*

In T2:

```sql
SHOW TABLES;
DESCRIBE etl_runs;
DESCRIBE etl_batches;
DESCRIBE etl_results;
```

Point out:
- `etl_runs` has `query_name`, `total_batches`, `avg_batch_size`, `malformed_count`, `runtime_ms`, `executed_at` — every metric the evaluation rubric asks for.
- `etl_batches` carries per-batch records and malformed counts, keyed by `(run_id, batch_id)`.
- `etl_results` is the unified results table, sparse schema, with a real `batch_id` per row.

---

### Section 10 — (Optional) Full dataset, full sweep *(≈ 1 minute, if time)*

> "What we just showed was 50,000 records for speed. The framework runs the full 3.4 million-record NASA dataset the same way — just larger batch size:"

```bash
./scripts/run.sh \
  --pipeline mapreduce \
  --input file://$PWD/nasa/logs \
  --batch-size 100000 \
  --query all
```

Then show:

```sql
SELECT pipeline, query_name, total_records, total_batches, runtime_ms
FROM etl_runs ORDER BY executed_at DESC LIMIT 1;
```

(Don't run this live during the recording unless you have time to spare — show a screenshot of a previous full-dataset run instead.)

---

### Section 11 — Closing *(≈ 30 seconds)*

**All on camera.** One member wraps up:

> "Recap — four pipelines, three queries, twelve combinations, all sharing the same parsing, batching, loading, and reporting code. Every result row in `etl_results` carries a real `batch_id`, every batch is captured in `etl_batches`, and the four backends produce the same answers when rolled up. The full README, the source, the schema, and this demo script are all in our Git repo. Thanks for watching."

Show the GitHub repo URL on screen for ~5 seconds.

---

## Quick-reference: full copy-paste sequence

For convenience, here is the entire on-camera command sequence in order. If something goes wrong mid-demo, you can re-execute from this block.

```bash
# === setup ===
cd ~/Developer/NoSQL/Project/etl-framework
export MYSQL_PWD='Etl@12345'

# fresh schema + small input
mysql -uetl etldb < sql/schema.sql
mkdir -p /tmp/demo
head -50000 nasa/logs/NASA_access_log_Jul95 > /tmp/demo/sample.log
wc -l /tmp/demo/sample.log

# === Section 4: MR + Q1 ===
./scripts/run.sh --pipeline mapreduce --input file:///tmp/demo/sample.log --batch-size 10000 --query q1
mysql -uetl etldb -e "SELECT pipeline, query_name, total_records, total_batches, avg_batch_size, runtime_ms FROM etl_runs ORDER BY executed_at DESC LIMIT 1;"
mysql -uetl etldb -e "SELECT * FROM etl_batches WHERE run_id = (SELECT run_id FROM etl_runs ORDER BY executed_at DESC LIMIT 1);"
mysql -uetl etldb -e "SELECT batch_id, log_date, status_code, request_count, total_bytes FROM etl_results WHERE run_id = (SELECT run_id FROM etl_runs ORDER BY executed_at DESC LIMIT 1) ORDER BY batch_id, status_code;"

# === Section 5: Pig + Q2 ===
./scripts/run.sh --pipeline pig --input file:///tmp/demo/sample.log --batch-size 10000 --query q2
mysql -uetl etldb -e "SELECT batch_id, resource_path, request_count, distinct_hosts FROM etl_results WHERE run_id = (SELECT run_id FROM etl_runs ORDER BY executed_at DESC LIMIT 1) AND query_name = 'top_resources' ORDER BY batch_id, request_count DESC LIMIT 25;"

# === Section 6: Mongo + Q3 ===
./scripts/run.sh --pipeline mongodb --input file:///tmp/demo/sample.log --batch-size 10000 --query q3
mysql -uetl etldb -e "SELECT batch_id, log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts FROM etl_results WHERE run_id = (SELECT run_id FROM etl_runs ORDER BY executed_at DESC LIMIT 1) AND query_name = 'hourly_errors' ORDER BY batch_id, log_hour;"

# === Section 7: Hive + all ===
./scripts/run.sh --pipeline hive --input file:///tmp/demo/sample.log --batch-size 10000 --query all
mysql -uetl etldb -e "SELECT query_name, COUNT(*) AS rows_stored FROM etl_results WHERE run_id = (SELECT run_id FROM etl_runs ORDER BY executed_at DESC LIMIT 1) GROUP BY query_name;"

# === Section 8: comparison ===
mysql -uetl etldb -e "SELECT pipeline, query_name, total_records, total_batches, avg_batch_size, runtime_ms FROM etl_runs WHERE executed_at >= (NOW() - INTERVAL 30 MINUTE) ORDER BY executed_at;"

# === Section 9: schema tour ===
mysql -uetl etldb -e "SHOW TABLES;"
mysql -uetl etldb -e "DESCRIBE etl_runs; DESCRIBE etl_batches; DESCRIBE etl_results;"
```

---

## Recording tips

- **Speak slowly and clearly.** The evaluator will pause and rewind.
- **Don't read the screen monotonically.** Explain *what* a column means, not just that it exists.
- **Zoom in** on the terminal font (≥ 14pt) so the result rows are legible in the video.
- **Name the file you're running** every time you press Enter. E.g. "Now we run the Pig pipeline against the same 50k-line sample, with batch size 10,000."
- **All members on cam** the whole time. Take turns narrating each section so everyone is heard.
- **One full take is fine** — if you fumble, just say "let me re-run that" and continue. The evaluator wants to see the system work, not a polished movie.
- If a pipeline takes > 30 seconds, **edit out the wait** in post — keep only the start ("now we run …") and the end (the report output).
- Final video file size: aim for < 20 MB if possible; otherwise upload to YouTube unlisted and link from the report.
