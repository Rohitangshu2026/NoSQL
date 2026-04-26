# NASA Log ETL Framework

A multi-pipeline ETL and reporting framework for NASA HTTP web server log analytics. This project supports pluggable pipelines (MapReduce and Pig implementations provided) to parse, aggregate, and store insights from web server logs into a relational database.

## Architecture & Flow

### Class Architecture

The framework relies on a core set of interfaces and models to abstract the execution details from the orchestrator (`ETLRunner`).

```mermaid
classDiagram
    class ETLRunner {
        +main(String[] args)
    }
    class PipelineFactory {
        +create(String name) Pipeline
    }
    class Pipeline {
        <<interface>>
        +execute(ETLConfig config) PipelineResult
    }
    class ETLConfig {
        -String pipelineName
        -String inputPath
        -int batchSize
        -UUID runId
    }
    class PipelineResult {
        -long totalRecords
        -int totalBatches
        -long malformedCount
        -long runtimeMs
        -List~ResultRow~ rows
    }
    class ResultRow {
        -int batchId
        -String queryName
        -long requestCount
        -long totalBytes
    }
    class ResultLoader {
        +load(ETLConfig config, PipelineResult result)
    }
    class Reporter {
        +printReport(ETLConfig config, PipelineResult result)
    }
    
    ETLRunner --> PipelineFactory : uses
    ETLRunner --> Pipeline : executes
    ETLRunner --> ResultLoader : saves
    ETLRunner --> Reporter : prints
    PipelineFactory ..> Pipeline : creates
    Pipeline ..> PipelineResult : returns
    PipelineResult "1" *-- "*" ResultRow : contains
```

### Execution Flow (Activity Diagram)

The following activity diagram illustrates the overall lifecycle of a single ETL run:

```mermaid
flowchart TD
    A[Start ETLRunner] --> B[Parse CLI Arguments]
    B --> C[Create ETLConfig with Run ID]
    C --> D{Pipeline Selected?}
    
    D -- "mapreduce" --> E[Instantiate MapReducePipeline]
    D -- "pig" --> F[Instantiate PigPipeline]
    
    E --> G[Run Hadoop MR Jobs]
    F --> H[Run Pig Latin Scripts]
    
    G --> I[Parse HDFS Output to ResultRow]
    H --> I
    
    I --> J[Construct PipelineResult]
    J --> K[ResultLoader: JDBC Batch Insert to MySQL]
    K --> L[Reporter: Print Analytics Summary]
    L --> M[End]
```

### MapReduce Pipeline Sequence

A closer look at how the MapReduce pipeline specifically handles the distributed execution:

```mermaid
sequenceDiagram
    participant Runner as ETLRunner
    participant MR as MapReducePipeline
    participant HDFS as Hadoop HDFS
    participant DB as MySQL DB

    Runner->>MR: execute(config)
    
    rect rgb(240, 248, 255)
    Note over MR,HDFS: Sequential Job Execution
    MR->>HDFS: Job 1: daily_traffic
    HDFS-->>MR: Job 1 Complete
    MR->>HDFS: Job 2: top_resources
    HDFS-->>MR: Job 2 Complete
    MR->>HDFS: Job 3: hourly_errors
    HDFS-->>MR: Job 3 Complete
    end
    
    MR->>HDFS: Read output part files
    HDFS-->>MR: Raw tab-separated output
    MR->>MR: Post-process (e.g. limit top 20 per batch)
    MR-->>Runner: PipelineResult (List~ResultRow~)
    
    Runner->>DB: ResultLoader.load()
    DB-->>Runner: Transaction Committed
```

## Build
```bash
mvn clean package
```

## Setup
1. Copy `config/db.properties.example` to `config/db.properties` and edit the credentials.
2. Initialize MySQL tables: 
   ```bash
   mysql -u etl -p etldb < sql/schema.sql
   ```
3. Setup HDFS and upload data: 
   ```bash
   ./scripts/setup_hdfs.sh
   # Follow the printed instructions to upload your logs
   ```

## Run
Use the `run.sh` script to execute the pipeline. The wrapper automatically injects the compiled JAR and database credentials.

```bash
./scripts/run.sh --pipeline mapreduce --input /nasa/logs --batch-size 1000
```
*You can swap `--pipeline mapreduce` with `--pipeline pig`.*

## Dataset notes:
- July file covers Jul 01–31 1995 (3,461,612 requests total across both files)
- August file covers Aug 04–31 1995 — NOT Aug 01–31.
  The web server was shut down Aug 01 14:52:01 to Aug 03 04:36:13 due to Hurricane Erin.
  Expect no records in this range; this is not a parsing error.
- Timestamp format is non-standard: `[DAY MON DD HH:MM:SS YYYY]`
  Example: `[Thu Jul 01 00:00:08 1995]`
