# NASA Log ETL Framework

A multi-pipeline ETL and reporting framework for NASA HTTP web server log analytics.

## Build
```bash
mvn clean package
```

## Setup
1. Copy `config/db.properties.example` to `config/db.properties` and edit the credentials.
2. Initialize MySQL tables: `mysql -u etl -p etldb < sql/schema.sql`
3. Setup HDFS and upload data: `./scripts/setup_hdfs.sh`

## Run
```bash
./scripts/run.sh --pipeline mapreduce --input /nasa/logs --batch-size 1000
```

## Dataset notes:
- July file covers Jul 01–31 1995 (3,461,612 requests total across both files)
- August file covers Aug 04–31 1995 — NOT Aug 01–31.
  The web server was shut down Aug 01 14:52:01 to Aug 03 04:36:13 due to Hurricane Erin.
  Expect no records in this range; this is not a parsing error.
- Timestamp format is non-standard: [DAY MON DD HH:MM:SS YYYY]
  Example: [Thu Jul 01 00:00:08 1995]
