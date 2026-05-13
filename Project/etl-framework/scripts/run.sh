#!/bin/bash
# Launcher that loads MySQL creds from config/db.properties and invokes the
# fat jar via `java -cp` (NOT `hadoop jar`, which chokes on uber-jars that
# bundle hive-jdbc).  We pull the Hadoop client classpath in so the local-mode
# ClientProtocolProvider SPI bindings resolve.
set -e

if [ ! -f "config/db.properties" ]; then
    echo "Error: config/db.properties not found."
    exit 1
fi

DB_URL=$(grep '^db.url='      config/db.properties | cut -d'=' -f2-)
DB_USER=$(grep '^db.user='    config/db.properties | cut -d'=' -f2-)
DB_PASS=$(grep '^db.password=' config/db.properties | cut -d'=' -f2-)

if ! command -v hadoop >/dev/null 2>&1; then
    echo "Error: 'hadoop' not on PATH — needed for local-mode classpath." >&2
    exit 1
fi

CP="target/etl-framework-1.0.jar:$(hadoop classpath)"

exec java -cp "$CP" com.etl.ETLRunner \
    --db-url  "$DB_URL" \
    --db-user "$DB_USER" \
    --db-pass "$DB_PASS" \
    "$@"
