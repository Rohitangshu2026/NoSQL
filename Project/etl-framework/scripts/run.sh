#!/bin/bash
set -e

if [ ! -f "config/db.properties" ]; then
    echo "Error: config/db.properties not found."
    exit 1
fi

DB_URL=$(grep '^db.url=' config/db.properties | cut -d'=' -f2-)
DB_USER=$(grep '^db.user=' config/db.properties | cut -d'=' -f2-)
DB_PASS=$(grep '^db.password=' config/db.properties | cut -d'=' -f2-)

hadoop jar target/etl-framework-1.0.jar com.etl.ETLRunner \
    --db-url "$DB_URL" \
    --db-user "$DB_USER" \
    --db-pass "$DB_PASS" \
    "$@"
