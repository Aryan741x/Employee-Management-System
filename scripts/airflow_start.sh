#!/usr/bin/env bash
set -euo pipefail

export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

echo "Waiting for PostgreSQL"
MAX_RETRIES=30
RETRY=0
until python3 -c "
import psycopg2, sys
try:
    psycopg2.connect(dbname='airflow', user='postgres', password='postgres', host='postgres', port='5432').close()
    sys.exit(0)
except Exception:
    sys.exit(1)
" 2>/dev/null; do
    RETRY=$((RETRY + 1))
    if [ "$RETRY" -ge "$MAX_RETRIES" ]; then
        exit 1
    fi
    sleep 3
done

airflow db migrate
airflow connections create-default-connections || true
airflow users create --username airflow --password airflow --firstname Admin --lastname User --role Admin --email admin@example.com || true
sleep 5
airflow dags unpause employee_local_pipeline || true
airflow scheduler &
exec airflow webserver
