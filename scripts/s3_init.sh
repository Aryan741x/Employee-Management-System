#!/usr/bin/env bash
set -e

echo "Waiting for LocalStack to be ready..."
until curl -s http://localstack:4566/_localstack/health | grep -q 's3' ; do
    sleep 2
done

echo "LocalStack is ready. Creating S3 bucket..."
aws --endpoint-url=http://localstack:4566 s3 mb s3://poc-bootcamp-capstone-project-group2 || true

echo "Uploading test data to Bronze layer..."
aws --endpoint-url=http://localstack:4566 s3 cp /data/employee_data.csv s3://poc-bootcamp-capstone-project-group2/bronze/employee_data/employee_data.csv
aws --endpoint-url=http://localstack:4566 s3 cp /data/employee_timeframe_data.csv s3://poc-bootcamp-capstone-project-group2/bronze/employee_timeframe_data/unprocessed/employee_timeframe_data.csv
aws --endpoint-url=http://localstack:4566 s3 cp /data/employee_leave_quota_data.csv s3://poc-bootcamp-capstone-project-group2/bronze/employee_leave_quota_data/employee_leave_quota_data.csv
aws --endpoint-url=http://localstack:4566 s3 cp /data/employee_leave_data.csv s3://poc-bootcamp-capstone-project-group2/bronze/employee_leave_data/employee_leave_data.csv
aws --endpoint-url=http://localstack:4566 s3 cp /data/employee_leave_calendar_data.csv s3://poc-bootcamp-capstone-project-group2/bronze/employee_calendar/employee_leave_calendar_data.csv
aws --endpoint-url=http://localstack:4566 s3 cp /data/marked_words.json s3://poc-bootcamp-capstone-project-group2/bronze/marked_words/marked_words.json

echo "S3 initialized successfully."
