import os
os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--packages org.apache.hadoop:hadoop-aws:3.3.4,'
    'com.amazonaws:aws-java-sdk-bundle:1.12.262 '
    'pyspark-shell'
)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, year, to_date, count, round, lit
from datetime import datetime
import boto3

spark = SparkSession.builder \
    .appName("Employee Leave Usage 80 Percent") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.driver.memory", "1g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

s3_client = boto3.client(
    "s3", endpoint_url="http://localstack:4566",
    aws_access_key_id="test", aws_secret_access_key="test"
)

bucket          = "poc-bootcamp-capstone-project-group2"
leave_data_path = f"s3a://{bucket}/gold/employee_leave_data/"
quota_data_path = f"s3a://{bucket}/silver/employee_leave_quota/"
output_path     = f"s3a://{bucket}/gold/employees_80_percent_leave_used/"
tracking_path   = f"s3a://{bucket}/silver/alerted_emp_ids_tracking/"
txt_prefix      = "gold/leave_alerts/"

run_date      = "2024-12-01"
current_month = 12
current_year  = 2024

leave_df = spark.read.parquet(leave_data_path)
leave_df = leave_df.withColumn("date", to_date(col("date")))

agg_df = leave_df.filter(
    (month(col("date")) < current_month) &
    (year(col("date"))  == current_year) &
    (col("status") == "ACTIVE")
).groupBy("emp_id").agg(count("*").alias("leaves_taken"))

try:
    quota_df = spark.read.parquet(quota_data_path).filter(col("year") == current_year)
    usage_df = agg_df.join(quota_df, "emp_id", "left") \
        .withColumn("leave_percent", round((col("leaves_taken") / col("leave_quota")) * 100, 2))
    flagged_df = usage_df.filter(col("leave_percent") > 80)
except Exception as e:
    print(f"WARNING: Quota data not found – skipping 80% report. ({e})")
    spark.stop()
    raise SystemExit(0)

flagged_df.write.mode("overwrite").parquet(output_path)

try:
    existing_df  = spark.read.parquet(tracking_path)
    existing_ids = [row["emp_id"] for row in existing_df.collect()]
except Exception:
    existing_df  = None
    existing_ids = []

new_df   = flagged_df.filter(~col("emp_id").isin(existing_ids))
new_rows = new_df.collect()

for row in new_rows:
    content = (
        f"Employee ID: {row['emp_id']}\n"
        f"Leave Taken: {row['leaves_taken']}\n"
        f"Leave Quota: {row['leave_quota']}\n"
        f"Usage:       {row['leave_percent']}%\n"
        f"Date:        {run_date}\n"
    )
    s3_client.put_object(
        Bucket=bucket,
        Key=f"{txt_prefix}{row['emp_id']}_{run_date}.txt",
        Body=content.encode("utf-8")
    )

if new_rows:
    new_ids_df = spark.createDataFrame([(r["emp_id"],) for r in new_rows], ["emp_id"])
    updated    = (existing_df.union(new_ids_df).dropDuplicates(["emp_id"])
                  if existing_df else new_ids_df)
    updated.write.mode("overwrite").parquet(tracking_path)

print(f"80% report done. New alerts: {len(new_rows)}")
spark.stop()