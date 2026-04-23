import boto3
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Spark config for LocalStack
spark = SparkSession.builder \
    .appName("Step1 Processing") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# LocalStack S3 client
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test"
)

bucket = "employee-bucket"

bronze_prefix = "bronze/step-1/"
silver_prefix = "silver/employee_table/"
gold_prefix = "gold/processed/"

today = datetime.utcnow()
year = today.strftime('%Y')
month = today.strftime('%m')
day = today.strftime('%d')

partition_path = f"{silver_prefix}year={year}/month={month}/day={day}/"
input_partition_path = f"s3a://{bucket}/{partition_path}"
gold_output_path = f"s3a://{bucket}/{gold_prefix}"

# Move files (simulate partitioning)
objects = s3.list_objects_v2(Bucket=bucket, Prefix=bronze_prefix).get("Contents", [])

for obj in objects:
    key = obj["Key"]
    if "year=" in key or key.endswith("/"):
        continue

    filename = os.path.basename(key)
    new_key = f"{partition_path}{filename}"

    s3.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": key}, Key=new_key)
    s3.delete_object(Bucket=bucket, Key=key)

# Read partitioned data
df = spark.read.option("header", True).csv(input_partition_path)

df_clean = df.select("name", "age", "emp_id") \
    .dropna(subset=["emp_id", "name", "age"]) \
    .filter(col("age") > 0) \
    .dropDuplicates(["emp_id", "name", "age"])

# Write to Gold
df_clean.write \
    .mode("append") \
    .parquet(gold_output_path)

print("Step1 processing complete!")

spark.stop()