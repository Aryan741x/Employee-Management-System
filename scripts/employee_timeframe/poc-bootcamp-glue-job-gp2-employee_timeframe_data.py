import os
os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--packages org.apache.hadoop:hadoop-aws:3.3.4,'
    'com.amazonaws:aws-java-sdk-bundle:1.12.262 '
    'pyspark-shell'
)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, lead, when, from_unixtime
from pyspark.sql.window import Window
import boto3
from urllib.parse import urlparse

spark = SparkSession.builder \
    .appName("Employee Timeframe Data") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

bucket           = "poc-bootcamp-capstone-project-group2"
input_path       = f"s3a://{bucket}/bronze/employee_timeframe_data/unprocessed/"
processed_prefix = "bronze/processed/"
silver_path      = f"s3a://{bucket}/silver/employee_timeframe_data/"
gold_path        = f"s3a://{bucket}/gold/employee-timeframe-opt/"

s3_client = boto3.client(
    "s3",
    endpoint_url="http://localstack:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test",
)

objects = s3_client.list_objects_v2(
    Bucket=bucket,
    Prefix="bronze/employee_timeframe_data/unprocessed/",
).get("Contents", [])

if not objects:
    print("No new employee timeframe files in unprocessed. Skipping.")
    spark.stop()
    raise SystemExit(0)

df = spark.read.option("header", True).csv(input_path)
input_files = df.inputFiles()

df = df.withColumn("start_date", col("start_date").cast("long")) \
       .withColumn("end_date",   col("end_date").cast("long")) \
       .withColumn("salary",     col("salary").cast("long"))

w = Window.partitionBy("emp_id", "start_date").orderBy(col("salary").desc())
df = df.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).drop("rn")

w2 = Window.partitionBy("emp_id").orderBy("start_date")
df = df.withColumn("next_start", lead("start_date").over(w2)) \
       .withColumn("end_date",
                   when(col("end_date").isNull(), col("next_start"))
                   .otherwise(col("end_date"))) \
       .drop("next_start")

df = df.withColumn("start_date", from_unixtime("start_date").cast("date")) \
       .withColumn("end_date",   from_unixtime("end_date").cast("date")) \
       .withColumn("status",     when(col("end_date").isNull(), "ACTIVE").otherwise("INACTIVE"))

df.write.mode("append").option("header", True).csv(silver_path)
df.write.mode("overwrite").partitionBy("status").parquet(gold_path)

s3 = boto3.resource(
    "s3",
    endpoint_url="http://localstack:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test"
)

for file_path in input_files:
    parsed   = urlparse(file_path)
    key      = parsed.path.lstrip('/')
    dest_key = key.replace("bronze/employee_timeframe_data/unprocessed/", processed_prefix)
    s3.Object(bucket, dest_key).copy_from(CopySource={"Bucket": bucket, "Key": key})
    s3.Object(bucket, key).delete()

print("Timeframe data processed!")
spark.stop()