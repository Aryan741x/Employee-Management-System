import os
os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--packages org.apache.hadoop:hadoop-aws:3.3.4,'
    'com.amazonaws:aws-java-sdk-bundle:1.12.262 '
    'pyspark-shell'
)

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Print 8 Percent Anomalies") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

bucket = "poc-bootcamp-capstone-project-group2"
output_path = f"s3a://{bucket}/gold/employee_leaves_count_info/run_date=2024-01-01/"

try:
    df = spark.read.parquet(output_path)
    print("\n--- 8% LEAVE ANOMALY REPORT ---")
    df.show(20, truncate=False)
except Exception as e:
    print(f"Error reading Parquet: {e}")

spark.stop()
