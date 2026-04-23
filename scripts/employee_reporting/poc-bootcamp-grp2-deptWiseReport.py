import os
os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--packages org.apache.hadoop:hadoop-aws:3.3.4,'
    'com.amazonaws:aws-java-sdk-bundle:1.12.262 '
    'pyspark-shell'
)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit
from datetime import datetime

spark = SparkSession.builder \
    .appName("Department Wise Report") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.driver.memory", "1g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

bucket      = "poc-bootcamp-capstone-project-group2"
time_path   = f"s3a://{bucket}/gold/employee-timeframe-opt/"
active_path = f"s3a://{bucket}/gold/employee-timeframe-opt/status=ACTIVE/"
output_path = f"s3a://{bucket}/gold/active_emp_by_desg/"

time_df   = spark.read.parquet(time_path)
active_df = spark.read.parquet(active_path)

active_count_df = active_df.groupBy("designation") \
    .agg(count(col("emp_id")).alias("active_employees"))

designation_df = time_df.select("designation").distinct()

final_df = designation_df \
    .join(active_count_df, "designation", "left") \
    .fillna({"active_employees": 0})

today = datetime.utcnow().strftime("%Y-%m-%d")
final_df.withColumn("run_date", lit(today)) \
    .write.mode("overwrite") \
    .partitionBy("run_date") \
    .parquet(output_path)

print("Department-wise report generated!")
spark.stop()