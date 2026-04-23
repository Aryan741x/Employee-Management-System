import os
os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--packages org.apache.hadoop:hadoop-aws:3.3.4,'
    'com.amazonaws:aws-java-sdk-bundle:1.12.262 '
    'pyspark-shell'
)

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Employee Leave Quota") \
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
input_path  = f"s3a://{bucket}/bronze/employee_leave_quota_data/"
output_path = f"s3a://{bucket}/silver/employee_leave_quota/"

schema = StructType([
    StructField("emp_id",      IntegerType(), True),
    StructField("leave_quota", IntegerType(), True),
    StructField("year",        IntegerType(), True),
])

df = spark.read.option("header", True).schema(schema).csv(input_path)

if df.count() == 0:
    print("No data found. Skipping...")
else:
    df = df.dropDuplicates(["emp_id", "leave_quota", "year"]) \
           .filter(col("emp_id").isNotNull() &
                   col("leave_quota").isNotNull() &
                   col("year").isNotNull())
    df.write.mode("append").partitionBy("year").parquet(output_path)
    print("Leave quota processed successfully!")

spark.stop()