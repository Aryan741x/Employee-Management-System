import os
os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--packages org.apache.hadoop:hadoop-aws:3.3.4,'
    'com.amazonaws:aws-java-sdk-bundle:1.12.262 '
    'pyspark-shell'
)

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import col, year

spark = SparkSession.builder \
    .appName("Employee Leave Calendar") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.driver.memory", "512m") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

bucket      = "poc-bootcamp-capstone-project-group2"
bronze_path = f"s3a://{bucket}/bronze/employee_calendar/"
silver_path = f"s3a://{bucket}/silver/employee_leave_calendar/"

schema = StructType([
    StructField("reason", StringType(),    True),
    StructField("date",   TimestampType(), True),
])

df = spark.read.format("csv").schema(schema).load(bronze_path)

if df.count() == 0:
    print("No data found. Skipping...")
else:
    df = df.dropDuplicates().withColumn("year", year(col("date")))
    df.write.mode("append").partitionBy("year").parquet(silver_path)
    print("Employee leave calendar processed!")

spark.stop()