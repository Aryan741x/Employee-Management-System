import os
# Must be set BEFORE pyspark imports so jars are on the classpath from JVM start
os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--packages org.apache.hadoop:hadoop-aws:3.3.4,'
    'com.amazonaws:aws-java-sdk-bundle:1.12.262 '
    'pyspark-shell'
)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from datetime import datetime

spark = SparkSession.builder \
    .appName("Employee Data Processing") \
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
input_path  = f"s3a://{bucket}/bronze/employee_data/"
output_path = f"s3a://{bucket}/gold/employee-data/"

today = datetime.utcnow()
year  = today.strftime('%Y')
month = today.strftime('%m')
day   = today.strftime('%d')

df = spark.read.option("header", True).csv(input_path)

if df.count() > 0:
    df_clean = (
        df.dropna(subset=["emp_id", "name", "age"])
          .filter(col("age") > 0)
          .dropDuplicates(["emp_id", "name", "age"])
          .withColumn("year",  lit(year))
          .withColumn("month", lit(month))
          .withColumn("day",   lit(day))
    )
    df_clean.write \
        .mode("append") \
        .partitionBy("year", "month", "day") \
        .parquet(output_path)
    print("Employee data processed successfully!")
else:
    print("No data found in bronze path.")

spark.stop()