import os
os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--packages org.apache.hadoop:hadoop-aws:3.3.4,'
    'com.amazonaws:aws-java-sdk-bundle:1.12.262 '
    'pyspark-shell'
)

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, lit, current_timestamp, row_number, when, sum as spark_sum,
    year, month, dayofweek
)
from pyspark.sql.types import StructType, StructField, StringType, DateType
from datetime import datetime

spark = SparkSession.builder \
    .appName("Employee Leave Data Processing") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

bucket                = "poc-bootcamp-capstone-project-group2"
bronze_path           = f"s3a://{bucket}/bronze/employee_leave_data/"
gold_path             = f"s3a://{bucket}/gold/employee_leave_data/"
emp_time_data_path    = f"s3a://{bucket}/gold/employee-timeframe-opt/"
holiday_calendar_path = f"s3a://{bucket}/silver/employee_leave_calendar/"

leave_schema = StructType([
    StructField("emp_id",  StringType(), True),
    StructField("date",    DateType(),   True),
    StructField("status",  StringType(), True),
])

# ── Step 1: Read full bronze CSV (contains all historical leave records) ──────
print("Reading bronze leave CSV...")
df = spark.read \
    .schema(leave_schema) \
    .option("header", True) \
    .csv(bronze_path)

# Filter out weekends and bad rows immediately
df = df.filter(~dayofweek(col("date")).isin([1, 7]))
df = df.dropna(subset=["emp_id", "date", "status"])
df = df.filter(col("status").isin(["ACTIVE", "CANCELLED"]))

# ── Step 2: Remove holidays (only exists after Jan 1st yearly run) ─────────────
try:
    holiday_df = spark.read.parquet(holiday_calendar_path) \
        .select(col("date").cast(DateType()).alias("date")).distinct()
    df = df.join(holiday_df, on="date", how="left_anti")
    print("Holiday calendar applied.")
except Exception as e:
    print(f"WARNING: Holiday calendar not found – skipping. ({e})")

# ── Step 3: Keep only active employees ─────────────────────────────────────────
try:
    emp_ids_df = spark.read.parquet(emp_time_data_path).select("emp_id").distinct()
    df = df.join(emp_ids_df, "emp_id", "left_semi")
    print("Active employee filter applied.")
except Exception as e:
    print(f"WARNING: Timeframe gold not found – skipping filter. ({e})")

today_date = datetime.utcnow().strftime('%Y-%m-%d')
df = df.withColumn("ingest_date",      lit(today_date)) \
       .withColumn("ingest_timestamp", current_timestamp())

# Split into partitions for the window function
df = df.repartition(8)

# ── Step 4: Resolve ACTIVE vs CANCELLED per (emp_id, date) ────────────────────
# NOTE: No historical merge — bronze CSV contains all records already.
# Merging with gold would double the data and cause OOM on 125MB file.
status_df = df.withColumn(
    "is_cancelled", when(col("status") == "CANCELLED", 1).otherwise(0)
).withColumn(
    "is_active", when(col("status") == "ACTIVE", 1).otherwise(0)
).groupBy("emp_id", "date").agg(
    spark_sum("is_cancelled").alias("cancelled_count"),
    spark_sum("is_active").alias("active_count")
).withColumn(
    "final_status",
    when(col("cancelled_count") >= col("active_count"), "CANCELLED").otherwise("ACTIVE")
).select("emp_id", "date", "final_status")

df = df.join(status_df, on=["emp_id", "date"], how="inner") \
       .filter(col("status") == col("final_status")) \
       .drop("final_status")

# ── Step 5: Deduplicate – keep latest ingest per (emp_id, date) ───────────────
window = Window.partitionBy("emp_id", "date") \
               .orderBy(col("ingest_timestamp").desc())

df = df.withColumn("row_num", row_number().over(window)) \
       .filter(col("row_num") == 1) \
       .drop("row_num")

# ── Step 6: Add partition columns and write to gold ───────────────────────────
df = df.withColumn("year",  year(col("date"))) \
       .withColumn("month", month(col("date")))

df.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(gold_path)

print("Employee leave data processed successfully!")
spark.stop()