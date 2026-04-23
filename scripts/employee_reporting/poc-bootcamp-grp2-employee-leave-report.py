import os
os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--packages org.apache.hadoop:hadoop-aws:3.3.4,'
    'com.amazonaws:aws-java-sdk-bundle:1.12.262 '
    'pyspark-shell'
)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, dayofweek, when, lit
from pyspark.sql.types import StructType, StructField, DateType
from datetime import datetime

spark = SparkSession.builder \
    .appName("Employee Leave Report") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

bucket        = "poc-bootcamp-capstone-project-group2"
calendar_path = f"s3a://{bucket}/silver/employee_leave_calendar/"
leave_path    = f"s3a://{bucket}/gold/employee_leave_data/"
output_path   = f"s3a://{bucket}/gold/employee_leaves_count_info/"

try:
    leave_df = spark.read.parquet(leave_path)
    leave_df = leave_df.withColumn("leave_date", col("date").cast(DateType()))
except Exception as e:
    print(f"WARNING: Gold leave data not found – skipping. ({e})")
    spark.stop()
    raise SystemExit(0)

try:
    calendar_df = spark.read.parquet(calendar_path)
    calendar_df = calendar_df.withColumn("date", col("date").cast(DateType()))
    print("Holiday calendar loaded.")
except Exception as e:
    print(f"WARNING: No holiday calendar – treating all weekdays as working. ({e})")
    calendar_df = spark.createDataFrame([], StructType([StructField("date", DateType(), True)]))

today_str       = "2024-01-01"
end_of_year_str = "2024-12-31"

date_range = spark.sql(
    f"SELECT explode(sequence(to_date('{today_str}'), to_date('{end_of_year_str}'))) AS date"
)

weekends = date_range.withColumn("dow", dayofweek("date")) \
                     .filter(col("dow").isin([1, 7])) \
                     .select("date")

non_working  = calendar_df.select("date").union(weekends).distinct()
working_days = date_range.join(non_working, "date", "left_anti")
total_days   = working_days.count()

future_leaves = leave_df \
    .filter(col("status") == "ACTIVE") \
    .filter(col("leave_date") >= "2024-01-01") \
    .dropDuplicates(["emp_id", "leave_date"]) \
    .join(working_days, leave_df.leave_date == working_days.date, "inner")

leave_counts = future_leaves.groupBy("emp_id") \
    .agg(countDistinct("leave_date").alias("upcoming_leaves_count"))

flagged = leave_counts.withColumn(
    "leave_percent", (col("upcoming_leaves_count") / lit(total_days)) * 100
).withColumn(
    "flagged", when(col("leave_percent") > 8, "Yes").otherwise("No")
)

final_df = flagged.filter(col("flagged") == "Yes") \
    .select("emp_id", "upcoming_leaves_count") \
    .withColumn("run_date", lit(today_str))

final_df.write.mode("overwrite").partitionBy("run_date").parquet(output_path)
print("Employee leave report generated!")
spark.stop()