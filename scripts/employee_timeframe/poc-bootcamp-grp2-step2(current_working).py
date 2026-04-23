import boto3
import os
import datetime
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, to_date, from_unixtime, when, row_number, lit, lead
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

spark = SparkSession.builder \
    .appName("Step2 Processing") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

bucket = "employee-bucket"

bronze_prefix = "bronze/step-2/"
processed_path = f"s3a://{bucket}/silver/employee_timeframe_processed/"
output_path = f"s3a://{bucket}/gold/employee_timeframe_output/"
input_path = f"s3a://{bucket}/{bronze_prefix}"

schema = StructType([
    StructField("emp_id", StringType(), True),
    StructField("designation", StringType(), True),
    StructField("start_date", LongType(), True),
    StructField("end_date", DoubleType(), True),
    StructField("salary", DoubleType(), True),
])

df = spark.read.option("header", True).schema(schema).csv(input_path)

if df.count() == 0:
    raise Exception("No data found")

df = df.withColumn("start_date", to_date(from_unixtime(col("start_date")))) \
       .withColumn("end_date", to_date(from_unixtime(col("end_date"))))

w = Window.partitionBy("emp_id", "start_date", "end_date").orderBy(col("salary").desc())
df = df.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).drop("rn")

w2 = Window.partitionBy("emp_id").orderBy("start_date")

df = df.withColumn("next_start", lead("start_date").over(w2)) \
       .withColumn("end_date", col("next_start")) \
       .drop("next_start") \
       .withColumn("status", when(col("end_date").isNull(), "ACTIVE").otherwise("INACTIVE"))

try:
    prev = spark.read.parquet(output_path)
except:
    prev = spark.createDataFrame([], df.schema)

combined = prev.unionByName(df)

final = combined.withColumn("next_start", lead("start_date").over(w2)) \
                .withColumn("end_date", col("next_start")) \
                .drop("next_start") \
                .withColumn("status", when(col("end_date").isNull(), "ACTIVE").otherwise("INACTIVE"))

final.write.mode("overwrite").partitionBy("status").parquet(output_path)

# Move files
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test"
)

today = datetime.datetime.utcnow().strftime("%Y-%m-%d")

objects = s3.list_objects_v2(Bucket=bucket, Prefix=bronze_prefix).get("Contents", [])

for obj in objects:
    key = obj["Key"]
    if key.endswith("/"):
        continue

    filename = os.path.basename(key)
    new_key = f"silver/employee_timeframe_processed/date={today}/{filename}"

    s3.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": key}, Key=new_key)
    s3.delete_object(Bucket=bucket, Key=key)

print("Step2 processing complete!")

spark.stop()