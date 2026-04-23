from pyspark.sql import SparkSession
import json, psycopg2, re
from pyspark.sql.types import *
from pyspark.sql.functions import col, lit, count, when, round, coalesce, udf, md5, concat_ws, from_json, current_timestamp
from datetime import datetime, timedelta

# Spark + Kafka + LocalStack
spark = SparkSession.builder \
    .appName("kafka-consumer-full") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# -----------------------------
# PostgreSQL UPSERT QUERY
# -----------------------------
postgres_query = """ 
-- same query (unchanged)
"""  # keep your original query here

# -----------------------------
# Kafka Stream
# -----------------------------
kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "employee-messages") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 10) \
    .load()

# -----------------------------
# Schema
# -----------------------------
message_schema = StructType() \
    .add("sender", StringType()) \
    .add("receiver", StringType()) \
    .add("message", StringType())

# -----------------------------
# FIXED: Load vocab locally
# -----------------------------
with open("data/vocab.json") as f:
    vocab = set(json.load(f))

with open("data/marked_words.json") as f:
    marked_words = set(json.load(f))

# -----------------------------
# Strike logic (UNCHANGED)
# -----------------------------
def func_strike_calculator(employee_strikes_df, employee_flags_count_df):
    employee_combined_df = employee_strikes_df.join(
        employee_flags_count_df,
        employee_strikes_df["employee_id"] == employee_flags_count_df["employee_id"],
        how="inner"
    ).select(
        employee_strikes_df["employee_id"], "salary",
        "strike_1","strike_2","strike_3","strike_4","strike_5",
        "strike_6","strike_7","strike_8","strike_9","strike_10",
        "no_of_strikes","strike_count"
    )

    updated_df = employee_combined_df

    for i in range(1, 11):
        salary_after_i_strikes = round(col("salary") * (0.9 ** i), 2)
        updated_df = updated_df.withColumn(
            f"strike_{i}",
            when((col("strike_count") >= i) & (col("no_of_strikes") < i), salary_after_i_strikes)
            .when((col("strike_count") < i), lit(None))
            .otherwise(col(f"strike_{i}"))
        )

    updated_df = updated_df.withColumn(
        "no_of_strikes", coalesce(col("strike_count"), lit(0))
    ).drop("strike_count")

    return updated_df

# -----------------------------
# Batch Processing
# -----------------------------
def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    conn = psycopg2.connect(
        dbname="capstone_project2",
        user="postgres",
        password="YOUR_PASSWORD",
        host="postgres",  # use service name for Docker
        port="5432"
    )
    cur = conn.cursor()

    json_df = batch_df.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), message_schema).alias("data")) \
        .select("data.*")

    def is_flagged(message):
        words = re.findall(r'\b\w+\b', message.upper())
        return any(word in marked_words for word in words) and all(word in vocab for word in words)

    is_flagged_udf = udf(is_flagged, BooleanType())

    flagged_df = json_df \
        .withColumn("is_flagged", is_flagged_udf(col("message"))) \
        .filter(col("is_flagged")) \
        .withColumn("start_date", current_timestamp()) \
        .select(col("sender").alias("employee_id"), "start_date")

    # -----------------------------
    # FIXED: LocalStack S3
    # -----------------------------
    flagged_df.write \
        .mode("append") \
        .parquet("s3a://employee-bucket/silver/flagged_messages/")

    print("Batch processed")

    cur.close()
    conn.close()

# -----------------------------
# STREAM START
# -----------------------------
query = kafka_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("update") \
    .option("checkpointLocation", "s3a://employee-bucket/checkpoints/") \
    .trigger(processingTime="30 seconds") \
    .start()

query.awaitTermination()