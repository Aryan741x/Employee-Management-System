from pyspark.sql import SparkSession
import json, psycopg2, re
from psycopg2.extras import execute_values
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType
)
from pyspark.sql.functions import (
    col, lit, round, coalesce, udf, from_json, current_timestamp
)
from datetime import datetime, timedelta

# ─────────────────────────────────────────────────────────────
# Spark Session  –  all S3 traffic goes to LocalStack
# ─────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("kafka-consumer-final") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.spark:spark-avro_2.12:3.5.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "org.postgresql:postgresql:42.7.3") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.AbstractFileSystem.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3A") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# ─────────────────────────────────────────────────────────────
# Kafka Stream
# ─────────────────────────────────────────────────────────────
kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "employee-messages") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 10) \
    .load()

# Message schema
message_schema = StructType([
    StructField("sender",   StringType(), True),
    StructField("receiver", StringType(), True),
    StructField("message",  StringType(), True),
])

# ─────────────────────────────────────────────────────────────
# FIX: absolute paths so this works inside the Docker container
# ─────────────────────────────────────────────────────────────
with open("/opt/airflow/data/vocab.json") as f:
    vocab = set(json.load(f))

with open("/opt/airflow/data/marked_words.json") as f:
    marked_words = set(json.load(f))

# ─────────────────────────────────────────────────────────────
# Batch Processing  (called once per micro-batch by Spark)
# ─────────────────────────────────────────────────────────────
def process_batch(batch_df, batch_id):

    if batch_df.isEmpty():
        print(f"[Batch {batch_id}] Empty – skipping.")
        return

    # ── Parse JSON messages ───────────────────────────────────
    json_df = batch_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), message_schema).alias("data")) \
        .select("data.*")

    # ── Flag messages containing reserved words ───────────────
    def is_flagged(message):
        if not message:
            return False
        words = re.findall(r'\b\w+\b', message.upper())
        return any(w in marked_words for w in words) and all(w in vocab for w in words)

    is_flagged_udf = udf(is_flagged, BooleanType())

    flagged_df = json_df \
        .withColumn("is_flagged", is_flagged_udf(col("message"))) \
        .filter(col("is_flagged")) \
        .withColumn("start_date", current_timestamp()) \
        .select(col("sender").alias("employee_id"), "start_date")

    # Keep only messages within the last 30 days
    one_month_ago = datetime.utcnow() - timedelta(days=30)
    flagged_df = flagged_df.filter(
        col("start_date") >= lit(one_month_ago.strftime('%Y-%m-%d %H:%M:%S'))
    )

    if flagged_df.isEmpty():
        print(f"[Batch {batch_id}] No flagged messages – skipping.")
        return

    # ── Write to LocalStack S3 (silver layer) ─────────────────
    flagged_df.write.mode("append").parquet(
        "s3a://poc-bootcamp-capstone-project-group2/silver/flagged_messages/"
    )

    # ── Persist to PostgreSQL ─────────────────────────────────
    conn = psycopg2.connect(
        dbname="capstone_project2",
        user="postgres",
        password="postgres",
        host="postgres",
        port="5432"
    )
    cur = conn.cursor()

    flagged_rows = [(r["employee_id"], r["start_date"]) for r in flagged_df.collect()]

    # Insert flagged messages log
    execute_values(
        cur,
        "INSERT INTO flagged_messages (employee_id, start_date) VALUES %s",
        flagged_rows,
    )

    # Aggregate new strikes per employee in this micro-batch
    strike_counts = {}
    for employee_id, _ in flagged_rows:
        strike_counts[employee_id] = strike_counts.get(employee_id, 0) + 1
    strike_rows = [(emp_id, inc) for emp_id, inc in strike_counts.items()]

    # Upsert strike counts
    execute_values(
        cur,
        """
        INSERT INTO employee_strikes (employee_id, salary, no_of_strikes)
        VALUES %s
        ON CONFLICT (employee_id) DO UPDATE
        SET no_of_strikes = employee_strikes.no_of_strikes + EXCLUDED.no_of_strikes
        """,
        strike_rows,
        template="(%s, NULL, %s)",
    )

    # Recalculate salary deductions for affected employees
    affected_ids = [row[0] for row in strike_rows]
    cur.execute(
        """
        UPDATE employee_strikes
        SET
            strike_1  = CASE WHEN no_of_strikes >= 1  AND salary IS NOT NULL THEN ROUND((salary * POWER(0.9, 1))::numeric,  2)::real ELSE NULL END,
            strike_2  = CASE WHEN no_of_strikes >= 2  AND salary IS NOT NULL THEN ROUND((salary * POWER(0.9, 2))::numeric,  2)::real ELSE NULL END,
            strike_3  = CASE WHEN no_of_strikes >= 3  AND salary IS NOT NULL THEN ROUND((salary * POWER(0.9, 3))::numeric,  2)::real ELSE NULL END,
            strike_4  = CASE WHEN no_of_strikes >= 4  AND salary IS NOT NULL THEN ROUND((salary * POWER(0.9, 4))::numeric,  2)::real ELSE NULL END,
            strike_5  = CASE WHEN no_of_strikes >= 5  AND salary IS NOT NULL THEN ROUND((salary * POWER(0.9, 5))::numeric,  2)::real ELSE NULL END,
            strike_6  = CASE WHEN no_of_strikes >= 6  AND salary IS NOT NULL THEN ROUND((salary * POWER(0.9, 6))::numeric,  2)::real ELSE NULL END,
            strike_7  = CASE WHEN no_of_strikes >= 7  AND salary IS NOT NULL THEN ROUND((salary * POWER(0.9, 7))::numeric,  2)::real ELSE NULL END,
            strike_8  = CASE WHEN no_of_strikes >= 8  AND salary IS NOT NULL THEN ROUND((salary * POWER(0.9, 8))::numeric,  2)::real ELSE NULL END,
            strike_9  = CASE WHEN no_of_strikes >= 9  AND salary IS NOT NULL THEN ROUND((salary * POWER(0.9, 9))::numeric,  2)::real ELSE NULL END,
            strike_10 = CASE WHEN no_of_strikes >= 10 AND salary IS NOT NULL THEN ROUND((salary * POWER(0.9, 10))::numeric, 2)::real ELSE NULL END,
            -- Flag employees with 10+ strikes as INACTIVE (BRD requirement)
            is_inactive = CASE WHEN no_of_strikes >= 10 THEN TRUE ELSE is_inactive END
        WHERE employee_id = ANY(%s)
        """,
        (affected_ids,),
    )

    conn.commit()
    print(f"[Batch {batch_id}] Processed {len(flagged_rows)} flagged message(s).")

    cur.close()
    conn.close()

# ─────────────────────────────────────────────────────────────
# Start Stream
# ─────────────────────────────────────────────────────────────
query = kafka_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("update") \
    .option("checkpointLocation",
            "s3a://poc-bootcamp-capstone-project-group2/gold/checkpoints/kafka-consumer/") \
    .trigger(processingTime="30 seconds") \
    .start()

print("Kafka consumer started. Listening on topic: employee-messages")
query.awaitTermination()