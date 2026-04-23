import os
os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--packages org.apache.hadoop:hadoop-aws:3.3.4,'
    'com.amazonaws:aws-java-sdk-bundle:1.12.262,'
    'org.postgresql:postgresql:42.7.3 '
    'pyspark-shell'
)

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import psycopg2

spark = SparkSession.builder \
    .appName("Timeframe To Strikes") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.driver.memory", "1g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read ACTIVE employees from gold layer
employee_timeframe_df = spark.read.parquet(
    "s3a://poc-bootcamp-capstone-project-group2/gold/employee-timeframe-opt/status=ACTIVE/"
)

emp_ids = [row["emp_id"] for row in employee_timeframe_df.select("emp_id").collect()]

if not emp_ids:
    print("No active employees found. Skipping timeframe-to-strikes.")
    spark.stop()
    raise SystemExit(0)

emp_id_list   = ",".join(f"'{e}'" for e in emp_ids)
emp_id_filter = f"({emp_id_list})"

query = f"(SELECT * FROM employee_strikes WHERE employee_id IN {emp_id_filter}) AS es"

employee_strikes_df = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/capstone_project2") \
    .option("dbtable", query) \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .load()

new_records = employee_timeframe_df.join(
    employee_strikes_df,
    employee_timeframe_df["emp_id"] == employee_strikes_df["employee_id"],
    "left_anti"
).select("emp_id", "salary")

existing_records = employee_timeframe_df.join(
    employee_strikes_df,
    (employee_timeframe_df["emp_id"] == employee_strikes_df["employee_id"]) &
    (employee_timeframe_df["salary"] != employee_strikes_df["salary"]),
    "inner"
).select(
    employee_timeframe_df["emp_id"], employee_timeframe_df["salary"],
    "strike_1","strike_2","strike_3","strike_4","strike_5",
    "strike_6","strike_7","strike_8","strike_9","strike_10",
    "no_of_strikes"
)

def func_strike_calculator(df):
    for i in range(1, 11):
        salary_after_i = round(col("salary") * (0.9 ** i), 2)
        df = df.withColumn(
            f"strike_{i}",
            when(col("no_of_strikes") >= i, salary_after_i).otherwise(lit(None))
        )
    return df

existing_records = func_strike_calculator(existing_records)

new_records_extended = new_records \
    .withColumn("strike_1",  lit(None).cast("float")) \
    .withColumn("strike_2",  lit(None).cast("float")) \
    .withColumn("strike_3",  lit(None).cast("float")) \
    .withColumn("strike_4",  lit(None).cast("float")) \
    .withColumn("strike_5",  lit(None).cast("float")) \
    .withColumn("strike_6",  lit(None).cast("float")) \
    .withColumn("strike_7",  lit(None).cast("float")) \
    .withColumn("strike_8",  lit(None).cast("float")) \
    .withColumn("strike_9",  lit(None).cast("float")) \
    .withColumn("strike_10", lit(None).cast("float")) \
    .withColumn("no_of_strikes", lit(0).cast("int"))

updated_df = existing_records.unionByName(new_records_extended) \
    .withColumnRenamed("emp_id", "employee_id")

updated_df.write.format("jdbc") \
    .mode("overwrite") \
    .option("url", "jdbc:postgresql://postgres:5432/capstone_project2") \
    .option("dbtable", "timeframeToStrikes_table") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .save()

conn = psycopg2.connect(
    dbname="capstone_project2", user="postgres",
    password="postgres", host="postgres", port="5432"
)
cur = conn.cursor()

cur.execute("""
INSERT INTO employee_strikes (
    employee_id, salary,
    strike_1, strike_2, strike_3, strike_4, strike_5,
    strike_6, strike_7, strike_8, strike_9, strike_10,
    no_of_strikes
)
SELECT * FROM timeframeToStrikes_table
ON CONFLICT (employee_id) DO UPDATE SET
    salary        = EXCLUDED.salary,
    strike_1      = EXCLUDED.strike_1,  strike_2  = EXCLUDED.strike_2,
    strike_3      = EXCLUDED.strike_3,  strike_4  = EXCLUDED.strike_4,
    strike_5      = EXCLUDED.strike_5,  strike_6  = EXCLUDED.strike_6,
    strike_7      = EXCLUDED.strike_7,  strike_8  = EXCLUDED.strike_8,
    strike_9      = EXCLUDED.strike_9,  strike_10 = EXCLUDED.strike_10,
    no_of_strikes = COALESCE(EXCLUDED.no_of_strikes, employee_strikes.no_of_strikes, 0);
""")
cur.execute("UPDATE employee_strikes SET no_of_strikes = 0 WHERE no_of_strikes IS NULL;")
conn.commit()
cur.close()
conn.close()

print("Timeframe to Strikes completed successfully!")
spark.stop()