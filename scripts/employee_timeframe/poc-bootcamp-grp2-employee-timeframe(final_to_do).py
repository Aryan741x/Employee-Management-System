from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, to_date, from_unixtime, when, row_number, lit, lead, current_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Spark
spark = SparkSession.builder \
    .appName("Employee Timeframe Final") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

bucket = "employee-bucket"

input_path = f"s3a://{bucket}/bronze/employee-timeframe-opt/"
output_path = f"s3a://{bucket}/gold/employee-timeframe-opt/"

schema = StructType([
    StructField("emp_id", StringType(), True),
    StructField("designation", StringType(), True),
    StructField("start_date", DoubleType(), True),
    StructField("end_date", DoubleType(), True),
    StructField("salary", DoubleType(), True),
])

df = spark.read.option("header", True).schema(schema).csv(input_path)

if df.count() == 0:
    print("No data found")
else:
    df = df.withColumn("start_date", to_date(from_unixtime(col("start_date")))) \
           .withColumn("end_date", to_date(from_unixtime(col("end_date"))))

    w1 = Window.partitionBy("emp_id", "start_date", "end_date").orderBy(col("salary").desc())
    df = df.withColumn("rn", row_number().over(w1)).filter(col("rn") == 1).drop("rn")

    w2 = Window.partitionBy("emp_id").orderBy("start_date")

    df = df.withColumn("next_start", lead("start_date").over(w2)) \
           .withColumn("end_date", col("next_start")) \
           .drop("next_start")

    df = df.withColumn("status", when(col("end_date").isNull(), "ACTIVE").otherwise("INACTIVE"))

    try:
        existing = spark.read.parquet(output_path)
    except:
        existing = spark.createDataFrame([], df.schema)

    combined = existing.unionByName(df)

    final = combined.withColumn("next_start", lead("start_date").over(w2)) \
                    .withColumn("end_date", col("next_start")) \
                    .drop("next_start") \
                    .withColumn("status", when(col("end_date").isNull(), "ACTIVE").otherwise("INACTIVE"))

    final.write.mode("overwrite").partitionBy("status").parquet(output_path)

    print("Final timeframe processed!")

spark.stop()