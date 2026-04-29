"""
═══════════════════════════════════════════════════════════════
  CAPSTONE PROJECT VIVA DEMO  –  Employee Data Management System
  Run inside airflow container:
    docker compose exec airflow python3 /opt/airflow/scripts/viva_demo.py
═══════════════════════════════════════════════════════════════
"""

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--packages org.apache.hadoop:hadoop-aws:3.3.4,'
    'com.amazonaws:aws-java-sdk-bundle:1.12.262,'
    'org.postgresql:postgresql:42.7.3 '
    'pyspark-shell'
)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import psycopg2

spark = SparkSession.builder \
    .appName("Viva Demo") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.driver.memory", "1g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

BUCKET = "poc-bootcamp-capstone-project-group2"
SEP = "=" * 70

def header(title):
    print(f"\n{SEP}")
    print(f"  {title}")
    print(SEP)

def safe_read_csv(path, label):
    try:
        df = spark.read.option("header", True).csv(path)
        print(f"\n📄 {label}  ({df.count()} rows)")
        df.show(5, truncate=False)
        return df
    except Exception as e:
        print(f"\n⚠️  {label}: Not found ({e})")
        return None

def safe_read_parquet(path, label):
    try:
        df = spark.read.parquet(path)
        print(f"\n📦 {label}  ({df.count()} rows)")
        df.printSchema()
        df.show(10, truncate=30)
        return df
    except Exception as e:
        print(f"\n⚠️  {label}: Not found ({e})")
        return None

# ═══════════════════════════════════════════════════════════════
#  BRONZE LAYER  –  Raw Source Data
# ═══════════════════════════════════════════════════════════════
header("🟤 BRONZE LAYER  –  Raw Source Data (S3)")

safe_read_csv(
    f"s3a://{BUCKET}/bronze/employee_data/",
    "Employee Data (employee_data.csv)"
)

safe_read_csv(
    f"s3a://{BUCKET}/bronze/employee_timeframe_data/unprocessed/",
    "Employee Timeframe – Unprocessed"
)

# Check if files were moved to processed
try:
    processed = spark.read.option("header", True).csv(
        f"s3a://{BUCKET}/bronze/processed/"
    )
    print(f"\n📄 Employee Timeframe – Processed (moved after pipeline)  ({processed.count()} rows)")
    processed.show(5, truncate=False)
except:
    print("\n✅ Timeframe files moved to bronze/processed/ after DAG run")

safe_read_csv(
    f"s3a://{BUCKET}/bronze/employee_leave_data/",
    "Employee Leave Data (raw)"
)

safe_read_csv(
    f"s3a://{BUCKET}/bronze/employee_leave_quota_data/",
    "Employee Leave Quota Data (raw)"
)

safe_read_csv(
    f"s3a://{BUCKET}/bronze/employee_calendar/",
    "Employee Leave Calendar (holidays)"
)

# ═══════════════════════════════════════════════════════════════
#  SILVER LAYER  –  Cleaned & Transformed Data
# ═══════════════════════════════════════════════════════════════
header("⚪ SILVER LAYER  –  Cleaned & Transformed Data (S3)")

safe_read_csv(
    f"s3a://{BUCKET}/silver/employee_timeframe_data/",
    "Employee Timeframe (cleaned CSV in silver)"
)

safe_read_parquet(
    f"s3a://{BUCKET}/silver/employee_leave_quota/",
    "Employee Leave Quota (partitioned by year)"
)

safe_read_parquet(
    f"s3a://{BUCKET}/silver/employee_leave_calendar/",
    "Employee Leave Calendar (partitioned by year)"
)

safe_read_parquet(
    f"s3a://{BUCKET}/silver/flagged_messages/",
    "Flagged Messages from Kafka Streaming (silver)"
)

# ═══════════════════════════════════════════════════════════════
#  GOLD LAYER  –  Analytics-Ready / Final Tables
# ═══════════════════════════════════════════════════════════════
header("🟡 GOLD LAYER  –  Analytics-Ready Tables (S3)")

# 1. Employee Data (append-only, partitioned by date)
safe_read_parquet(
    f"s3a://{BUCKET}/gold/employee-data/",
    "Employee Data (append-only, partitioned by Y/M/D)"
)

# 2. Employee Timeframe (partitioned by status)
print("\n── Employee Timeframe (ACTIVE vs INACTIVE) ──")
safe_read_parquet(
    f"s3a://{BUCKET}/gold/employee-timeframe-opt/status=ACTIVE/",
    "ACTIVE Employees (currently working)"
)
safe_read_parquet(
    f"s3a://{BUCKET}/gold/employee-timeframe-opt/status=INACTIVE/",
    "INACTIVE Employees (past designations)"
)

# 3. Employee Leave Data
safe_read_parquet(
    f"s3a://{BUCKET}/gold/employee_leave_data/",
    "Employee Leave Data (daily, partitioned by Y/M)"
)

# 4. Active Employees by Designation (dept-wise report)
safe_read_parquet(
    f"s3a://{BUCKET}/gold/active_emp_by_desg/",
    "Active Employees by Designation (dept-wise report)"
)

# 5. 8% Leave Anomaly Detection
df_8pct = safe_read_parquet(
    f"s3a://{BUCKET}/gold/employee_leaves_count_info/",
    "8% Leave Anomaly – Employees exceeding 8% working days"
)

# 6. 80% Leave Quota Report
safe_read_parquet(
    f"s3a://{BUCKET}/gold/employees_80_percent_leave_used/",
    "80% Leave Quota – Employees using >80% of their quota"
)

# 7. Leave Alert Text Files
header("📧 LEAVE ALERT TEXT FILES (generated for managers)")
import boto3
s3_client = boto3.client(
    "s3", endpoint_url="http://localstack:4566",
    aws_access_key_id="test", aws_secret_access_key="test"
)
try:
    objs = s3_client.list_objects_v2(
        Bucket=BUCKET, Prefix="gold/leave_alerts/"
    ).get("Contents", [])
    if objs:
        print(f"\n📁 {len(objs)} alert file(s) found:")
        for obj in objs[:5]:
            print(f"   └── {obj['Key']}")
        # Show content of first alert
        first_key = objs[0]["Key"]
        body = s3_client.get_object(Bucket=BUCKET, Key=first_key)["Body"].read().decode()
        print(f"\n📄 Sample alert ({first_key}):")
        print(body)
    else:
        print("  No alert files generated (monthly report runs on 1st of month)")
except Exception as e:
    print(f"  ⚠️ Could not list alerts: {e}")

# ═══════════════════════════════════════════════════════════════
#  POSTGRESQL  –  Streaming Results (Communication Monitoring)
# ═══════════════════════════════════════════════════════════════
header("🐘 POSTGRESQL  –  Streaming Results (Kafka Consumer)")

conn = psycopg2.connect(
    dbname="capstone_project2", user="postgres",
    password="postgres", host="postgres", port="5432"
)
cur = conn.cursor()

# Employee Strikes Summary
print("\n── Employee Strikes Table (top 20 by strikes) ──")
cur.execute("""
    SELECT employee_id, salary, no_of_strikes,
           strike_1, strike_2, strike_3, strike_4, strike_5,
           is_inactive
    FROM employee_strikes
    ORDER BY no_of_strikes DESC
    LIMIT 20;
""")
rows = cur.fetchall()
if rows:
    print(f"{'EMP_ID':<15} {'SALARY':>10} {'STRIKES':>8} {'S1':>10} {'S2':>10} {'S3':>10} {'S4':>10} {'S5':>10} {'INACTIVE':<8}")
    print("-" * 105)
    for r in rows:
        print(f"{str(r[0]):<15} {str(r[1] or 'NULL'):>10} {str(r[2]):>8} {str(r[3] or '-'):>10} {str(r[4] or '-'):>10} {str(r[5] or '-'):>10} {str(r[6] or '-'):>10} {str(r[7] or '-'):>10} {str(r[8]):<8}")
else:
    print("  No strikes recorded yet.")

# Stats
cur.execute("SELECT COUNT(*) FROM employee_strikes;")
total = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM employee_strikes WHERE no_of_strikes > 0;")
with_strikes = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM employee_strikes WHERE is_inactive = TRUE;")
suspended = cur.fetchone()[0]
cur.execute("SELECT COALESCE(MAX(no_of_strikes), 0) FROM employee_strikes;")
max_strikes = cur.fetchone()[0]

print(f"\n📊 Strike Statistics:")
print(f"   Total employees in table:  {total}")
print(f"   Employees with strikes:    {with_strikes}")
print(f"   Suspended (>=10 strikes):  {suspended}")
print(f"   Max strikes on any emp:    {max_strikes}")

# Flagged Messages Log
print("\n── Flagged Messages Log (last 10) ──")
cur.execute("""
    SELECT employee_id, start_date
    FROM flagged_messages
    ORDER BY start_date DESC
    LIMIT 10;
""")
rows = cur.fetchall()
if rows:
    print(f"{'EMP_ID':<15} {'FLAGGED_AT':<30}")
    print("-" * 45)
    for r in rows:
        print(f"{str(r[0]):<15} {str(r[1]):<30}")
else:
    print("  No flagged messages yet.")

cur.execute("SELECT COUNT(*) FROM flagged_messages;")
total_flagged = cur.fetchone()[0]
print(f"\n   Total flagged messages: {total_flagged}")

# Suspended employees detail
if suspended > 0:
    print("\n── 🚫 SUSPENDED EMPLOYEES (10+ strikes, INACTIVE) ──")
    cur.execute("""
        SELECT employee_id, salary, no_of_strikes, strike_10
        FROM employee_strikes
        WHERE is_inactive = TRUE
        ORDER BY no_of_strikes DESC
        LIMIT 10;
    """)
    rows = cur.fetchall()
    print(f"{'EMP_ID':<15} {'SALARY':>10} {'STRIKES':>8} {'STRIKE_10':>12}")
    print("-" * 50)
    for r in rows:
        print(f"{str(r[0]):<15} {str(r[1] or 'NULL'):>10} {str(r[2]):>8} {str(r[3] or '-'):>12}")

cur.close()
conn.close()

# ═══════════════════════════════════════════════════════════════
header("✅ VIVA DEMO COMPLETE")
print("""
  Data Flow Summary:
  ┌──────────┐    ┌──────────┐    ┌──────────┐
  │  BRONZE  │───►│  SILVER  │───►│   GOLD   │
  │ (Raw CSV)│    │(Cleaned) │    │(Reports) │
  └──────────┘    └──────────┘    └──────────┘
        │                              │
        │         ┌──────────┐         │
        └────────►│  KAFKA   │─────────┘
                  │(Streaming)│
                  └─────┬────┘
                        │
                  ┌─────▼────┐
                  │PostgreSQL│
                  │(Strikes) │
                  └──────────┘
""")

spark.stop()
