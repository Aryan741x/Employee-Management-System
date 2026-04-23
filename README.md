# 🎓 Employee Management System — Viva Presentation Guide

> This is your step‑by‑step script for the viva. Follow it **top to bottom**. Each section tells you **what to say**, **what to show**, and **what command to run** — in order.

---

## ⏱️ Before the Viva — Pre‑Setup (Do This 20 min Before)

Make sure Docker Desktop is running. Open **one PowerShell terminal** in the project root and run:

```bash
docker-compose up -d --build
```

This boots **8 containers**: `zookeeper`, `kafka`, `kafka-init`, `postgres`, `localstack`, `s3-init`, `airflow`, `kafka-consumer`.

Wait ~2 minutes for everything to become healthy. Verify with:

```bash
docker-compose ps
```

You should see `airflow`, `postgres`, `kafka`, `zookeeper`, `localstack`, `kafka-consumer` as **running**, and `kafka-init`, `s3-init` as **exited (0)** (they are one‑shot init containers, that's expected).

> **Also open the Airflow UI** at [http://localhost:8081](http://localhost:8081) — login with **airflow / airflow**. Make sure the DAG `employee_local_pipeline` is visible and **unpaused**.

Now you're ready. Keep the terminal and browser open.

---

---

# 🎤 VIVA FLOW — Step by Step

---

## STEP 1 — Introduction (2 min)

### What to Say:
> "Good morning/afternoon. Our project is an **Employee Management System** that uses a **hybrid batch‑and‑streaming data pipeline**.
>
> The **problem** we're solving has two parts:
> 1. **Batch side** — Every day, we need to process employee data, timeframe records, and leave records. We clean them, store them in a data lake, and generate reports — like flagging employees who have taken more than **8% of total working days** as leave (anomaly detection), and flagging employees who've used more than **80% of their annual leave quota**.
> 2. **Streaming side** — Employees send chat messages. We need to detect **policy‑violating messages** in real‑time. When someone sends a flagged word, the system records a **strike** against them and automatically **deducts 10% of their salary** per strike. After **10 strikes**, they are marked **inactive**."

### What to Show:
- Show the project folder structure in VS Code to give a quick visual.

---

## STEP 2 — Architecture & Tech Stack (3 min)

### What to Say:
> "Let me walk you through the architecture. Everything runs inside **Docker containers** via Docker Compose."

### What to Show:
- Open `docker-compose.yml` in VS Code and scroll through it while explaining each service:

### Explain Each Service:
| Service | What to Say |
|---|---|
| **Zookeeper** | "Zookeeper is needed for Kafka coordination. It keeps track of Kafka brokers." |
| **Kafka** | "Apache Kafka is our message broker. It has a topic called `employee-messages` where chat messages are published." |
| **kafka-init** | "This is a one-shot container that creates the `employee-messages` topic when the system starts." |
| **PostgreSQL** | "Postgres is our relational database. It stores the `employee_strikes`, `employee_strikes_stg`, and `flagged_messages` tables. The init.sql script creates these automatically." |
| **LocalStack** | "LocalStack simulates AWS S3 locally. We use it as our data lake — it has Bronze, Silver, and Gold layers." |
| **s3-init** | "Another one-shot container. It creates the S3 bucket `poc-bootcamp-capstone-project-group2` and uploads all our raw CSV/JSON files into the **Bronze layer**." |
| **Airflow** | "Apache Airflow orchestrates the entire batch pipeline. It runs our PySpark jobs in sequence as a DAG." |
| **kafka-consumer** | "This is a **Spark Structured Streaming** application. It listens to the Kafka topic 24/7, checks messages for flagged words, and writes strikes to Postgres." |

### Then Say:
> "The data flows through a **Medallion Architecture**:
> - **Bronze** — raw CSVs uploaded to S3 (employee data, timeframe, leaves, calendar, quota, marked words).
> - **Silver** — cleaned and deduplicated data (Parquet format in S3).
> - **Gold** — aggregated reports and final outputs (Parquet in S3)."

---

## STEP 3 — Show the Airflow DAG (3 min)

### What to Say:
> "Now let me show you how the batch pipeline is orchestrated."

### What to Show:
- Open the **Airflow UI** at [http://localhost:8081](http://localhost:8081).
- Click on the DAG **`employee_local_pipeline`**.
- Click on the **Graph** tab to show the task flow.

### Then Say (while pointing at each node):
> "The DAG runs daily at 7 AM UTC. Here's the flow:
>
> 1. **yearly_check** — A BranchPythonOperator. On January 1st, it runs the yearly tasks: `leave_quota` (processes leave quotas from Bronze → Silver) and `leave_calendar` (processes holiday calendar from Bronze → Silver). On other days, it skips them.
>
> 2. **employee_data** — Reads `employee_data.csv` from Bronze, cleans it (drops nulls, removes invalid ages, deduplicates), and writes to **Gold** as Parquet partitioned by year/month/day.
>
> 3. **employee_timeframe** — Reads timeframe CSV from Bronze, deduplicates, computes ACTIVE vs INACTIVE status based on end dates, writes to **Silver** (CSV) and **Gold** (Parquet partitioned by status). After processing, it moves the file from `unprocessed/` to `processed/` in S3.
>
> 4. **timeframe_to_strikes** — This is the bridge between batch and streaming. It reads ACTIVE employees from the Gold timeframe data, checks their salary, and syncs it into the `employee_strikes` table in Postgres. This way the streaming consumer knows each employee's salary for penalty calculation.
>
> 5. **employee_leaves** — Reads leave data from Bronze (~125 MB), filters out weekends and holidays, resolves ACTIVE vs CANCELLED statuses, deduplicates, and writes to **Gold** partitioned by year/month.
>
> 6. **dept_report** — Counts active employees per designation and writes a department‑wise report to Gold.
>
> 7. **leave_report** — This is the **8% anomaly detection**. It counts each active employee's leave days, calculates the percentage against total working days, and flags anyone above 8%.
>
> 8. **monthly_check** → **run_monthly** — On the 1st of each month, runs the **80% leave usage** report. It compares leaves taken vs annual quota. If an employee has used more than 80%, it generates a text alert file in S3."

### What to Show Next:
- Open `dags/my_pg2_version4.py` in VS Code and show the DAG flow comment block (lines 186–215) — it visually shows the linear pipeline.

---

## STEP 4 — Trigger the Batch Pipeline & Show Results (5 min)

### What to Say:
> "Let me trigger the DAG manually and show you the results."

### Command to Run:
```bash
docker exec airflow airflow dags trigger employee_local_pipeline
```

### Then Say:
> "The DAG is now running. Let me show you the progress in Airflow."

### What to Show:
- In the Airflow UI, click on the DAG → **Grid** or **Graph** view. Watch tasks turn green as they complete.
- Wait for tasks to finish (takes ~3-5 minutes).

---

### 4a — Show the S3 Data Lake (Bronze → Silver → Gold)

### What to Say:
> "Let me show you the data lake structure in S3."

### Command to Run:
```bash
docker exec localstack aws --endpoint-url=http://localhost:4566 s3 ls s3://poc-bootcamp-capstone-project-group2/ --recursive | head -50
```

### Then Say:
> "You can see the Bronze layer has raw CSVs, the Silver layer has cleaned Parquet files (timeframe, leave calendar, leave quota, and flagged messages from streaming), and the Gold layer has the final aggregated reports."

---

### 4b — Show the 8% Leave Anomaly Report (Gold Table)

### What to Say:
> "Now let me show the **8% leave anomaly report**. This flags employees whose leave count exceeds 8% of total working days."

### Command to Run:
```bash
docker exec airflow python3 /opt/airflow/scripts/print_8_percent.py
```

### What This Shows:
A table with columns: `emp_id`, `upcoming_leaves_count`. These are employees flagged for exceeding 8% leave.

### Then Say:
> "These employees have taken or planned more than 8% of working days as leave. This data sits in the Gold layer at `gold/employee_leaves_count_info/`. The report is generated by the `leave_report` task in the DAG."

---

### 4c — Show the 80% Leave Usage Report (Gold Table)

### What to Say:
> "The 80% report runs monthly. It compares each employee's actual leaves taken against their annual leave quota."

### Command to Run:
```bash
docker exec airflow python3 -c "
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 pyspark-shell'
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('read-80-pct').config('spark.hadoop.fs.s3a.endpoint','http://localstack:4566').config('spark.hadoop.fs.s3a.access.key','test').config('spark.hadoop.fs.s3a.secret.key','test').config('spark.hadoop.fs.s3a.path.style.access','true').config('spark.hadoop.fs.s3a.impl','org.apache.hadoop.fs.s3a.S3AFileSystem').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
try:
    df = spark.read.parquet('s3a://poc-bootcamp-capstone-project-group2/gold/employees_80_percent_leave_used/')
    print('\\n--- 80% LEAVE USAGE REPORT ---')
    df.show(20, truncate=False)
except Exception as e:
    print(f'Not available yet (runs monthly): {e}')
spark.stop()
"
```

### Then Say:
> "These employees have consumed over 80% of their annual leave quota. For each one, a text alert file is also written to S3 under `gold/leave_alerts/`."

---

### 4d — Show the Department‑Wise Report (Gold Table)

### What to Say:
> "We also generate a department‑wise active employee count."

### Command to Run:
```bash
docker exec airflow python3 -c "
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 pyspark-shell'
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('read-dept').config('spark.hadoop.fs.s3a.endpoint','http://localstack:4566').config('spark.hadoop.fs.s3a.access.key','test').config('spark.hadoop.fs.s3a.secret.key','test').config('spark.hadoop.fs.s3a.path.style.access','true').config('spark.hadoop.fs.s3a.impl','org.apache.hadoop.fs.s3a.S3AFileSystem').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
df = spark.read.parquet('s3a://poc-bootcamp-capstone-project-group2/gold/active_emp_by_desg/')
print('\\n--- DEPARTMENT-WISE ACTIVE EMPLOYEE REPORT ---')
df.show(20, truncate=False)
spark.stop()
"
```

### Then Say:
> "This shows how many employees are currently active in each designation. It's generated by the `dept_report` task."

---

### 4e — Show the Employee Timeframe Gold Data

### What to Say:
> "Let me also show the employee timeframe data — this shows which employees are ACTIVE and which are INACTIVE."

### Command to Run:
```bash
docker exec airflow python3 -c "
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 pyspark-shell'
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('read-timeframe').config('spark.hadoop.fs.s3a.endpoint','http://localstack:4566').config('spark.hadoop.fs.s3a.access.key','test').config('spark.hadoop.fs.s3a.secret.key','test').config('spark.hadoop.fs.s3a.path.style.access','true').config('spark.hadoop.fs.s3a.impl','org.apache.hadoop.fs.s3a.S3AFileSystem').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
df = spark.read.parquet('s3a://poc-bootcamp-capstone-project-group2/gold/employee-timeframe-opt/')
print('\\n--- EMPLOYEE TIMEFRAME (ACTIVE/INACTIVE) ---')
df.show(20, truncate=False)
spark.stop()
"
```

---

## STEP 5 — Show the Strikes Table in Postgres (bridge to streaming) (2 min)

### What to Say:
> "The `timeframe_to_strikes` task syncs active employees and their salaries from the Gold layer into the PostgreSQL `employee_strikes` table. This is the bridge between batch and streaming — the streaming consumer uses this table to know each employee's salary for penalty deductions."

### Command to Run:
```bash
docker exec postgres psql -U postgres -d capstone_project2 -c "SELECT employee_id, salary, no_of_strikes, strike_1, strike_2, is_inactive FROM employee_strikes LIMIT 10;"
```

### Then Say:
> "Right now, `no_of_strikes` is 0 for everyone because no chat violations have occurred yet. The salary column is populated from the timeframe data. Let me now show you the streaming pipeline in action."

---

## STEP 6 — Real‑Time Streaming Demo (5 min)

### What to Say:
> "Now for the streaming part. The `kafka-consumer` container is already running — it uses **Spark Structured Streaming** to listen to the `employee-messages` Kafka topic every 30 seconds."

### 6a — Show the Consumer is Running

### Command to Run:
```bash
docker logs kafka-consumer --tail 10
```

### Then Say:
> "You can see it says 'Kafka consumer started. Listening on topic: employee-messages'. It processes micro-batches every 30 seconds."

---

### 6b — Send a Test Violation Message

### What to Say:
> "Now I'll simulate an employee sending a chat message that contains a **policy‑violating word**. The producer sends this to Kafka, the consumer picks it up, checks the words against `marked_words.json` and `vocab.json`, and if it's flagged, it records a strike."

### Command to Run:
```bash
docker exec airflow python3 /opt/airflow/scripts/test_kafka_message.py
```

### Then Say:
> "The message has been sent for employee **EMP042**. It says 'This is a violation using IAF9TTGGC2 immediately.' The word **IAF9TTGGC2** is in our `marked_words.json` dictionary, so it will be flagged."

### Wait 30–40 seconds for the consumer to process it. Then show the consumer logs:

```bash
docker logs kafka-consumer --tail 15
```

### You Should See:
> `[Batch X] Processed 1 flagged message(s).`

---

### 6c — Verify the Strike in Postgres

### What to Say:
> "Let me verify that the strike was recorded in the database."

### Command to Run:
```bash
docker exec postgres psql -U postgres -d capstone_project2 -c "SELECT employee_id, salary, no_of_strikes, strike_1, is_inactive FROM employee_strikes WHERE employee_id = 'EMP042';"
```

### Then Say:
> "You can see `no_of_strikes` is now **1** for EMP042. The `strike_1` column shows the reduced salary — it's **90% of the original salary** (10% deduction). If this employee gets 10 strikes, the `is_inactive` flag will be set to TRUE and they'll be deactivated."

---

### 6d — Show the Flagged Messages Log

### Command to Run:
```bash
docker exec postgres psql -U postgres -d capstone_project2 -c "SELECT * FROM flagged_messages ORDER BY start_date DESC LIMIT 5;"
```

### Then Say:
> "Every flagged message is also logged in the `flagged_messages` table with the employee ID and timestamp. And on the S3 side, the flagged message data is also written to the **Silver layer** at `silver/flagged_messages/` as Parquet."

---

### 6e — (Optional) Run the Full Producer

### What to Say:
> "I can also run the full producer which sends all messages from `messages.json` — about 50,000+ messages. The consumer will process them in batches."

### Command to Run:
```bash
docker-compose run --rm kafka-producer
```

> **Note**: This will take a while. You can let it run for 30 seconds then Ctrl+C, and show the strikes table again to see multiple employees getting strikes.

### Then Show Updated Strikes:
```bash
docker exec postgres psql -U postgres -d capstone_project2 -c "SELECT employee_id, salary, no_of_strikes, strike_1, strike_2, strike_3, is_inactive FROM employee_strikes WHERE no_of_strikes > 0 ORDER BY no_of_strikes DESC LIMIT 10;"
```

---

## STEP 7 — Conclusion (2 min)

### What to Say:
> "To summarize:
>
> 1. We built a **fully containerised hybrid data pipeline** using Docker Compose — one command to bring up the entire system.
> 2. The **batch pipeline** is orchestrated by Airflow. It processes employee data through a Medallion Architecture (Bronze → Silver → Gold) using PySpark, and generates two key reports:
>    - **8% anomaly** — flags employees with excessive leave.
>    - **80% usage** — alerts when employees exhaust their annual quota.
> 3. The **streaming pipeline** uses Kafka + Spark Structured Streaming for real‑time policy enforcement. Flagged messages automatically trigger salary deductions and strikes in PostgreSQL.
> 4. The system is modular, scalable, and uses industry‑standard tools: **Airflow, Spark, Kafka, PostgreSQL, S3 (LocalStack), Docker**.
>
> Possible extensions could include a dashboard for HR, email alerts for 80% usage, or scaling Kafka to multiple partitions for higher throughput."

---

## 📋 Quick‑Reference Command Cheat Sheet

| # | Purpose | Command |
|---|---------|---------|
| 1 | Start everything | `docker-compose up -d --build` |
| 2 | Check container status | `docker-compose ps` |
| 3 | Open Airflow UI | [http://localhost:8081](http://localhost:8081) (airflow / airflow) |
| 4 | Trigger the batch DAG | `docker exec airflow airflow dags trigger employee_local_pipeline` |
| 5 | View 8% anomaly report | `docker exec airflow python3 /opt/airflow/scripts/print_8_percent.py` |
| 6 | View employee strikes table | `docker exec postgres psql -U postgres -d capstone_project2 -c "SELECT * FROM employee_strikes LIMIT 10;"` |
| 7 | Send a test violation message | `docker exec airflow python3 /opt/airflow/scripts/test_kafka_message.py` |
| 8 | Check consumer logs | `docker logs kafka-consumer --tail 15` |
| 9 | Verify strikes after violation | `docker exec postgres psql -U postgres -d capstone_project2 -c "SELECT * FROM employee_strikes WHERE employee_id='EMP042';"` |
| 10 | View flagged messages log | `docker exec postgres psql -U postgres -d capstone_project2 -c "SELECT * FROM flagged_messages ORDER BY start_date DESC LIMIT 5;"` |
| 11 | Run full producer | `docker-compose run --rm kafka-producer` |
| 12 | View S3 data lake contents | `docker exec localstack aws --endpoint-url=http://localhost:4566 s3 ls s3://poc-bootcamp-capstone-project-group2/ --recursive \| head -50` |
| 13 | Stop everything | `docker-compose down` |

---

## ❓ Common Viva Questions & Answers

**Q: Why did you use LocalStack instead of real AWS?**
> "LocalStack simulates AWS S3 locally so we don't need cloud credentials or incur costs during development and demos. In production, we'd simply change the endpoint URL to the real AWS S3."

**Q: Why Medallion Architecture (Bronze / Silver / Gold)?**
> "Bronze stores raw, unprocessed data. Silver stores cleaned and validated data. Gold stores business-level aggregated reports. This separation makes data lineage clear and allows reprocessing at any layer."

**Q: How does the strike system work?**
> "The consumer checks each chat message word against `marked_words.json` (flagged words) and `vocab.json` (valid vocabulary). If a message contains a flagged word and all words are in the vocabulary, it's considered a violation. Each violation increments the employee's strike count and reduces their salary by 10% compounding — `salary × 0.9^n` where n is the strike number. At 10 strikes, the employee is marked inactive."

**Q: Why Spark Structured Streaming instead of plain Kafka consumer?**
> "Spark Structured Streaming gives us exactly-once processing semantics, checkpoint-based fault tolerance (checkpoints stored in S3), and micro-batch processing. If the consumer crashes and restarts, it picks up from the last checkpoint."

**Q: What happens if the batch DAG fails midway?**
> "Airflow has built-in retries (configured as 1 retry with 5-minute delay). Each task is idempotent — they can be re-run safely. The yearly and monthly tasks use BranchPythonOperators so they only execute on the correct dates."

**Q: Why Docker Compose?**
> "It lets us define all 8 services and their dependencies in one YAML file. Health checks ensure services start in proper order. Anyone can clone the repo and run `docker-compose up` to get the full system running."

---

## ✅ Pre-Viva Checklist

- [ ] Docker Desktop is running
- [ ] `docker-compose up -d --build` runs without errors
- [ ] `docker-compose ps` shows all services healthy
- [ ] Airflow UI loads at [http://localhost:8081](http://localhost:8081)
- [ ] DAG `employee_local_pipeline` is visible and unpaused
- [ ] Trigger the DAG once and verify all tasks go green
- [ ] `print_8_percent.py` shows the anomaly report
- [ ] `test_kafka_message.py` sends a message successfully
- [ ] `employee_strikes` table shows the strike for EMP042
- [ ] `flagged_messages` table shows the logged violation
- [ ] Practice this script at least once end-to-end
