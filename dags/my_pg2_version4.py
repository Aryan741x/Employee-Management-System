from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import subprocess

# ─────────────────────────────────────────────────────────────
# Default args
# ─────────────────────────────────────────────────────────────
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'employee_local_pipeline',
    default_args=default_args,
    description='LocalStack Employee Data Pipeline (Batch)',
    schedule_interval='0 7 * * *',   # Daily at 07:00 UTC
    start_date=days_ago(1),
    catchup=False,
    tags=['employee', 'batch', 'localstack'],
)

# ─────────────────────────────────────────────────────────────
# Helper: run a Python script as a subprocess
# ─────────────────────────────────────────────────────────────
def run_script(script_path):
    """Returns a callable that runs the given script with python3."""
    def _run():
        result = subprocess.run(
            ["python3", script_path],
            check=True,
            capture_output=True,
            text=True,
        )
        print(result.stdout)
        if result.stderr:
            print("[STDERR]", result.stderr)
    return _run

# ─────────────────────────────────────────────────────────────
# Branch Logic
# ─────────────────────────────────────────────────────────────
def monthly_checker(**ctx):
    """Returns monthly task ID on the 1st of each month."""
    today = datetime.utcnow()
    return 'run_monthly' if today.day == 1 else 'skip_monthly'

def yearly_checker(**ctx):
    """Returns yearly task ID on Jan 1st only."""
    today = datetime.utcnow()
    return 'run_yearly_group' if (today.day == 1 and today.month == 1) else 'skip_yearly'

# ─────────────────────────────────────────────────────────────
# TASK DEFINITIONS
# ─────────────────────────────────────────────────────────────

# ── Yearly branch (runs at VERY START – Jan 1st only) ────────
yearly_check = BranchPythonOperator(
    task_id='yearly_check',
    python_callable=yearly_checker,
    dag=dag,
)

run_yearly_group = EmptyOperator(
    task_id='run_yearly_group',
    dag=dag,
)

t_leave_quota = PythonOperator(
    task_id='leave_quota',
    python_callable=run_script(
        '/opt/airflow/scripts/employee_leaves_data/poc-bootcamp-glue-job-gp2-employee_leave_quota.py'
    ),
    dag=dag,
)

t_leave_calendar = PythonOperator(
    task_id='leave_calendar',
    python_callable=run_script(
        '/opt/airflow/scripts/employee_leaves_data/poc-bootcamp-glue-job-gp2-employee_leave_calendar.py'
    ),
    dag=dag,
)

yearly_done = EmptyOperator(
    task_id='yearly_done',
    dag=dag,
)

skip_yearly = EmptyOperator(
    task_id='skip_yearly',
    dag=dag,
)

# Join after yearly branch (wait for EITHER path)
yearly_join = EmptyOperator(
    task_id='yearly_join',
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    dag=dag,
)

# ── Daily batch tasks ─────────────────────────────────────────
t1_employee_data = PythonOperator(
    task_id='employee_data',
    python_callable=run_script(
        '/opt/airflow/scripts/employee_data/poc-bootcamp-grp2-employee-data.py'
    ),
    dag=dag,
)

t2_employee_timeframe = PythonOperator(
    task_id='employee_timeframe',
    python_callable=run_script(
        '/opt/airflow/scripts/employee_timeframe/poc-bootcamp-glue-job-gp2-employee_timeframe_data.py'
    ),
    dag=dag,
)

# Syncs timeframe salaries → strikes table  (must run after timeframe)
t3_timeframe_to_strikes = PythonOperator(
    task_id='timeframe_to_strikes',
    python_callable=run_script(
        '/opt/airflow/scripts/kafka/poc-bootcamp-grp2-timeframeToStrikes.py'
    ),
    dag=dag,
)

t4_employee_leaves = PythonOperator(
    task_id='employee_leaves',
    python_callable=run_script(
        '/opt/airflow/scripts/employee_leaves_data/poc-bootcamp-glue-job-gp2-employee_leave_data.py'
    ),
    dag=dag,
)

t5_dept_report = PythonOperator(
    task_id='dept_report',
    python_callable=run_script(
        '/opt/airflow/scripts/employee_reporting/poc-bootcamp-grp2-deptWiseReport.py'
    ),
    dag=dag,
)

t6_leave_report = PythonOperator(
    task_id='leave_report',
    python_callable=run_script(
        '/opt/airflow/scripts/employee_reporting/poc-bootcamp-grp2-employee-leave-report.py'
    ),
    dag=dag,
)

# ── Monthly branch ────────────────────────────────────────────
monthly_check = BranchPythonOperator(
    task_id='monthly_check',
    python_callable=monthly_checker,
    dag=dag,
)

t_availed_leaves = PythonOperator(
    task_id='run_monthly',
    python_callable=run_script(
        '/opt/airflow/scripts/employee_reporting/poc-bootcamp-glue-job-gp2-employee_reporting_80%.py'
    ),
    dag=dag,
)

skip_monthly = EmptyOperator(
    task_id='skip_monthly',
    dag=dag,
)

pipeline_done = EmptyOperator(
    task_id='pipeline_done',
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    dag=dag,
)

# ─────────────────────────────────────────────────────────────
# DAG DEPENDENCY FLOW  (strictly linear — no cycles)
#
#  yearly_check ──► run_yearly_group ──► [t_leave_quota, t_leave_calendar]
#               \                                    │
#                ──► skip_yearly ───────────────────►│
#                                                    ▼
#                                              yearly_join
#                                                    │
#                                                    ▼
#                                         t1_employee_data
#                                                    │
#                                                    ▼
#                                       t2_employee_timeframe
#                                                    │
#                                                    ▼
#                                      t3_timeframe_to_strikes
#                                                    │
#                                                    ▼
#                                        t4_employee_leaves
#                                                    │
#                                            ┌───────┴──────┐
#                                            ▼              ▼
#                                      t5_dept_report  t6_leave_report
#                                            │              │
#                                            └──────┬───────┘
#                                                   ▼
#                                            monthly_check
#                                           /             \
#                                   run_monthly       skip_monthly
#                                           \             /
#                                            pipeline_done
# ─────────────────────────────────────────────────────────────

# Yearly branch
yearly_check >> [run_yearly_group, skip_yearly]
run_yearly_group >> [t_leave_quota, t_leave_calendar] >> yearly_done >> yearly_join
skip_yearly >> yearly_join

# Daily pipeline
yearly_join >> t1_employee_data >> t2_employee_timeframe >> t3_timeframe_to_strikes
t3_timeframe_to_strikes >> t4_employee_leaves
t4_employee_leaves >> [t5_dept_report, t6_leave_report]

# Monthly branch (after daily reports)
[t5_dept_report, t6_leave_report] >> monthly_check
monthly_check >> [t_availed_leaves, skip_monthly]
[t_availed_leaves, skip_monthly] >> pipeline_done