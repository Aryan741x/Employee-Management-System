"""
Monthly Cooldown Script
-----------------------
Resets strikes and restores salary for all NON-INACTIVE employees.
Employees with is_inactive = TRUE (10+ strikes) are excluded.

Runs on the 1st of each month via the Airflow DAG.
"""
import psycopg2

def run_monthly_cooldown():
    conn = psycopg2.connect(
        dbname="capstone_project2",
        user="postgres",
        password="postgres",
        host="postgres",
        port="5432"
    )
    cur = conn.cursor()

    # Reset strikes and restore salary for non-suspended employees
    cur.execute("""
        UPDATE employee_strikes
        SET
            no_of_strikes = 0,
            strike_1  = NULL,
            strike_2  = NULL,
            strike_3  = NULL,
            strike_4  = NULL,
            strike_5  = NULL,
            strike_6  = NULL,
            strike_7  = NULL,
            strike_8  = NULL,
            strike_9  = NULL,
            strike_10 = NULL
        WHERE is_inactive = FALSE OR is_inactive IS NULL;
    """)

    affected = cur.rowcount
    conn.commit()

    # Also clean up old flagged_messages for cooled-down employees
    # (keep history but only for the last 30 days for non-inactive)
    cur.execute("""
        DELETE FROM flagged_messages
        WHERE employee_id IN (
            SELECT employee_id FROM employee_strikes
            WHERE is_inactive = FALSE OR is_inactive IS NULL
        )
        AND start_date < NOW() - INTERVAL '30 days';
    """)

    conn.commit()
    print(f"Monthly cooldown complete. Reset strikes for {affected} active employees.")
    print("Employees with is_inactive=TRUE were excluded from cooldown.")

    cur.close()
    conn.close()

if __name__ == "__main__":
    run_monthly_cooldown()
