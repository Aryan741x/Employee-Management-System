-- =============================================================
-- PostgreSQL init script
-- Runs automatically on first container start via
--   /docker-entrypoint-initdb.d/init.sql
-- =============================================================

-- ── 1. Create separate Airflow metadata database ─────────────
-- (default DB is capstone_project2; Airflow needs its own)
SELECT 'CREATE DATABASE airflow'
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'airflow'
)\gexec

-- ── 2. Application tables live in capstone_project2 (default) ─

-- Main strikes table
CREATE TABLE IF NOT EXISTS employee_strikes (
    employee_id   TEXT PRIMARY KEY,
    salary        DOUBLE PRECISION,
    strike_1      REAL DEFAULT NULL,
    strike_2      REAL DEFAULT NULL,
    strike_3      REAL DEFAULT NULL,
    strike_4      REAL DEFAULT NULL,
    strike_5      REAL DEFAULT NULL,
    strike_6      REAL DEFAULT NULL,
    strike_7      REAL DEFAULT NULL,
    strike_8      REAL DEFAULT NULL,
    strike_9      REAL DEFAULT NULL,
    strike_10     REAL DEFAULT NULL,
    no_of_strikes INTEGER DEFAULT 0,
    is_inactive   BOOLEAN DEFAULT FALSE
);

-- Staging / temp table used by timeframeToStrikes.py
CREATE TABLE IF NOT EXISTS employee_strikes_stg (
    employee_id   TEXT PRIMARY KEY,
    salary        DOUBLE PRECISION,
    strike_1      REAL DEFAULT NULL,
    strike_2      REAL DEFAULT NULL,
    strike_3      REAL DEFAULT NULL,
    strike_4      REAL DEFAULT NULL,
    strike_5      REAL DEFAULT NULL,
    strike_6      REAL DEFAULT NULL,
    strike_7      REAL DEFAULT NULL,
    strike_8      REAL DEFAULT NULL,
    strike_9      REAL DEFAULT NULL,
    strike_10     REAL DEFAULT NULL,
    no_of_strikes INTEGER DEFAULT 0
);

-- Flagged messages log
CREATE TABLE IF NOT EXISTS flagged_messages (
    id          SERIAL PRIMARY KEY,
    employee_id TEXT        NOT NULL,
    start_date  TIMESTAMP   NOT NULL DEFAULT NOW()
);

-- Index for faster employee lookups
CREATE INDEX IF NOT EXISTS idx_flagged_emp ON flagged_messages (employee_id);
CREATE INDEX IF NOT EXISTS idx_strikes_emp ON employee_strikes (employee_id);
