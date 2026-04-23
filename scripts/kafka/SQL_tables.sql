-- =========================
-- MAIN TABLE
-- =========================
CREATE TABLE IF NOT EXISTS employee_strikes (
    employee_id TEXT PRIMARY KEY,
    salary DOUBLE PRECISION,
    strike_1 REAL DEFAULT NULL,
    strike_2 REAL DEFAULT NULL,
    strike_3 REAL DEFAULT NULL,
    strike_4 REAL DEFAULT NULL,
    strike_5 REAL DEFAULT NULL,
    strike_6 REAL DEFAULT NULL,
    strike_7 REAL DEFAULT NULL,
    strike_8 REAL DEFAULT NULL,
    strike_9 REAL DEFAULT NULL,
    strike_10 REAL DEFAULT NULL,
    no_of_strikes INTEGER DEFAULT 0
);

-- =========================
-- FLAGGED MESSAGES TABLE
-- =========================
CREATE TABLE IF NOT EXISTS flagged_messages (
    employee_id TEXT,
    start_date TIMESTAMP
);

-- =========================
-- STAGING TABLE
-- =========================
CREATE TABLE IF NOT EXISTS employee_strikes_stg (
    employee_id TEXT PRIMARY KEY,
    salary DOUBLE PRECISION,
    strike_1 REAL DEFAULT NULL,
    strike_2 REAL DEFAULT NULL,
    strike_3 REAL DEFAULT NULL,
    strike_4 REAL DEFAULT NULL,
    strike_5 REAL DEFAULT NULL,
    strike_6 REAL DEFAULT NULL,
    strike_7 REAL DEFAULT NULL,
    strike_8 REAL DEFAULT NULL,
    strike_9 REAL DEFAULT NULL,
    strike_10 REAL DEFAULT NULL,
    no_of_strikes INTEGER DEFAULT 0
);