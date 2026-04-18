-- Create dedicated database and schema
CREATE DATABASE IF NOT EXISTS SNOWGUARD_DB;
USE DATABASE SNOWGUARD_DB;
CREATE SCHEMA IF NOT EXISTS INSURANCE;
USE SCHEMA INSURANCE;
 
-- Create warehouse (XS is enough for solo project)
CREATE WAREHOUSE IF NOT EXISTS SNOWGUARD_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;
USE WAREHOUSE SNOWGUARD_WH;

-- Claims raw table
CREATE TABLE IF NOT EXISTS RAW_CLAIMS (
    claim_id        STRING,
    policy_id       STRING,
    customer_id     STRING,
    claim_amount    NUMBER(18,2),
    claim_date      TIMESTAMP_NTZ,
    claim_status    STRING,
    loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
 
-- Policy raw table
CREATE TABLE IF NOT EXISTS RAW_POLICY (
    policy_id       STRING,
    customer_id     STRING,
    policy_type     STRING,
    start_date      DATE,
    end_date        DATE,
    premium_amount  NUMBER(18,2),
    loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
 
-- Premium raw table
CREATE TABLE IF NOT EXISTS RAW_PREMIUM (
    premium_id      STRING,
    policy_id       STRING,
    due_date        DATE,
    paid_amount     NUMBER(18,2),
    payment_status  STRING,
    loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Stores stress check results for all 3 pipelines
CREATE TABLE IF NOT EXISTS PIPELINE_HEALTH_LOG (
    log_id          STRING DEFAULT UUID_STRING(),
    pipeline_name   STRING,
    check_time      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    total_rows      NUMBER,
    null_rate       NUMBER(5,2),
    duplicate_rate  NUMBER(5,2),
    volume_drop_pct NUMBER(5,2),
    minutes_since_load NUMBER,
    stress_score    NUMBER(5,1),
    risk_level      STRING,
    alert_fired     BOOLEAN DEFAULT FALSE
);
 
-- Stores failure patterns for future reference
CREATE TABLE IF NOT EXISTS FAILURE_PATTERNS (
    pattern_id      STRING DEFAULT UUID_STRING(),
    pipeline_name   STRING,
    captured_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    null_rate       NUMBER(5,2),
    duplicate_rate  NUMBER(5,2),
    root_cause      STRING
);

CREATE OR REPLACE FILE FORMAT SNOWGUARD_CSV_FORMAT
TYPE = 'CSV'
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'
NULL_IF = ('NULL', '');

CREATE OR REPLACE STAGE SNOWGUARD_STAGE;

COPY INTO RAW_CLAIMS
FROM @SNOWGUARD_STAGE/claims_data_120.csv
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);

SELECT * FROM RAW_CLAIMS;

COPY INTO RAW_POLICY
FROM @SNOWGUARD_STAGE/policy_data_120.csv
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);

SELECT * FROM RAW_POLICY;

COPY INTO RAW_PREMIUM
FROM @SNOWGUARD_STAGE/premium_data_120.csv
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);

SELECT * FROM RAW_PREMIUM;