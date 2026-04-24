# SnowGuard - Insurance Pipeline Monitor

Real-time data quality monitoring for Insurance pipelines (Claims, Policy & Premium) built entirely on Snowflake.

## Architecture

```
CSV Files --> @SNOWGUARD_STAGE --> RAW Tables --> Health Check Procedures --> PIPELINE_HEALTH_LOG
                                                        |                          |
                                                   Failure DNA              Stress Score
                                                   (patterns)            (0-100 scoring)
                                                        |                          |
                                                        v                          v
                                                FAILURE_PATTERNS         Email Alerts (CRITICAL/WARNING)
```

## Project Structure

| File | Description |
|------|-------------|
| `SNOWGAURD.sql` | Database, schema, warehouse, tables, stage setup & CSV loading |
| `SNOWGUARD_PROC.sql` | Health check stored procedures for all 3 pipelines |
| `SNOWGUARD_TASK.sql` | Automated task scheduling (1-min health checks + email alerts) |
| `SNOWGUARD_ALERTS.sql` | Notification integration & alert configuration |
| `SNOWGUARD_MASK.sql` | Dynamic data masking policy for customer_id |
| `SNOWGUARD_DUMMY.sql` | Test data for simulating CRITICAL, WARNING & HEALTHY scenarios |

## Setup (Run in Order)

```sql
-- 1. Infrastructure
Run: SNOWGAURD.sql

-- 2. Procedures
Run: SNOWGUARD_PROC.sql

-- 3. Masking
Run: SNOWGUARD_MASK.sql

-- 4. Alerts
Run: SNOWGUARD_ALERTS.sql

-- 5. Tasks (automated monitoring)
Run: SNOWGUARD_TASK.sql
```

## Stress Score Rules

| Check | Threshold | Points |
|-------|-----------|--------|
| Null rate | > 5% | +30 |
| Duplicate rate | > 2% | +25 |
| Volume drop | > 50% | +25 |
| Freshness lag | > 60 min | +20 |

| Score | Risk Level |
|-------|------------|
| >= 70 | CRITICAL |
| >= 40 | WARNING |
| < 40 | HEALTHY |

## Features

- **3-Pipeline Monitoring**: Claims, Policy, Premium health checks
- **Stress Scoring**: 0-100 composite score based on null rate, duplicates, volume drops, freshness
- **Automated Tasks**: Runs every 1 minute via Snowflake Tasks
- **Email Alerts**: Auto-sends email on CRITICAL/WARNING via notification integration
- **Failure DNA Library**: Stores failure patterns for root cause analysis
- **Data Masking**: Dynamic masking on customer_id (role-based access)
- **Streamlit Dashboard**: Real-time visual monitoring (SnowGuard UI)

## Streamlit Dashboard

The project includes a Streamlit in Snowflake dashboard with:
- KPI cards per pipeline (color-coded risk)
- Stress score trend chart (last 24 hours)
- Detailed health metrics table
- Failure pattern library
- Auto-refresh every 60 seconds

## Cost Control

- Warehouse: `SNOWGUARD_WH` (X-Small, auto-suspend 60s)
- Suspend all tasks when not needed:
```sql
ALTER TASK TASK_CHECK_CLAIMS SUSPEND;
ALTER TASK TASK_CHECK_POLICY SUSPEND;
ALTER TASK TASK_CHECK_PREMIUM SUSPEND;
ALTER TASK TASK_ALERT_EMAIL SUSPEND;
```

## Tech Stack

- Snowflake (SQL, Stored Procedures, Tasks, Alerts)
- Streamlit in Snowflake
- Snowflake Email Notification Integration
- Dynamic Data Masking

## Author

*Prasad* | Built with Snowflake
