-- Run this manually or let Task call it automatically
CREATE OR REPLACE PROCEDURE CHECK_CLAIMS_HEALTH()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    v_total_rows    NUMBER;
    v_null_rate     NUMBER;
    v_dup_rate      NUMBER;
    v_vol_drop      NUMBER;
    v_mins_since    NUMBER;
    v_score         NUMBER := 0;
    v_risk          STRING;
    v_avg_daily     NUMBER;
BEGIN
 
    -- Count total rows loaded today
    SELECT COUNT(*) INTO :v_total_rows
    FROM RAW_CLAIMS
    WHERE loaded_at >= DATEADD('hour', -24, CURRENT_TIMESTAMP());
 
    -- Calculate null rate on critical fields
    SELECT ROUND(
        (SUM(CASE WHEN claim_id IS NULL OR policy_id IS NULL
                   OR claim_amount IS NULL THEN 1 ELSE 0 END)
         / NULLIF(COUNT(*),0)) * 100, 2)
    INTO :v_null_rate
    FROM RAW_CLAIMS
    WHERE loaded_at >= DATEADD('hour', -24, CURRENT_TIMESTAMP());
 
    -- Calculate duplicate rate on claim_id
    SELECT ROUND(
        (1 - COUNT(DISTINCT claim_id) / NULLIF(COUNT(*),0)) * 100, 2)
    INTO :v_dup_rate
    FROM RAW_CLAIMS
    WHERE loaded_at >= DATEADD('hour', -24, CURRENT_TIMESTAMP());
 
    -- Get average daily volume (last 7 days)
    SELECT AVG(daily_count) INTO :v_avg_daily FROM (
        SELECT DATE(loaded_at), COUNT(*) AS daily_count
        FROM RAW_CLAIMS
        WHERE loaded_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
        GROUP BY 1
    );
 
    -- Volume drop percentage vs average
    v_vol_drop := CASE WHEN :v_avg_daily > 0
        THEN ROUND((1 - :v_total_rows / :v_avg_daily) * 100, 2)
        ELSE 0 END;
 
    -- Minutes since last record loaded
    SELECT DATEDIFF('minute', MAX(loaded_at), CURRENT_TIMESTAMP())
    INTO :v_mins_since FROM RAW_CLAIMS;
    -- Calculate stress score using rules
    v_score := 0;
    IF (:v_null_rate > 5)  THEN v_score := :v_score + 30; END IF;
    IF (:v_dup_rate > 2)   THEN v_score := :v_score + 25; END IF;
    IF (:v_vol_drop > 50)  THEN v_score := :v_score + 25; END IF;
    IF (:v_mins_since > 60) THEN v_score := :v_score + 20; END IF;
 
    -- Assign risk level
    v_risk := CASE
        WHEN :v_score >= 70 THEN 'CRITICAL'
        WHEN :v_score >= 40 THEN 'WARNING'
        ELSE 'HEALTHY' END;
 
    -- Write result to health log
    INSERT INTO PIPELINE_HEALTH_LOG (
        pipeline_name, total_rows, null_rate, duplicate_rate,
        volume_drop_pct, minutes_since_load, stress_score,
        risk_level, alert_fired)
    VALUES ('CLAIMS', :v_total_rows, :v_null_rate, :v_dup_rate,
            :v_vol_drop, :v_mins_since, :v_score,
            :v_risk, FALSE);
 
    -- Store failure DNA if critical
    IF (:v_score >= 70) THEN
        INSERT INTO FAILURE_PATTERNS (pipeline_name, null_rate,
            duplicate_rate, root_cause)
        VALUES ('CLAIMS', :v_null_rate, :v_dup_rate,
            'Score: ' || :v_score || ' | Nulls: ' || :v_null_rate ||
            '% | Dups: ' || :v_dup_rate || '%');
    END IF;
 
    RETURN 'CLAIMS checked. Score: ' || :v_score || ' | ' || :v_risk;
END;
$$;

CREATE OR REPLACE PROCEDURE CHECK_POLICY_HEALTH()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    v_total_rows    NUMBER;
    v_null_rate     NUMBER;
    v_dup_rate      NUMBER;
    v_vol_drop      NUMBER;
    v_mins_since    NUMBER;
    v_score         NUMBER := 0;
    v_risk          STRING;
    v_avg_daily     NUMBER;
BEGIN

    SELECT COUNT(*) INTO :v_total_rows
    FROM RAW_POLICY
    WHERE loaded_at >= DATEADD('hour', -24, CURRENT_TIMESTAMP());

    SELECT ROUND(
        (SUM(CASE WHEN policy_id IS NULL OR customer_id IS NULL
                   OR premium_amount IS NULL THEN 1 ELSE 0 END)
         / NULLIF(COUNT(*),0)) * 100, 2)
    INTO :v_null_rate
    FROM RAW_POLICY
    WHERE loaded_at >= DATEADD('hour', -24, CURRENT_TIMESTAMP());

    SELECT ROUND(
        (1 - COUNT(DISTINCT policy_id) / NULLIF(COUNT(*),0)) * 100, 2)
    INTO :v_dup_rate
    FROM RAW_POLICY
    WHERE loaded_at >= DATEADD('hour', -24, CURRENT_TIMESTAMP());

    SELECT AVG(daily_count) INTO :v_avg_daily FROM (
        SELECT DATE(loaded_at), COUNT(*) AS daily_count
        FROM RAW_POLICY
        WHERE loaded_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
        GROUP BY 1
    );

    v_vol_drop := CASE WHEN :v_avg_daily > 0
        THEN ROUND((1 - :v_total_rows / :v_avg_daily) * 100, 2)
        ELSE 0 END;

    SELECT DATEDIFF('minute', MAX(loaded_at), CURRENT_TIMESTAMP())
    INTO :v_mins_since FROM RAW_POLICY;

    v_score := 0;
    IF (:v_null_rate > 5)  THEN v_score := :v_score + 30; END IF;
    IF (:v_dup_rate > 2)   THEN v_score := :v_score + 25; END IF;
    IF (:v_vol_drop > 50)  THEN v_score := :v_score + 25; END IF;
    IF (:v_mins_since > 60) THEN v_score := :v_score + 20; END IF;

    v_risk := CASE
        WHEN :v_score >= 70 THEN 'CRITICAL'
        WHEN :v_score >= 40 THEN 'WARNING'
        ELSE 'HEALTHY' END;

    INSERT INTO PIPELINE_HEALTH_LOG (
        pipeline_name, total_rows, null_rate, duplicate_rate,
        volume_drop_pct, minutes_since_load, stress_score,
        risk_level, alert_fired)
    VALUES ('POLICY', :v_total_rows, :v_null_rate, :v_dup_rate,
            :v_vol_drop, :v_mins_since, :v_score,
            :v_risk, FALSE);

    IF (:v_score >= 70) THEN
        INSERT INTO FAILURE_PATTERNS (pipeline_name, null_rate,
            duplicate_rate, root_cause)
        VALUES ('POLICY', :v_null_rate, :v_dup_rate,
            'Score: ' || :v_score || ' | Nulls: ' || :v_null_rate ||
            '% | Dups: ' || :v_dup_rate || '%');
    END IF;

    RETURN 'POLICY checked. Score: ' || :v_score || ' | ' || :v_risk;
END;
$$;

CREATE OR REPLACE PROCEDURE CHECK_PREMIUM_HEALTH()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    v_total_rows    NUMBER;
    v_null_rate     NUMBER;
    v_dup_rate      NUMBER;
    v_vol_drop      NUMBER;
    v_mins_since    NUMBER;
    v_score         NUMBER := 0;
    v_risk          STRING;
    v_avg_daily     NUMBER;
BEGIN

    SELECT COUNT(*) INTO :v_total_rows
    FROM RAW_PREMIUM
    WHERE loaded_at >= DATEADD('hour', -24, CURRENT_TIMESTAMP());

    SELECT ROUND(
        (SUM(CASE WHEN premium_id IS NULL OR policy_id IS NULL
                   OR paid_amount IS NULL THEN 1 ELSE 0 END)
         / NULLIF(COUNT(*),0)) * 100, 2)
    INTO :v_null_rate
    FROM RAW_PREMIUM
    WHERE loaded_at >= DATEADD('hour', -24, CURRENT_TIMESTAMP());

    SELECT ROUND(
        (1 - COUNT(DISTINCT premium_id) / NULLIF(COUNT(*),0)) * 100, 2)
    INTO :v_dup_rate
    FROM RAW_PREMIUM
    WHERE loaded_at >= DATEADD('hour', -24, CURRENT_TIMESTAMP());

    SELECT AVG(daily_count) INTO :v_avg_daily FROM (
        SELECT DATE(loaded_at), COUNT(*) AS daily_count
        FROM RAW_PREMIUM
        WHERE loaded_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
        GROUP BY 1
    );

    v_vol_drop := CASE WHEN :v_avg_daily > 0
        THEN ROUND((1 - :v_total_rows / :v_avg_daily) * 100, 2)
        ELSE 0 END;

    SELECT DATEDIFF('minute', MAX(loaded_at), CURRENT_TIMESTAMP())
    INTO :v_mins_since FROM RAW_PREMIUM;

    v_score := 0;
    IF (:v_null_rate > 5)  THEN v_score := :v_score + 30; END IF;
    IF (:v_dup_rate > 2)   THEN v_score := :v_score + 25; END IF;
    IF (:v_vol_drop > 50)  THEN v_score := :v_score + 25; END IF;
    IF (:v_mins_since > 60) THEN v_score := :v_score + 20; END IF;

    v_risk := CASE
        WHEN :v_score >= 70 THEN 'CRITICAL'
        WHEN :v_score >= 40 THEN 'WARNING'
        ELSE 'HEALTHY' END;

    INSERT INTO PIPELINE_HEALTH_LOG (
        pipeline_name, total_rows, null_rate, duplicate_rate,
        volume_drop_pct, minutes_since_load, stress_score,
        risk_level, alert_fired)
    VALUES ('PREMIUM', :v_total_rows, :v_null_rate, :v_dup_rate,
            :v_vol_drop, :v_mins_since, :v_score,
            :v_risk, FALSE);

    IF (:v_score >= 70) THEN
        INSERT INTO FAILURE_PATTERNS (pipeline_name, null_rate,
            duplicate_rate, root_cause)
        VALUES ('PREMIUM', :v_null_rate, :v_dup_rate,
            'Score: ' || :v_score || ' | Nulls: ' || :v_null_rate ||
            '% | Dups: ' || :v_dup_rate || '%');
    END IF;

    RETURN 'PREMIUM checked. Score: ' || :v_score || ' | ' || :v_risk;
END;
$$;
