-- Run as ACCOUNTADMIN
CREATE OR REPLACE NOTIFICATION INTEGRATION SNOWGUARD_EMAIL
    TYPE = EMAIL
    ENABLED = TRUE;
-- Alert fires when stress score >= 70 (CRITICAL)
CREATE OR REPLACE ALERT ALERT_CLAIMS_CRITICAL
    WAREHOUSE = SNOWGUARD_WH
    SCHEDULE  = '5 MINUTE'
IF (EXISTS (
    SELECT 1 FROM PIPELINE_HEALTH_LOG
    WHERE pipeline_name = 'CLAIMS'
    AND   risk_level    = 'CRITICAL'
    AND   alert_fired   = FALSE
    AND   check_time   >= DATEADD('minute', -5, CURRENT_TIMESTAMP())
))


THEN CALL SYSTEM$SEND_EMAIL(
    'SNOWGUARD_EMAIL',
    'taruntejys2010@gmail.com',
    '⚠️ SNOWGUARD CRITICAL: Claims Pipeline at Risk',
    (SELECT
        'SNOWGUARD ALERT' || CHR(10) ||
        '========================' || CHR(10) ||
        'Pipeline   : CLAIMS' || CHR(10) ||
        'Stress Score: ' || stress_score || '/100' || CHR(10) ||
        'Null Rate   : ' || null_rate || '%' || CHR(10) ||
        'Dup Rate    : ' || duplicate_rate || '%' || CHR(10) ||
        'Action      : Immediate review required' || CHR(10) ||
        '========================' || CHR(10) ||
        'SnowGuard | DNASURE Insurance Ops'
    FROM PIPELINE_HEALTH_LOG
    WHERE pipeline_name = 'CLAIMS'
    AND   risk_level    = 'CRITICAL'
    ORDER BY check_time DESC LIMIT 1)
);
 
-- Repeat same pattern for POLICY and PREMIUM alerts
-- Just change pipeline_name and email subject
 
-- Resume alerts
ALTER ALERT ALERT_CLAIMS_CRITICAL RESUME;
