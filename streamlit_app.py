# SnowGuard — Live Insurance Pipeline Dashboard
# Paste this into Streamlit in Snowflake
import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
 
session = get_active_session()
 
st.set_page_config(page_title='SnowGuard', layout='wide')
st.title('❄️ SnowGuard — Insurance Pipeline Monitor')
st.caption('DNASURE | Real-time health monitoring for Claims, Policy & Premium')
 
# ── Latest status per pipeline ──────────────────────────
latest = session.sql('''
    SELECT pipeline_name, stress_score, risk_level,
           null_rate, duplicate_rate, minutes_since_load,
           check_time
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY pipeline_name
                   ORDER BY check_time DESC) AS rn
        FROM SNOWGUARD_DB.INSURANCE.PIPELINE_HEALTH_LOG
    ) WHERE rn = 1
    ORDER BY stress_score DESC
''').to_pandas()
 
# ── Top KPI cards ───────────────────────────────────────
col1, col2, col3 = st.columns(3)
for i, row in latest.iterrows():
    col = [col1, col2, col3][i]
    color = ('🔴' if row['RISK_LEVEL'] == 'CRITICAL'
             else '🟡' if row['RISK_LEVEL'] == 'WARNING'
             else '🟢')
    with col:
        st.metric(
            label=f"{color} {row['PIPELINE_NAME']} Pipeline",
            value=f"{row['STRESS_SCORE']}/100",
            delta=row['RISK_LEVEL']
        )
 
st.divider()
 
# ── Detailed table ──────────────────────────────────────
st.subheader('📋 Current Health Details')
st.dataframe(latest[[
    'PIPELINE_NAME','STRESS_SCORE','RISK_LEVEL',
    'NULL_RATE','DUPLICATE_RATE','MINUTES_SINCE_LOAD'
]], use_container_width=True)
 
# ── Trend chart ─────────────────────────────────────────
st.subheader('📈 Stress Score Trend (Last 24 Hours)')
trend = session.sql('''
    SELECT pipeline_name, check_time, stress_score
    FROM SNOWGUARD_DB.INSURANCE.PIPELINE_HEALTH_LOG
    WHERE check_time >= DATEADD(hour,-24,CURRENT_TIMESTAMP())
    ORDER BY check_time
''').to_pandas()
 
for pipeline in ['CLAIMS','POLICY','PREMIUM']:
    df = trend[trend['PIPELINE_NAME']==pipeline]
    if not df.empty:
        st.line_chart(df.set_index('CHECK_TIME')['STRESS_SCORE'],
                      use_container_width=True)
 
# ── Failure DNA library ─────────────────────────────────
st.subheader('🧬 Failure Pattern Library')
dna = session.sql('''
    SELECT pipeline_name, captured_at,
           null_rate, duplicate_rate, root_cause
    FROM SNOWGUARD_DB.INSURANCE.FAILURE_PATTERNS
    ORDER BY captured_at DESC LIMIT 20
''').to_pandas()
st.dataframe(dna, use_container_width=True)
 
# ── Auto-refresh every 60 seconds ──────────────────────
import time
st.caption(f'Last refreshed: {pd.Timestamp.now().strftime("%H:%M:%S")}')
time.sleep(60)
st.rerun()
