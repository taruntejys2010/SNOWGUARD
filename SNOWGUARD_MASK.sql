-- Masking policy for customer IDs (show only last 4 chars)
CREATE OR REPLACE MASKING POLICY MASK_CUSTOMER_ID
    AS (val STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('SYSADMIN','SNOWGUARD_ADMIN')
        THEN val
        ELSE CONCAT('***-', RIGHT(val, 4))
    END;
 
-- Apply to RAW_CLAIMS customer_id column
ALTER TABLE RAW_CLAIMS MODIFY COLUMN customer_id
    SET MASKING POLICY MASK_CUSTOMER_ID;
 
-- Apply to RAW_POLICY customer_id column
ALTER TABLE RAW_POLICY MODIFY COLUMN customer_id
    SET MASKING POLICY MASK_CUSTOMER_ID;
