-- Stored procedure for daily health check on partner transaction streaming data.
-- Co-authored with CoCo
-- ============================================================================
-- STORED PROCEDURE: CHECK_PARTNER_TRANSACTIONS_HEALTH
-- Purpose: Performs a daily health check on continuous streaming data.
-- It catches corrupt files that Snowpipe skipped and alerts on missing schema fields.
-- ============================================================================
CREATE OR REPLACE PROCEDURE FINTECH_PROD.GOVERNANCE.CHECK_PARTNER_TRANSACTIONS_HEALTH()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    parse_error_count INT DEFAULT 0;
    missing_key_count INT DEFAULT 0;
    result_summary STRING DEFAULT '';
BEGIN
    -- ------------------------------------------------------------------
    -- CHECK 1: Look for Corrupt / Unparseable Files
    -- ------------------------------------------------------------------
    -- If a file in S3 is broken or not valid JSON, Snowpipe skips it completely.
    -- Because it never lands in the raw table, Dynamic Tables can't see it.
    -- VALIDATE_PIPE_LOAD does not support pipes with transforms, so we use
    -- COPY_HISTORY instead to find files that failed or had errors.
    
    CREATE OR REPLACE TEMPORARY TABLE TEMP_PIPE_ERRORS AS
    SELECT *
    FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'FINTECH_PROD.RAW.PARTNER_TRANSACTIONS',
        START_TIME => DATEADD('day', -1, CURRENT_TIMESTAMP())
    ))
    WHERE STATUS = 'LOAD_FAILED' OR ERROR_COUNT > 0;
    
    -- Count how many bad files were skipped today
    parse_error_count := (SELECT COUNT(*) FROM TEMP_PIPE_ERRORS);

    -- ------------------------------------------------------------------
    -- CHECK 2: Look for Schema Drift (Missing Fields)
    -- ------------------------------------------------------------------
    -- Pull the baseline list of fields we expected to see from SCHEMA_SNAPSHOTS
    LET expected_keys ARRAY := (
        SELECT COALESCE(ARRAY_AGG(column_key), ARRAY_CONSTRUCT())
        FROM FINTECH_PROD.GOVERNANCE.SCHEMA_SNAPSHOTS
        WHERE source_name = 'partner_transactions'
    );
    
    -- Extract all actual top-level JSON fields present in the raw data over the last 24 hours
    LET actual_keys ARRAY := (
        SELECT ARRAY_AGG(DISTINCT UPPER(f.key))
        FROM FINTECH_PROD.RAW.PARTNER_TRANSACTIONS, LATERAL FLATTEN(input => raw_payload) f
        WHERE _loaded_at >= DATEADD('day', -1, CURRENT_TIMESTAMP())
    );
    
    -- Compare expected vs actual fields to find anything that went missing
    LET missing_keys ARRAY := ARRAY_EXCEPT(:expected_keys, :actual_keys);
    missing_key_count := ARRAY_SIZE(:missing_keys);

    -- ------------------------------------------------------------------
    -- CHECK 3: Build the Result Summary & Update Field Baselines
    -- ------------------------------------------------------------------
    -- Append parse error details if any files failed
    IF (parse_error_count > 0) THEN
        result_summary := result_summary || parse_error_count || ' file(s) failed to parse. ';
    END IF;
    
    -- Append schema drift details if any expected fields disappeared
    IF (missing_key_count > 0) THEN
        result_summary := result_summary || 'Schema drift: missing ' || ARRAY_TO_STRING(:missing_keys, ', ') || '. ';
    END IF;

    -- Update SCHEMA_SNAPSHOTS so new JSON fields seen today are remembered for tomorrow
    MERGE INTO FINTECH_PROD.GOVERNANCE.SCHEMA_SNAPSHOTS AS target
    USING (SELECT VALUE::STRING AS column_key FROM TABLE(FLATTEN(:actual_keys))) AS source
    ON target.source_name = 'partner_transactions' AND target.column_key = source.column_key
    WHEN MATCHED THEN UPDATE SET last_seen_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (source_name, column_key) VALUES ('partner_transactions', source.column_key);

    -- If no issues were found, return a success message; otherwise return the error summary
    IF (LENGTH(result_summary) = 0) THEN
        RETURN 'Clean: no parse errors, no schema drift in the last 24h.';
    END IF;
    
    RETURN result_summary;
END;
$$;

-- ============================================================================
-- AUTOMATED TASK SCHEDULE
-- Purpose: Automatically runs the health check procedure once every day at 8:00 AM UTC.
-- ============================================================================
CREATE OR REPLACE TASK FINTECH_PROD.GOVERNANCE.CHECK_PARTNER_TRANSACTIONS_HEALTH_TASK
    WAREHOUSE = INGEST_WH
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
AS
CALL FINTECH_PROD.GOVERNANCE.CHECK_PARTNER_TRANSACTIONS_HEALTH();

-- Resume the task so it starts running on schedule
ALTER TASK FINTECH_PROD.GOVERNANCE.CHECK_PARTNER_TRANSACTIONS_HEALTH_TASK RESUME;

-- ============================================================================
-- PERMISSIONS & TEST RUN
-- ============================================================================
-- Grant execution permissions to the transformation role
GRANT USAGE ON PROCEDURE FINTECH_PROD.GOVERNANCE.CHECK_PARTNER_TRANSACTIONS_HEALTH() TO ROLE FINTECH_TRANSFORMER;

-- Execute manually right now to confirm everything works as expected
CALL FINTECH_PROD.GOVERNANCE.CHECK_PARTNER_TRANSACTIONS_HEALTH();

