-- ============================================================================
-- STEP 1: Create persistent landing and schema snapshot tracking tables.
-- ============================================================================
CREATE TABLE IF NOT EXISTS FINTECH_PROD.GOVERNANCE.LANDING_ACCOUNT_ACTIVITY (
    raw_payload   VARIANT,
    _source_file  STRING
);

CREATE TABLE IF NOT EXISTS FINTECH_PROD.GOVERNANCE.SCHEMA_SNAPSHOTS (
    source_name    STRING,
    column_key     STRING,
    first_seen_at  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    last_seen_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- STEP 2: Define the procedure to load, validate, audit, and promote data.
-- ============================================================================
CREATE OR REPLACE PROCEDURE FINTECH_PROD.GOVERNANCE.LOAD_AND_VALIDATE_ACCOUNT_ACTIVITY()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    total_rows_loaded INT DEFAULT 0;
    null_rows INT DEFAULT 0;
    missing_key_count INT DEFAULT 0;
    err_msg STRING;
BEGIN
    -- ------------------------------------------------------------------------
    -- 1. INGESTION: Copy new NDJSON files from S3 stage into landing table.
    -- ------------------------------------------------------------------------
    COPY INTO FINTECH_PROD.GOVERNANCE.LANDING_ACCOUNT_ACTIVITY (raw_payload, _source_file)
    FROM (SELECT $1, METADATA$FILENAME FROM @FINTECH_PROD.RAW.ACCOUNT_ACTIVITY_STAGE)
    FILE_FORMAT = (FORMAT_NAME = FINTECH_PROD.RAW.NDJSON_FORMAT)
    ON_ERROR = 'ABORT_STATEMENT';

    -- ------------------------------------------------------------------------
    -- 2. FILE TRACKING: Extract file names loaded in this specific run.
    -- ------------------------------------------------------------------------
    LET copy_qid STRING := LAST_QUERY_ID();
    LET new_files ARRAY := ARRAY_CONSTRUCT();
    BEGIN
        new_files := (
            SELECT COALESCE(ARRAY_AGG("file"), ARRAY_CONSTRUCT())
            FROM TABLE(RESULT_SCAN(:copy_qid))
            WHERE "status" = 'LOADED'
        );
    EXCEPTION
        WHEN OTHER THEN
            new_files := ARRAY_CONSTRUCT();
    END;

    -- Exit early if no new files arrived in this run
    IF (ARRAY_SIZE(:new_files) = 0) THEN
        RETURN 'No new files to load -- all files already loaded previously.';
    END IF;

    -- ------------------------------------------------------------------------
    -- 3. VALIDATION CHECKS (Run ONLY on newly loaded files from this batch)
    -- ------------------------------------------------------------------------

    -- Check A: Total row count for newly ingested files
    total_rows_loaded := (
        SELECT COUNT(*) FROM FINTECH_PROD.GOVERNANCE.LANDING_ACCOUNT_ACTIVITY s
        WHERE EXISTS (SELECT 1 FROM TABLE(FLATTEN(:new_files)) f WHERE ENDSWITH(f.value::STRING, s._source_file))
    );

    -- Check B: Dynamic Schema Drift Check
    -- Fetch previous baseline keys from SCHEMA_SNAPSHOTS
    LET expected_keys ARRAY := (
        SELECT COALESCE(ARRAY_AGG(column_key), ARRAY_CONSTRUCT())
        FROM FINTECH_PROD.GOVERNANCE.SCHEMA_SNAPSHOTS
        WHERE source_name = 'account_activity'
    );

    -- Extract distinct top-level JSON keys present in today's run
    LET actual_keys ARRAY := (
        SELECT ARRAY_AGG(DISTINCT UPPER(f.key))
        FROM FINTECH_PROD.GOVERNANCE.LANDING_ACCOUNT_ACTIVITY s, LATERAL FLATTEN(input => s.raw_payload) f
        WHERE EXISTS (SELECT 1 FROM TABLE(FLATTEN(:new_files)) nf WHERE ENDSWITH(nf.value::STRING, s._source_file))
    );

    -- Identify if any historically required keys are missing from today's load
    LET missing_keys ARRAY := ARRAY_EXCEPT(:expected_keys, :actual_keys);
    missing_key_count := ARRAY_SIZE(:missing_keys);

    -- Check C: Ensure primary business key (account_id) is not NULL
    null_rows := (
        SELECT COUNT(*) FROM FINTECH_PROD.GOVERNANCE.LANDING_ACCOUNT_ACTIVITY s
        WHERE EXISTS (SELECT 1 FROM TABLE(FLATTEN(:new_files)) f WHERE ENDSWITH(f.value::STRING, s._source_file))
        AND raw_payload:account_id IS NULL
    );

    -- ------------------------------------------------------------------------
    -- 4. ERROR EVALUATION: Prioritize and format error messages
    -- ------------------------------------------------------------------------
    IF (missing_key_count > 0) THEN
        err_msg := 'Schema drift: missing fields ' || ARRAY_TO_STRING(:missing_keys, ', ');
    ELSEIF (null_rows > 0) THEN
        err_msg := null_rows || ' rows with missing account_id.';
    ELSEIF (total_rows_loaded = 0) THEN
        err_msg := 'No rows found for newly loaded file(s).';
    ELSE
        err_msg := NULL; -- All checks passed!
    END IF;

    -- ------------------------------------------------------------------------
    -- 5. AUDIT LOGGING: Record execution status in governance log
    -- ------------------------------------------------------------------------
    INSERT INTO FINTECH_PROD.GOVERNANCE.INGESTION_AUDIT_LOG (
        source_name, status, load_timestamp, rows_loaded, duplicate_rows, null_rows, error_message
    )
    VALUES (
        'account_activity', IFF(:err_msg IS NULL, 'Success', 'Failed'),
        CURRENT_TIMESTAMP(), :total_rows_loaded, 0, :null_rows, :err_msg
    );

    -- ------------------------------------------------------------------------
    -- 6. GUARDRAIL: If validation failed, throw an exception to abort.
    -- ------------------------------------------------------------------------
    IF (:err_msg IS NOT NULL) THEN
        EXECUTE IMMEDIATE
            'DECLARE validation_failed EXCEPTION (-20001, ''' ||
            REPLACE(:err_msg, '''', '''''') ||
            '''); BEGIN RAISE validation_failed; END;';
    END IF;

    -- ------------------------------------------------------------------------
    -- 7. PROMOTION TO RAW: Move validated records to target RAW table.
    -- ------------------------------------------------------------------------
    INSERT INTO FINTECH_PROD.RAW.ACCOUNT_ACTIVITY (raw_payload, _source_file)
    SELECT raw_payload, _source_file 
    FROM FINTECH_PROD.GOVERNANCE.LANDING_ACCOUNT_ACTIVITY s
    WHERE EXISTS (SELECT 1 FROM TABLE(FLATTEN(:new_files)) f WHERE ENDSWITH(f.value::STRING, s._source_file));

    -- ------------------------------------------------------------------------
    -- 8. BASELINE UPDATE: Update baseline snapshot for subsequent runs.
    -- ------------------------------------------------------------------------
    MERGE INTO FINTECH_PROD.GOVERNANCE.SCHEMA_SNAPSHOTS AS target
    USING (
        SELECT DISTINCT f.value::STRING AS column_key
        FROM TABLE(FLATTEN(input => :actual_keys)) f
    ) AS source
    ON target.source_name = 'account_activity' AND target.column_key = source.column_key
    WHEN MATCHED THEN 
        UPDATE SET last_seen_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN 
        INSERT (source_name, column_key) VALUES ('account_activity', source.column_key);

    RETURN 'Success: loaded ' || total_rows_loaded || ' new row(s).';
END;
$$;

-- Execute the procedure
CALL FINTECH_PROD.GOVERNANCE.LOAD_AND_VALIDATE_ACCOUNT_ACTIVITY();

-- Execute the procedure
CALL FINTECH_PROD.GOVERNANCE.LOAD_AND_VALIDATE_ACCOUNT_ACTIVITY();

USE ROLE ACCOUNTADMIN;

GRANT USAGE ON PROCEDURE FINTECH_PROD.GOVERNANCE.LOAD_AND_VALIDATE_ACCOUNT_ACTIVITY() TO ROLE FINTECH_TRANSFORMER;

GRANT USAGE ON PROCEDURE FINTECH_PROD.GOVERNANCE.LOAD_AND_VALIDATE_SETTLEMENTS() TO ROLE FINTECH_TRANSFORMER;

GRANT USAGE ON PROCEDURE FINTECH_PROD.GOVERNANCE.LOAD_AND_VALIDATE_EXCHANGE_RATES() TO ROLE FINTECH_TRANSFORMER;

GRANT USAGE ON PROCEDURE FINTECH_PROD.GOVERNANCE.LOAD_AND_VALIDATE_SUPPORT_CASES() TO ROLE FINTECH_TRANSFORMER;



USE ROLE ACCOUNTADMIN;

GRANT SELECT, INSERT ON TABLE FINTECH_PROD.GOVERNANCE.INGESTION_AUDIT_LOG TO ROLE FINTECH_TRANSFORMER;

GRANT USAGE ON SCHEMA FINTECH_PROD.GOVERNANCE TO ROLE FINTECH_TRANSFORMER;

GRANT SELECT, INSERT ON TABLE FINTECH_PROD.GOVERNANCE.LANDING_ACCOUNT_ACTIVITY TO ROLE FINTECH_TRANSFORMER;

GRANT SELECT, INSERT ON TABLE FINTECH_PROD.RAW.ACCOUNT_ACTIVITY TO ROLE FINTECH_TRANSFORMER;

GRANT USAGE ON STAGE FINTECH_PROD.RAW.ACCOUNT_ACTIVITY_STAGE TO ROLE FINTECH_TRANSFORMER;

GRANT USAGE ON WAREHOUSE INGEST_WH TO ROLE FINTECH_TRANSFORMER;