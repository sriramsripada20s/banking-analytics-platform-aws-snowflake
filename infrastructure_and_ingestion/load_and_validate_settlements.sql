-- ============================================================================
-- STEP 1: Create a persistent landing table (holding dock) for incoming CSV data.
-- We use a persistent table (not TEMPORARY) so Snowflake remembers previously
-- loaded files across runs, preventing the duplicate-file reload bug.
-- ============================================================================
CREATE TABLE IF NOT EXISTS FINTECH_PROD.GOVERNANCE.LANDING_SETTLEMENTS (
    settlement_id           STRING,
    transaction_id          STRING,
    account_id              STRING,
    merchant_id             STRING,
    transaction_timestamp   TIMESTAMP_NTZ,
    settlement_date         DATE,
    amount                  NUMBER(12,2),
    currency                STRING,
    settlement_status       STRING,
    fee_amount              NUMBER(12,2),
    _source_file            STRING
);

-- ============================================================================
-- STEP 2: Define the procedure to load, validate, audit, and promote data.
-- ============================================================================
-- ============================================================================
-- Procedure to load, validate, and audit CSV settlement data from S3 stage into RAW.
-- Uses Snowflake's INFER_SCHEMA to dynamically detect CSV headers and compare
-- them against our central SCHEMA_SNAPSHOTS table to catch schema drift.
-- ============================================================================
CREATE OR REPLACE PROCEDURE FINTECH_PROD.GOVERNANCE.LOAD_AND_VALIDATE_SETTLEMENTS()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    total_rows_loaded INT DEFAULT 0;
    duplicate_rows INT DEFAULT 0;
    null_rows INT DEFAULT 0;
    missing_key_count INT DEFAULT 0;
    err_msg STRING;
BEGIN
    -- ------------------------------------------------------------------------
    -- 1. PRE-INGESTION SCHEMA DRIFT CHECK (Runs BEFORE loading data into tables)
    -- Compare CSV column headers against our last known good baseline.
    -- ------------------------------------------------------------------------
    
    -- Fetch expected column names from our governance baseline table
    LET expected_keys ARRAY := (
        SELECT COALESCE(ARRAY_AGG(column_key), ARRAY_CONSTRUCT())
        FROM FINTECH_PROD.GOVERNANCE.SCHEMA_SNAPSHOTS
        WHERE source_name = 'settlements'
    );

    -- Automatically infer the column names directly from the CSV file(s) sitting on stage
    LET actual_keys ARRAY := (
        SELECT ARRAY_AGG(COLUMN_NAME)
        FROM TABLE(INFER_SCHEMA(
            LOCATION => '@FINTECH_PROD.RAW.SETTLEMENTS_STAGE',
            FILE_FORMAT => 'FINTECH_PROD.RAW.CSV_FORMAT'
        ))
    );

    -- Find any columns that existed in previous runs but are missing in today's file
    LET missing_keys ARRAY := ARRAY_EXCEPT(:expected_keys, :actual_keys);
    missing_key_count := ARRAY_SIZE(:missing_keys);

    -- Fail fast if columns vanished upstream before doing any table loading
    IF (missing_key_count > 0) THEN
        err_msg := 'Schema drift: missing/changed columns ' || ARRAY_TO_STRING(:missing_keys, ', ');

        -- Log failure to audit table
        INSERT INTO FINTECH_PROD.GOVERNANCE.INGESTION_AUDIT_LOG (
            source_name, status, load_timestamp, rows_loaded, duplicate_rows, null_rows, error_message
        )
        VALUES ('settlements', 'Failed', CURRENT_TIMESTAMP(), 0, 0, 0, :err_msg);

        -- Raise SQL exception to stop execution and trigger Airflow alert
        EXECUTE IMMEDIATE
            'DECLARE validation_failed EXCEPTION (-20001, ''' ||
            REPLACE(:err_msg, '''', '''''') ||
            '''); BEGIN RAISE validation_failed; END;';
    END IF;

    -- ------------------------------------------------------------------------
    -- 2. INGESTION: Copy CSV records into persistent landing table.
    -- Snowflake tracks file metadata to automatically skip previously loaded files.
    -- ------------------------------------------------------------------------
    COPY INTO FINTECH_PROD.GOVERNANCE.LANDING_SETTLEMENTS (
        settlement_id, transaction_id, account_id, merchant_id, transaction_timestamp,
        settlement_date, amount, currency, settlement_status, fee_amount, _source_file
    )
    FROM (
        SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, METADATA$FILENAME
        FROM @FINTECH_PROD.RAW.SETTLEMENTS_STAGE
    )
    FILE_FORMAT = (FORMAT_NAME = FINTECH_PROD.RAW.CSV_FORMAT)
    PATTERN = '.*\\.csv'
    ON_ERROR = 'ABORT_STATEMENT';

    -- ------------------------------------------------------------------------
    -- 3. FILE TRACKING: Identify which files were newly loaded in this run.
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
            new_files := ARRAY_CONSTRUCT(); -- Set to empty array if no files were loaded
    END;

    -- Exit early if no new files arrived in this run
    IF (ARRAY_SIZE(:new_files) = 0) THEN
        RETURN 'No new files to load -- all files already loaded previously.';
    END IF;

    -- ------------------------------------------------------------------------
    -- 4. DATA QUALITY CHECKS (Run ONLY on newly ingested records)
    -- ------------------------------------------------------------------------

    -- Check A: Total row count for the newly loaded file(s)
    total_rows_loaded := (
        SELECT COUNT(*) FROM FINTECH_PROD.GOVERNANCE.LANDING_SETTLEMENTS s
        WHERE EXISTS (SELECT 1 FROM TABLE(FLATTEN(:new_files)) f WHERE ENDSWITH(f.value::STRING, s._source_file))
    );

    -- Check B: Ensure core primary keys/IDs are not NULL
    null_rows := (
        SELECT COUNT(*) FROM FINTECH_PROD.GOVERNANCE.LANDING_SETTLEMENTS s
        WHERE EXISTS (SELECT 1 FROM TABLE(FLATTEN(:new_files)) f WHERE ENDSWITH(f.value::STRING, s._source_file))
        AND (settlement_id IS NULL OR account_id IS NULL OR merchant_id IS NULL)
    );

    -- Check C: Ensure there are no duplicate settlement_ids within this batch
    duplicate_rows := (
        SELECT COUNT(*)
        FROM (
            SELECT settlement_id, ROW_NUMBER() OVER (PARTITION BY settlement_id ORDER BY settlement_id) AS rn
            FROM FINTECH_PROD.GOVERNANCE.LANDING_SETTLEMENTS s
            WHERE EXISTS (SELECT 1 FROM TABLE(FLATTEN(:new_files)) f WHERE ENDSWITH(f.value::STRING, s._source_file))
        )
        WHERE rn > 1
    );

    -- ------------------------------------------------------------------------
    -- 5. ERROR EVALUATION: Format error messages if validation failed
    -- ------------------------------------------------------------------------
    IF (null_rows > 0) THEN
        err_msg := null_rows || ' rows with missing settlement_id/account_id/merchant_id.';
    ELSEIF (duplicate_rows > 0) THEN
        err_msg := duplicate_rows || ' duplicate settlement_id(s) found.';
    ELSEIF (total_rows_loaded = 0) THEN
        err_msg := 'No rows found for newly loaded file(s).';
    ELSE
        err_msg := NULL; -- All checks passed!
    END IF;

    -- ------------------------------------------------------------------------
    -- 6. AUDIT LOGGING: Log execution status to audit table
    -- ------------------------------------------------------------------------
    INSERT INTO FINTECH_PROD.GOVERNANCE.INGESTION_AUDIT_LOG (
        source_name, status, load_timestamp, rows_loaded, duplicate_rows, null_rows, error_message
    )
    VALUES (
        'settlements', IFF(:err_msg IS NULL, 'Success', 'Failed'),
        CURRENT_TIMESTAMP(), :total_rows_loaded, :duplicate_rows, :null_rows, :err_msg
    );

    -- ------------------------------------------------------------------------
    -- 7. GUARDRAIL: If validation failed, throw an exception to crash task.
    -- Prevents bad data promotion and notifies Airflow.
    -- ------------------------------------------------------------------------
    IF (:err_msg IS NOT NULL) THEN
        EXECUTE IMMEDIATE
            'DECLARE validation_failed EXCEPTION (-20001, ''' ||
            REPLACE(:err_msg, '''', '''''') ||
            '''); BEGIN RAISE validation_failed; END;';
    END IF;

    -- ------------------------------------------------------------------------
    -- 8. PROMOTION TO RAW: All checks passed! Copy clean rows to RAW target table.
    -- ------------------------------------------------------------------------
    INSERT INTO FINTECH_PROD.RAW.SETTLEMENTS (
        settlement_id, transaction_id, account_id, merchant_id, transaction_timestamp,
        settlement_date, amount, currency, settlement_status, fee_amount
    )
    SELECT settlement_id, transaction_id, account_id, merchant_id, transaction_timestamp,
           settlement_date, amount, currency, settlement_status, fee_amount
    FROM FINTECH_PROD.GOVERNANCE.LANDING_SETTLEMENTS s
    WHERE EXISTS (SELECT 1 FROM TABLE(FLATTEN(:new_files)) f WHERE ENDSWITH(f.value::STRING, s._source_file));

    -- ------------------------------------------------------------------------
    -- 9. BASELINE UPDATE: Save today's column schema into baseline snapshot table.
    -- This becomes the expected schema baseline for tomorrow's run.
    -- ------------------------------------------------------------------------
    MERGE INTO FINTECH_PROD.GOVERNANCE.SCHEMA_SNAPSHOTS AS target
    USING (
        SELECT DISTINCT f.value::STRING AS column_key 
        FROM TABLE(FLATTEN(input => :actual_keys)) f
    ) AS source
    ON target.source_name = 'settlements' AND target.column_key = source.column_key
    WHEN MATCHED THEN 
        UPDATE SET last_seen_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN 
        INSERT (source_name, column_key) VALUES ('settlements', source.column_key);

    RETURN 'Success: loaded ' || total_rows_loaded || ' new row(s).';
END;
$$;

-- Grant execution privileges to transformation role
GRANT USAGE ON PROCEDURE FINTECH_PROD.GOVERNANCE.LOAD_AND_VALIDATE_SETTLEMENTS() TO ROLE FINTECH_TRANSFORMER;

-- Execute the procedure
CALL FINTECH_PROD.GOVERNANCE.LOAD_AND_VALIDATE_SETTLEMENTS();