/* ================================================================
   SECTION 11 — Stream & Task
   ================================================================ */
-- Phase 1: Set Up Storage, Landing Table, and Ingest Initial Data

USE ROLE FINTECH_ADMIN;
USE WAREHOUSE INGEST_WH;
USE DATABASE FINTECH_PROD;
USE SCHEMA RAW;

-- 1. Stage pointing at the new prefix
CREATE STAGE IF NOT EXISTS FINTECH_PROD.RAW.ACCOUNT_ACTIVITY_STAGE
    URL = 's3://fintech-project-sriram2026/account_activity/'
    STORAGE_INTEGRATION = FINTECH_S3_INT
    FILE_FORMAT = FINTECH_PROD.RAW.NDJSON_FORMAT;

-- confirm Snowflake can see the file(s) before loading
LIST @FINTECH_PROD.RAW.ACCOUNT_ACTIVITY_STAGE;

-- 2. RAW table
CREATE TABLE IF NOT EXISTS FINTECH_PROD.RAW.ACCOUNT_ACTIVITY (
    raw_payload   VARIANT,
    _loaded_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file     STRING
);

-- 3. Initial Load
COPY INTO FINTECH_PROD.RAW.ACCOUNT_ACTIVITY (raw_payload, _source_file)
FROM (SELECT $1, METADATA$FILENAME FROM @FINTECH_PROD.RAW.ACCOUNT_ACTIVITY_STAGE)
FILE_FORMAT = (FORMAT_NAME = FINTECH_PROD.RAW.NDJSON_FORMAT)
ON_ERROR = 'CONTINUE';

-- 4. Initial Ingestion Confirmation
SELECT COUNT(*), MAX(_loaded_at) FROM FINTECH_PROD.RAW.ACCOUNT_ACTIVITY;

SELECT * FROM FINTECH_PROD.RAW.ACCOUNT_ACTIVITY;

SELECT raw_payload:account_id::STRING, raw_payload:account_status::STRING,
       raw_payload:current_balance::NUMBER(12,2)
FROM FINTECH_PROD.RAW.ACCOUNT_ACTIVITY
LIMIT 10;

-- Phase 2: Create the Stream, Target Table, and Automated Merge Task

-- Run in Snowsight
USE ROLE FINTECH_ADMIN;
USE WAREHOUSE INGEST_WH;
USE DATABASE FINTECH_PROD;
USE SCHEMA RAW;

-- Step 1 — Create the Stream on your RAW table
CREATE STREAM IF NOT EXISTS FINTECH_PROD.RAW.ACCOUNT_ACTIVITY_STREAM 
    ON TABLE FINTECH_PROD.RAW.ACCOUNT_ACTIVITY;

-- Verify it is empty (Should return FALSE)
SELECT SYSTEM$STREAM_HAS_DATA('FINTECH_PROD.RAW.ACCOUNT_ACTIVITY_STREAM');

-- Step 2 — Create the final Core production destination table
CREATE SCHEMA IF NOT EXISTS FINTECH_PROD.CORE;

CREATE TABLE IF NOT EXISTS FINTECH_PROD.CORE.ACCOUNTS_CURRENT (
    account_id              STRING PRIMARY KEY,
    customer_id             STRING,
    account_status          STRING,
    current_balance         NUMBER(12,2),
    currency                STRING,
    updated_at              TIMESTAMP_NTZ,
    _merged_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Step 3 — Create and activate the automation Task
CREATE OR REPLACE TASK FINTECH_PROD.GOVERNANCE.MERGE_ACCOUNT_ACTIVITY_TASK
    WAREHOUSE = DBT_WH
    SCHEDULE = 'USING CRON 0 7 * * * UTC'
WHEN
    SYSTEM$STREAM_HAS_DATA('FINTECH_PROD.RAW.ACCOUNT_ACTIVITY_STREAM')
AS
MERGE INTO FINTECH_PROD.CORE.ACCOUNTS_CURRENT AS target
USING (
    SELECT
        raw_payload:account_id::STRING             AS account_id,
        raw_payload:customer_id::STRING            AS customer_id,
        raw_payload:account_status::STRING         AS account_status,
        raw_payload:current_balance::NUMBER(12,2)  AS current_balance,
        raw_payload:currency::STRING               AS currency,
        raw_payload:updated_at::TIMESTAMP_NTZ      AS updated_at
    FROM FINTECH_PROD.RAW.ACCOUNT_ACTIVITY_STREAM
    WHERE METADATA$ACTION = 'INSERT'
) AS source
ON target.account_id = source.account_id
WHEN MATCHED THEN UPDATE SET
    target.customer_id = source.customer_id,
    target.account_status = source.account_status,
    target.current_balance = source.current_balance,
    target.currency = source.currency,
    target.updated_at = source.updated_at,
    target._merged_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    account_id, customer_id, account_status, current_balance, currency, updated_at
) VALUES (
    source.account_id, source.customer_id, source.account_status,
    source.current_balance, source.currency, source.updated_at
);

-- CRITICAL: Task objects are created suspended. You must resume them!
ALTER TASK FINTECH_PROD.GOVERNANCE.MERGE_ACCOUNT_ACTIVITY_TASK RESUME;

-- Verify task configuration state
SHOW TASKS LIKE 'MERGE_ACCOUNT_ACTIVITY_TASK' IN SCHEMA FINTECH_PROD.GOVERNANCE;

-- Phase 3: Create the Pipe in Snowflake

USE ROLE FINTECH_ADMIN;
USE DATABASE FINTECH_PROD;
USE SCHEMA RAW;

CREATE OR REPLACE PIPE FINTECH_PROD.RAW.ACCOUNT_ACTIVITY_PIPE
    AUTO_INGEST = TRUE
AS
COPY INTO FINTECH_PROD.RAW.ACCOUNT_ACTIVITY (raw_payload, _source_file)
FROM (SELECT $1, METADATA$FILENAME FROM @FINTECH_PROD.RAW.ACCOUNT_ACTIVITY_STAGE)
FILE_FORMAT = (FORMAT_NAME = FINTECH_PROD.RAW.NDJSON_FORMAT)
ON_ERROR = 'CONTINUE';

SHOW PIPES LIKE 'ACCOUNT_ACTIVITY_PIPE' IN SCHEMA FINTECH_PROD.RAW;

-- Confirm Snowpipe loaded the file:
SELECT COUNT(*), _source_file 
FROM FINTECH_PROD.RAW.ACCOUNT_ACTIVITY 
GROUP BY _source_file 
ORDER BY MAX(_loaded_at) DESC 
LIMIT 5;

-- Confirm the Stream automatically captured the new updates:
SELECT SYSTEM$STREAM_HAS_DATA('FINTECH_PROD.RAW.ACCOUNT_ACTIVITY_STREAM'); 
-- Expecting: TRUE

SELECT COUNT(*) FROM FINTECH_PROD.RAW.ACCOUNT_ACTIVITY_STREAM;
-- Expecting: 100 (matching the rows you generated)

-- Normally in prudtcion setting we dont manually run tasks but here in order to check if the captured data in stream is merging into ffinal table we will be manully triggering the task

-- Step 1: Force the Task to Run Immediately

-- 1. Switch to Account Admin to grant the global privilege
USE ROLE ACCOUNTADMIN;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE FINTECH_ADMIN;

-- 2. Switch back to your Fintech Admin role
USE ROLE FINTECH_ADMIN;

-- 3. Now force the task to execute again
EXECUTE TASK FINTECH_PROD.GOVERNANCE.MERGE_ACCOUNT_ACTIVITY_TASK;

-- Step 2: Verify the Execution & Final Data

SELECT * 
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'MERGE_ACCOUNT_ACTIVITY_TASK',
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP())
)) 
ORDER BY SCHEDULED_TIME DESC;

-- Eventbridge has been scheduled to run for every 3 days at 6AM UTC 

SELECT account_id, customer_id, account_status, current_balance, _merged_at
FROM FINTECH_PROD.CORE.ACCOUNTS_CURRENT
ORDER BY _merged_at ASC;

SELECT COUNT(DISTINCT account_id) as total_accounts FROM FINTECH_PROD.CORE.ACCOUNTS_CURRENT; 



-- Did the pipe actually catch anything?
SELECT SYSTEM$PIPE_STATUS('FINTECH_PROD.RAW.ACCOUNT_ACTIVITY_PIPE');

-- Did the task run successfully?
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'MERGE_ACCOUNT_ACTIVITY_TASK',
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP())
)) ORDER BY SCHEDULED_TIME DESC LIMIT 5;

-- Did the merge actually populate CORE.ACCOUNTS_CURRENT?
SELECT COUNT(DISTINCT account_id) FROM FINTECH_PROD.CORE.ACCOUNTS_CURRENT;

-- Is the stream now empty (proof the task consumed it)?
SELECT SYSTEM$STREAM_HAS_DATA('FINTECH_PROD.RAW.ACCOUNT_ACTIVITY_STREAM');







