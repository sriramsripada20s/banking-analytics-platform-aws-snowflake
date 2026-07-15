/* ================================================================
   SECTION 4 — STORAGE INTEGRATION (fresh, pointing at your new role)
   ================================================================ */
DROP STORAGE INTEGRATION IF EXISTS FINTECH_S3_INT;
 
-- >>> enter NEW IAM role's ARN here <<<
CREATE STORAGE INTEGRATION FINTECH_S3_INT
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<AWS ACCOUNT_ID>:role/snowflake_fintech_role_new'
    STORAGE_ALLOWED_LOCATIONS = ('s3://fintech-project-sriram2026/');

DESC STORAGE INTEGRATION FINTECH_S3_INT;
-- ^^^ Copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID from this output

GRANT USAGE ON INTEGRATION FINTECH_S3_INT TO ROLE FINTECH_LOADER;
GRANT USAGE ON INTEGRATION FINTECH_S3_INT TO ROLE FINTECH_ADMIN;

USE ROLE FINTECH_ADMIN;
USE WAREHOUSE INGEST_WH;
USE DATABASE FINTECH_PROD;
USE SCHEMA RAW;

/* ================================================================
   SECTION 5 — FILE FORMATS 
   Because CSV and JSON are structured completely differently, you must create a separate named File Format object for each.
   ================================================================ */
CREATE FILE FORMAT IF NOT EXISTS FINTECH_PROD.RAW.CSV_FORMAT
    TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"';
 
CREATE FILE FORMAT IF NOT EXISTS FINTECH_PROD.RAW.NDJSON_FORMAT
    TYPE = JSON STRIP_OUTER_ARRAY = FALSE;


