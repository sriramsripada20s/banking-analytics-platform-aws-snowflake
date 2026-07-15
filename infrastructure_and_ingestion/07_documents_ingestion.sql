/* ================================================================
   SECTION 9 — DOCUMENTS: support cases (NDJSON) + policies (PDF)
   ================================================================ */
CREATE STAGE IF NOT EXISTS FINTECH_PROD.RAW.SUPPORT_CASES_STAGE
    URL = 's3://fintech-project-sriram2026/documents/support_cases/'
    STORAGE_INTEGRATION = FINTECH_S3_INT
    FILE_FORMAT = FINTECH_PROD.RAW.NDJSON_FORMAT;
 
CREATE TABLE IF NOT EXISTS FINTECH_PROD.RAW.SUPPORT_CASES (
    raw_payload VARIANT, _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), _source_file STRING
);
 
COPY INTO FINTECH_PROD.RAW.SUPPORT_CASES (raw_payload, _source_file)
FROM (SELECT $1, METADATA$FILENAME FROM @FINTECH_PROD.RAW.SUPPORT_CASES_STAGE)
FILE_FORMAT = (FORMAT_NAME = FINTECH_PROD.RAW.NDJSON_FORMAT) ON_ERROR = 'CONTINUE';
 
SELECT raw_payload:case_id::STRING, raw_payload:category::STRING, raw_payload:narrative::STRING
FROM FINTECH_PROD.RAW.SUPPORT_CASES LIMIT 10;
 
-- Policy documents: PDFs, no COPY INTO needed yet — Cortex Search
-- (built later) reads directly from this stage
CREATE STAGE IF NOT EXISTS FINTECH_PROD.RAW.POLICY_DOCS_STAGE
    URL = 's3://fintech-project-sriram2026/documents/policies/'
    STORAGE_INTEGRATION = FINTECH_S3_INT;
 
LIST @FINTECH_PROD.RAW.POLICY_DOCS_STAGE;   -- confirm the 5 PDFs are visible

/* In simple terms, we are using a directory table here because Snowflake's AI cannot read a raw PDF file directly.

A standard Snowflake table can only hold text and numbers. A PDF sitting in an S3 bucket is a physical file (unstructured data).

The directory table acts as a bridge. It looks into your S3 bucket and builds a clean SQL list of your files (showing paths like medical_policy_2026.pdf).

Your script then reads that list, grabs each file path one by one, and feeds it into the AI_PARSE_DOCUMENT function so the AI knows exactly which file to look at, read, and extract text from.
*/

-- Step 1 — Enable the directory table on the stage
USE ROLE FINTECH_ADMIN;
USE WAREHOUSE CORTEX_WH;
USE DATABASE FINTECH_PROD;
USE SCHEMA RAW;

ALTER STAGE POLICY_DOCS_STAGE SET DIRECTORY = (ENABLE = TRUE);
--REFRESH tells Snowflake to go look at S3 right now and populate the directory listing — without it, the directory table would be empty even though the files are sitting in the bucket.
ALTER STAGE POLICY_DOCS_STAGE REFRESH;

-- Step 2 — Confirm the directory table actually sees your files
SELECT RELATIVE_PATH, SIZE, LAST_MODIFIED
FROM DIRECTORY(@POLICY_DOCS_STAGE);

-- Step 3 — Clear out the hardcoded version and reload properly
-- Since you already loaded 5 rows via the hardcoded approach, clear it so you don't end up with duplicates:

CREATE TABLE IF NOT EXISTS POLICY_DOCUMENTS (
    file_name       STRING,
    extracted_text  STRING,
    page_count      INT,
    _loaded_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- It loops through the raw files in your S3 stage using the directory table to find their file paths.
-- It passes each file to Cortex AI, extracts the text and page counts via OCR, and saves those details into your table
INSERT INTO POLICY_DOCUMENTS (file_name, extracted_text, page_count)
SELECT
    RELATIVE_PATH,
    parsed:content::STRING,
    parsed:metadata:pageCount::INT
FROM DIRECTORY(@POLICY_DOCS_STAGE),
LATERAL (
    SELECT SNOWFLAKE.CORTEX.AI_PARSE_DOCUMENT(
        TO_FILE('@POLICY_DOCS_STAGE', RELATIVE_PATH), {'mode': 'OCR'}
    ) AS parsed
);

--displays each processed file's name, its page length, and a clean 200-character snippet of the AI's extracted text so you can quickly inspect and confirm the data loaded successfully.
SELECT file_name, page_count, LEFT(extracted_text, 200) AS preview
FROM POLICY_DOCUMENTS;




