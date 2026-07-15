/* ================================================================
   SECTION 6 — SETTLEMENTS (batch pattern, CSV)
   ================================================================ */
CREATE STAGE IF NOT EXISTS FINTECH_PROD.RAW.SETTLEMENTS_STAGE
    URL = 's3://fintech-project-sriram2026/settlements/'
    STORAGE_INTEGRATION = FINTECH_S3_INT
    FILE_FORMAT = FINTECH_PROD.RAW.CSV_FORMAT;
 
LIST @FINTECH_PROD.RAW.SETTLEMENTS_STAGE;   -- confirm files are visible before continuing
CREATE TABLE IF NOT EXISTS FINTECH_PROD.RAW.SETTLEMENTS (
    settlement_id           STRING,
    transaction_id          STRING,
    account_id              STRING,
    merchant_id             STRING,
    transaction_timestamp   TIMESTAMP_NTZ,
    settlement_date         DATE,
    amount                  NUMBER(12,2),
    currency                STRING,
    settlement_status       STRING,
    fee_amount              NUMBER(10,2),
    _loaded_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file            STRING
)
-- COPYING the data from Stage to Snowflake Table
-- ON_ERROR = 'CONTINUE' tells Snowflake: "If you hit an error while parsing a row or a file, do not stop. Skip the bad rows, load all the valid data you can find, and keep moving
COPY INTO FINTECH_PROD.RAW.SETTLEMENTS
    (settlement_id, transaction_id, account_id, merchant_id,
     transaction_timestamp, settlement_date, amount, currency,
     settlement_status, fee_amount)
FROM @FINTECH_PROD.RAW.SETTLEMENTS_STAGE
FILE_FORMAT = (FORMAT_NAME = FINTECH_PROD.RAW.CSV_FORMAT)
PATTERN = '.*\\.csv'
ON_ERROR = 'CONTINUE';

SELECT * FROM FINTECH_PROD.RAW.SETTLEMENTS;
SELECT COUNT(*), SUM(amount) FROM FINTECH_PROD.RAW.SETTLEMENTS;