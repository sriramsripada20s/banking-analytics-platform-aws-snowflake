/* ================================================================
   SECTION 10 — EXCHANGE RATES (scheduled API pattern)
   ================================================================ */
CREATE STAGE IF NOT EXISTS FINTECH_PROD.RAW.EXCHANGE_RATE_STAGE
    URL = 's3://fintech-project-sriram2026/external_api/exchange_rates/'
    STORAGE_INTEGRATION = FINTECH_S3_INT
    FILE_FORMAT = FINTECH_PROD.RAW.NDJSON_FORMAT;
 
CREATE TABLE IF NOT EXISTS FINTECH_PROD.RAW.EXTERNAL_EXCHANGE_RATES (
    raw_payload VARIANT, _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), _source_file STRING
);
 
COPY INTO FINTECH_PROD.RAW.EXTERNAL_EXCHANGE_RATES (raw_payload, _source_file)
FROM (SELECT $1, METADATA$FILENAME FROM @FINTECH_PROD.RAW.EXCHANGE_RATE_STAGE)
FILE_FORMAT = (FORMAT_NAME = FINTECH_PROD.RAW.NDJSON_FORMAT) ON_ERROR = 'CONTINUE';
 
SELECT raw_payload:payload:rates FROM FINTECH_PROD.RAW.EXTERNAL_EXCHANGE_RATES LIMIT 5;

/* Step 1 — You are telling Snowflake: "I am giving you explicit permission to look out at the internet, but only to talk to this one specific website: api.frankfurter.dev." It blocks the database from communicating with any other server.

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE NETWORK RULE FINTECH_PROD.GOVERNANCE.FRANKFURTER_NETWORK_RULE
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = ('api.frankfurter.dev');
    
Step 2 — External Access Integration (no secret needed — this API has no auth)
t activates the connection and controls who can use it. By granting access to FINTECH_ADMIN, you are letting your development role pass through that firewall hole.

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION FRANKFURTER_ACCESS_INTEGRATION
    ALLOWED_NETWORK_RULES = (FINTECH_PROD.GOVERNANCE.FRANKFURTER_NETWORK_RULE)
    ENABLED = TRUE;


Step 3 — Table + stored procedure that calls the API and inserts directly
write a miniature Python script that lives directly inside Snowflake. It uses the popular requests library to jump over to the Frankfurter API, grab the latest live currency exchange rates for the USD, and immediately run an INSERT statement to drop that fresh data straight into your table.

USE ROLE FINTECH_ADMIN;
USE DATABASE FINTECH_PROD;
USE SCHEMA RAW;

CREATE TABLE IF NOT EXISTS EXTERNAL_EXCHANGE_RATES_DIRECT (
    raw_payload   VARIANT,
    _loaded_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE PROCEDURE FETCH_EXCHANGE_RATES()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.11
HANDLER = 'main'
EXTERNAL_ACCESS_INTEGRATIONS = (FRANKFURTER_ACCESS_INTEGRATION)
PACKAGES = ('snowflake-snowpark-python', 'requests')
AS
$$
import requests

def main(session):
    resp = requests.get("https://api.frankfurter.dev/v1/latest?base=USD", timeout=10)
    resp.raise_for_status()
    payload = resp.text

    session.sql(
        "INSERT INTO EXTERNAL_EXCHANGE_RATES_DIRECT (raw_payload) SELECT PARSE_JSON(?)",
        params=[payload]
    ).collect()

    return f"Inserted rates: {payload[:100]}"
$$;

Step 4 — Run it
run CALL FETCH_EXCHANGE_RATES();. Snowflake spins up a tiny background container, executes your Python script, hits the live API endpoint over the web, grabs the data, and writes it directly to your table—all in a couple of seconds without using any external infrastructure.

CALL FETCH_EXCHANGE_RATES();

SELECT * FROM EXTERNAL_EXCHANGE_RATES_DIRECT ORDER BY _loaded_at DESC LIMIT 5;
*/
