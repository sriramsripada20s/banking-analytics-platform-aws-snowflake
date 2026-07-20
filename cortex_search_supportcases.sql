-- Lets us search support case narratives by meaning, not just exact words
CREATE OR REPLACE CORTEX SEARCH SERVICE SUPPORT_CASES_SEARCH

  -- the text column to search
  ON NARRATIVE

  -- extra columns to return and filter by (not searched themselves)
  ATTRIBUTES CASE_ID, CUSTOMER_ID, CATEGORY, PRIORITY, STATUS

  -- warehouse used to build/refresh the search index
  WAREHOUSE = 'CORTEX_WH'

  -- how often to refresh the index -- shorter than policy docs since
  -- new support cases come in much more frequently
  TARGET_LAG = '1 hour'

  -- only re-index new/changed rows, not everything every time
  REFRESH_MODE = INCREMENTAL

AS (
  -- one row here = one searchable case
  SELECT
    case_id,
    customer_id,
    category,
    priority,
    status,
    narrative   -- the actual text being searched
  FROM FINTECH_PROD.STAGING.STG_SUPPORT_CASES
);
