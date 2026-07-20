-- Lets us search policy documents by meaning, not just exact words
CREATE OR REPLACE CORTEX SEARCH SERVICE POLICY_DOCS_SEARCH

  -- the text column to search
  ON EXTRACTED_TEXT

  -- extra columns to return and filter by (not searched themselves)
  ATTRIBUTES FILE_NAME, PAGE_COUNT

  -- warehouse used to build/refresh the search index
  WAREHOUSE = 'CORTEX_WH'

  -- how often to refresh the index from the source table
  TARGET_LAG = '1 day'

  -- only re-index new/changed rows, not everything every time
  REFRESH_MODE = INCREMENTAL

AS (
  -- one row here = one searchable document
  SELECT
    file_name,
    extracted_text,   -- the actual text being searched
    page_count
  FROM FINTECH_PROD.RAW.POLICY_DOCUMENTS
);
