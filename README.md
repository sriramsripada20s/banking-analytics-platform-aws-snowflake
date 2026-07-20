# banking-analytics-platform-aws-snowflake-dbt-airflow
End-to-end fintech data platform on AWS + Snowflake — multi-pattern ingestion (batch, event-driven, API), dbt transformations, and Cortex AI-powered analytics, built to demonstrate production data engineering patterns from raw data to a governed semantic layer....



## Ingestion Layer — FINTECH_PROD

### Storage Integration

| Property | Value |
|---|---|
| Name | `FINTECH_S3_INT` |
| Type | External Stage (S3) |
| Provider | AWS S3 |
| IAM Role | `arn:aws:iam::<AWS_ACCOUNT_ID>:role/snowflake_fintech_role_new` |
| Allowed Locations | `s3://fintech-project-sriram2026/` |
| File Formats | `CSV_FORMAT` (header-skip CSV), `NDJSON_FORMAT` (newline-delimited JSON) |

### Ingestion by Data Source

| Source | RAW Table | Mechanism | Why This Pattern | Stage | File Format | Rows | Last Loaded |
|---|---|---|---|---|---|---|---|
| Settlements | `RAW.SETTLEMENTS` | Batch `COPY INTO` | Periodic CSV dumps from payment processor; no real-time need | `RAW.SETTLEMENTS_STAGE` (`/settlements/`) | `CSV_FORMAT` | 200,000 | 2026-07-16 09:05 |
| Partner Transactions | `RAW.PARTNER_TRANSACTIONS` | Snowpipe (`AUTO_INGEST`) | Partner pushes NDJSON files continuously; event-driven keeps latency low | `RAW.PARTNER_TXN_STAGE` (`/partner_transactions/`) | `NDJSON_FORMAT` | 200,000 | 2026-07-16 09:11 |
| Customers | `RAW.CUSTOMERS_API_RAW` | Batch `COPY INTO` | API snapshots exported as NDJSON; loaded on-demand | `RAW.CUSTOMERS_API_STAGE` (`/source_api/customers/`) | `NDJSON_FORMAT` | 1 (file) | 2026-07-14 13:14 |
| Accounts | `RAW.ACCOUNTS_API_RAW` | Batch `COPY INTO` | Same API snapshot pattern as customers | `RAW.ACCOUNTS_API_STAGE` (`/source_api/accounts/`) | `NDJSON_FORMAT` | 1 (file) | 2026-07-14 13:14 |
| Merchants | `RAW.MERCHANTS_API_RAW` | Batch `COPY INTO` | Same API snapshot pattern as customers | `RAW.MERCHANTS_API_STAGE` (`/source_api/merchants/`) | `NDJSON_FORMAT` | 1 (file) | 2026-07-14 13:14 |
| Support Cases | `RAW.SUPPORT_CASES` | Batch `COPY INTO` | Historical case narratives as NDJSON; bulk-loaded once, updated periodically | `RAW.SUPPORT_CASES_STAGE` (`/documents/support_cases/`) | `NDJSON_FORMAT` | 200,000 | 2026-07-16 09:05 |
| Policy Documents | `RAW.POLICY_DOCUMENTS` | `AI_PARSE_DOCUMENT` (OCR) | Unstructured PDFs require AI extraction; no tabular format exists | `RAW.POLICY_DOCS_STAGE` (`/documents/policies/`) + directory table | N/A (binary PDF) | 5 | 2026-07-14 14:03 |
| Exchange Rates | `RAW.EXTERNAL_EXCHANGE_RATES` | Batch `COPY INTO` + live API procedure | S3 file for backfill; Python stored proc (`FETCH_EXCHANGE_RATES`) for live calls via External Access Integration | `RAW.EXCHANGE_RATE_STAGE` (`/external_api/exchange_rates/`) | `NDJSON_FORMAT` | 1 (file) | 2026-07-14 14:11 |
| Account Activity | `RAW.ACCOUNT_ACTIVITY` | Snowpipe (`AUTO_INGEST`) → Stream → Task | Continuous balance/status updates need real-time capture and SCD-1 merge into CORE | `RAW.ACCOUNT_ACTIVITY_STAGE` (`/account_activity/`) | `NDJSON_FORMAT` | 2,100 | 2026-07-19 03:00 |

### Stream + Task Pipeline (Account Activity CDC)

| Component | Detail |
|---|---|
| Stream | `RAW.ACCOUNT_ACTIVITY_STREAM` on `RAW.ACCOUNT_ACTIVITY` |
| Stream Type | DELTA (default — captures INSERT, UPDATE, DELETE) |
| What it tracks | All new rows inserted into `RAW.ACCOUNT_ACTIVITY` by Snowpipe |
| Target table | `CORE.ACCOUNTS_CURRENT` (98 rows, last merged 2026-07-14 16:29) |
| Task | `GOVERNANCE.MERGE_ACCOUNT_ACTIVITY_TASK` |
| Schedule | `CRON 0 7 * * * UTC` (daily at 7:00 AM UTC) |
| Trigger condition | `SYSTEM$STREAM_HAS_DATA(...)` — skips execution if no new data |
| Operation | `MERGE` (upsert) keyed on `account_id` — updates existing accounts, inserts new ones |
| Warehouse | `DBT_WH` |
| State | STARTED (active) |

### Pipes

| Pipe | Schema | Target Table | Auto-Ingest | State |
|---|---|---|---|---|
| `PARTNER_TXN_PIPE` | `RAW` | `RAW.PARTNER_TRANSACTIONS` | Yes | Active |
| `ACCOUNT_ACTIVITY_PIPE` | `RAW` | `RAW.ACCOUNT_ACTIVITY` | Yes | Active |

Both pipes use a Snowflake-managed SQS queue, `NDJSON_FORMAT`, and `ON_ERROR = 'CONTINUE'`.

### Data Flow

```
S3 Bucket (fintech-project-sriram2026)
│
├── /settlements/                  → Batch COPY → RAW.SETTLEMENTS
├── /partner_transactions/         → S3 Event → SQS → Snowpipe → RAW.PARTNER_TRANSACTIONS
├── /source_api/customers/         → Batch COPY → RAW.CUSTOMERS_API_RAW
├── /source_api/accounts/          → Batch COPY → RAW.ACCOUNTS_API_RAW
├── /source_api/merchants/         → Batch COPY → RAW.MERCHANTS_API_RAW
├── /documents/support_cases/      → Batch COPY → RAW.SUPPORT_CASES
├── /documents/policies/           → AI_PARSE_DOCUMENT (OCR) → RAW.POLICY_DOCUMENTS
├── /external_api/exchange_rates/  → Batch COPY → RAW.EXTERNAL_EXCHANGE_RATES
└── /account_activity/             → S3 Event → SQS → Snowpipe → RAW.ACCOUNT_ACTIVITY
                                                                        │
                                                              Stream (CDC capture)
                                                                        │
                                                              Task (daily MERGE)
                                                                        ▼
                                                          CORE.ACCOUNTS_CURRENT
```


## Transformation Layer — FINTECH_PROD (dbt)

**Total tests: 92 | All passing** 

### Schema Routing Macro

```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```

**Why it's needed:** dbt prepends the target schema as a prefix by default (e.g., `DEV_STAGING`). This override strips that prefix so models land in the exact schema specified in `dbt_project.yml` — `STAGING`, `INTERMEDIATE`, `MARTS` — regardless of the target profile.

### Staging Layer
*Materialized as views in `FINTECH_PROD.STAGING`.*

| Model | Source | Business Logic | Rows | Tests |
|---|---|---|---|---|
| `stg_customers` | `RAW.CUSTOMERS_API_RAW` | Flatten JSON, dedup by `customer_id` (`ROW_NUMBER()` on `updated_at DESC`) | ~200,000 | unique, not_null, accepted_values |
| `stg_accounts` | `RAW.ACCOUNTS_API_RAW` | Flatten JSON, dedup by `account_id` | ~287,118 | unique, not_null, accepted_values, relationships → `stg_customers` |
| `stg_accounts_current` | `CORE.ACCOUNTS_CURRENT` | Pass-through of live state (Stream+Task pipeline, external to dbt) | 98 | unique, not_null, accepted_values |
| `stg_merchants` | `RAW.MERCHANTS_API_RAW` | Flatten JSON, dedup by `merchant_id` | ~300 | unique, not_null, accepted_values |
| `stg_settlements` | `RAW.SETTLEMENTS` | Typed column pass-through (already-structured CSV) | ~200,000 | unique, not_null, accepted_values, relationships → `stg_accounts`, `stg_merchants` |
| `stg_partner_transactions` | `RAW.PARTNER_TRANSACTIONS` | Extract typed fields from VARIANT, no flattening needed | ~200,000 | unique, not_null, accepted_values, relationships → `stg_accounts`, `stg_merchants` |
| `stg_support_cases` | `RAW.SUPPORT_CASES` | Extract typed fields from VARIANT | ~200,000 | unique, not_null, accepted_values, relationships → `stg_customers` |
| `stg_exchange_rates` | `RAW.EXTERNAL_EXCHANGE_RATES` | Flatten nested rate object into one row per (base, quote, date) | variable | not_null |
| `stg_account_activity` | `RAW.ACCOUNT_ACTIVITY` | Extract typed fields from VARIANT | ~2,100 | not_null, accepted_values, relationships → `stg_accounts` |

### Intermediate Layer
*Materialized as tables in `FINTECH_PROD.INTERMEDIATE`.*

| Model | Sources | Business Logic | Rows | Tests |
|---|---|---|---|---|
| `int_transactions_unified` | `stg_settlements` + `stg_partner_transactions` | `UNION ALL` into one schema; derives `transaction_date`, `is_weekend`, `is_late_night`, `amount_bucket`, outcome flags | 400,000 | unique, not_null, accepted_values, relationships |
| `int_accounts_enriched` | `stg_accounts` + `stg_accounts_current` | `LEFT JOIN` accounts to live state; `COALESCE` falls back to creation-time snapshot when no activity exists | 287,118 | Row-count guarded by singular test (below) |
| `int_customer_activity_summary` | `stg_customers` + `int_accounts_enriched` + `int_transactions_unified` + `stg_support_cases` | `LEFT JOIN`s to account/transaction/support rollups; derives `engagement_segment` and `has_risk_flag` | 200,000 | unique, not_null, accepted_values |
| `int_merchant_risk_summary` | `stg_merchants` + `int_transactions_unified` | `LEFT JOIN` to transaction aggregates; derives `decline_rate_pct`, `is_high_decline_merchant` (≥20 txns AND >15% decline rate) | 300 | unique, not_null, accepted_values |

### Marts Layer
*Materialized as tables in `FINTECH_PROD.MARTS`.*

| Model | Source | Business Logic | Rows | Tests |
|---|---|---|---|---|
| `mart_customer_360` | `int_customer_activity_summary` | Pass-through — BI/Cortex Analyst interface for customer analytics | 200,000 | unique, not_null |
| `mart_daily_transaction_kpi` | `int_transactions_unified` | `GROUP BY transaction_date`, daily volume/decline aggregates | 2 *(expected — synthetic data spans only 2 distinct dates)* | unique, not_null |
| `mart_merchant_performance` | `int_merchant_risk_summary` | Pass-through — BI/Cortex Analyst interface for merchant risk | 300 | unique, not_null |

### Singular (Custom) Tests

| Test | Validates | Status |
|---|---|---|
| `assert_customer_summary_rowcount_matches_source` | `int_customer_activity_summary` row count = `stg_customers` row count — proves no customer dropped by `LEFT JOIN`s | PASS |
| `assert_merchant_summary_rowcount_matches_source` | `int_merchant_risk_summary` row count = `stg_merchants` row count | PASS |

### Source Freshness Monitoring

| Source | Warn After | Error After |
|---|---|---|
| `RAW.PARTNER_TRANSACTIONS` | 12 hours | 48 hours |
| `RAW.ACCOUNT_ACTIVITY` | 3 days | 7 days |

### Design Tradeoffs

| Topic | Decision |
|---|---|
| Deliberate `LEFT JOIN` pattern | Every intermediate model anchors on the entity table (customers/merchants/accounts), so zero-activity entities still appear — critical since these models exist to *identify* dormancy/inactivity, not hide it. Enforced by the singular tests above. |
| Dedup logic | Reference entities (`stg_customers`/`stg_accounts`/`stg_merchants`) dedup via `ROW_NUMBER()`; event tables (settlements, partner transactions) never dedup — every row is a distinct real event. |
| Label leakage avoided | `engagement_segment`/`has_risk_flag` are behavioral signals, not the ML label itself — the churn model consumes this table as features, never trains directly on a pre-baked label stored here. |
| `CORE.ACCOUNTS_CURRENT` sourced externally | Maintained by the Stream+Task pipeline outside dbt; consumed via `source()`, never `ref()`, honestly reflecting that dbt doesn't own this table's freshness. |
| High-decline threshold | Requires ≥20 transactions *and* >15% decline rate — prevents low-volume merchants from being falsely flagged on statistical noise. |

### Lineage: Raw Sources → Marts

| Mart | Ultimate Raw Sources |
|---|---|
| `MART_CUSTOMER_360` | Customers, Accounts, Accounts Current, Settlements, Partner Transactions, Support Cases |
| `MART_DAILY_TRANSACTION_KPI` | Settlements, Partner Transactions |
| `MART_MERCHANT_PERFORMANCE` | Merchants, Settlements, Partner Transactions |


# Airflow DAG
<img width="1346" height="905" alt="image" src="https://github.com/user-attachments/assets/b70d0d40-7f4f-4900-b14e-b30cef63a6d5" />

# Airflow DAG Success Scenario
<img width="1225" height="476" alt="image" src="https://github.com/user-attachments/assets/4a8f8722-0e9f-4afd-bd81-afce7940b8c7" />

# Airflow DAG Success Message on Slack
<img width="1507" height="602" alt="image" src="https://github.com/user-attachments/assets/652ececd-9b12-4a72-8e35-fa0d3d0aeb8b" />

# Airflow Pipeline Fail Scenario 
<img width="1272" height="703" alt="image" src="https://github.com/user-attachments/assets/6970b8d8-eca1-41ba-ba0a-64371bab71fa" />

# Airflow DAG Fail Message on Slack
<img width="1237" height="576" alt="image" src="https://github.com/user-attachments/assets/6cccd813-c238-4a12-80d3-68e2d5666cbf" />


# Cortex Analyst 
<img width="1772" height="857" alt="image" src="https://github.com/user-attachments/assets/9150274b-721e-4667-b1c5-71e5d3552ca3" />


## Cortex Agent — `FINTECH_ASSISTANT_AGENT`

An orchestrated AI agent combining structured (Cortex Analyst) and
unstructured (Cortex Search) data access into a single conversational
interface, built via Snowflake CoCo.

<img width="1102" height="471" alt="image" src="https://github.com/user-attachments/assets/728ed7cb-efc7-4a20-8b8f-a89a4e6a0110" />


### 1. Tools Wired to the Agent

| Tool | Type | Backed By | Warehouse |
|---|---|---|---|
| `fintech_analytics` | Cortex Analyst (text-to-SQL) | `FINTECH_ANALYTICS_VIEW` semantic view | `CORTEX_WH` |
| `policy_docs_search` | Cortex Search | `POLICY_DOCS_SEARCH` service | Service-managed |
| `support_cases_search` | Cortex Search | `SUPPORT_CASES_SEARCH` service, filterable by category/priority/status | Service-managed |

### 2. Agent Configuration

- **Audience**: Internal operations team
- **Tone**: Concise & technical — data-first, tables over prose, no filler
- **Routing logic**: Quantitative questions → Cortex Analyst; policy/compliance questions → policy search; incident/complaint history → support case search; multi-domain questions → calls tools in sequence and synthesizes a combined answer

### 3. Evaluation Dataset

- **Methodology**: Ground truth built by directly querying the underlying marts and document corpus — independent of the agent's own output, so the evaluation can actually catch agent mistakes rather than validate them
- **30 test questions**, covering:
  - 8 pure Cortex Analyst questions
  - 4 pure policy search questions
  - 4 pure support case search questions
  - 4 multi-tool questions (requiring correct orchestration across tools)
  - 5 core answer-correctness checks
  - 2 edge cases (out-of-scope handling, refusal behavior)
  - 3 instruction-compliance checks (tone/format adherence)
- **Metrics evaluated**: tool selection accuracy, tool execution accuracy, answer correctness, logical consistency

### 4. Status

Agent built, published (v2), and fully wired to all three tools.
Evaluation dataset and config built and deployed. **Evaluation
execution itself is currently blocked** — the underlying
`DATA_AGENT_RUN` API required to invoke the agent programmatically is
restricted on Snowflake trial accounts. All infrastructure is ready to
run immediately once the account is upgraded from trial.









