# Customer 360 & Cortex AI Analytics Platform (Snowflake, dbt, Airflow)
An enterprise-grade financial data platform that ingests batch and streaming financial workloads, enforces real-time governance and schema drift detection, transforms data using a dbt Medallion architecture, and surfaces natural-language analytics via Snowflake Cortex AI.

[![dbt Core](https://img.shields.io/badge/dbt--core-1.8%2B-FF694A?style=flat&logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![Snowflake](https://img.shields.io/badge/Snowflake-Enterprise-29B5E8?style=flat&logo=snowflake&logoColor=white)](https://www.snowflake.com/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8%2B-017CEE?style=flat&logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Astronomer Cosmos](https://img.shields.io/badge/Astronomer-Cosmos-1C2B41?style=flat&logo=astronomer&logoColor=white)](https://astronomer.github.io/astronomer-cosmos/)
[![AWS S3](https://img.shields.io/badge/AWS-S3-569A31?style=flat&logo=amazons3&logoColor=white)](https://aws.amazon.com/s3/)
[![AWS Lambda](https://img.shields.io/badge/AWS-Lambda-FF9900?style=flat&logo=awslambda&logoColor=white)](https://aws.amazon.com/lambda/)
[![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?style=flat&logo=python&logoColor=white)](https://www.python.org/)
[![XGBoost](https://img.shields.io/badge/Snowpark%20ML-XGBoost-2B5B84?style=flat&logo=xgboost&logoColor=white)](https://xgboost.readthedocs.io/)
[![Snowflake Cortex](https://img.shields.io/badge/Snowflake-Cortex%20AI-7B1FA2?style=flat&logo=snowflake&logoColor=white)](https://docs.snowflake.com/en/user-guide/cortex-overview)
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white)](https://www.docker.com/)
[![Astro CLI](https://img.shields.io/badge/Astro%20CLI-Astronomer-FF5252?style=flat&logo=astronomer&logoColor=white)](https://www.astronomer.io/astro-cli/)

## Business Problem
Fintech platforms process millions of financial events daily across disparate sources—including settlement feeds, partner transaction streams, account activity logs, and customer support tickets.

* **Fragmented Data & Schema Drift:** Inconsistent schemas and unexpected upstream contract changes frequently crash downstream jobs or corrupt reporting data.
* **Reactive Risk Management:** Customer churn and fraud risks are typically discovered after financial loss occurs, rather than proactively flagged.
* **Query Latency for Operations:** Business and risk teams rely heavily on data engineers to write custom SQL for daily operational questions and policy checks.

> **💡 Proposed Solution:** An automated financial data platform built using AWS, Snowflake, dbt, and Airflow that isolates schema drift at ingestion, models a unified Medallion layer for predictive ML churn scoring, and surfaces conversational analytics via Snowflake Cortex AI.

## 📌 Architecture Overview 
Data flows from batch and streaming sources through AWS staging into Snowflake, where it undergoes automated validation, dbt Medallion transformations, Snowpark ML churn scoring, and Snowflake Cortex AI indexing.

<img width="1151" height="562" alt="image" src="https://github.com/user-attachments/assets/7de36013-b61d-4074-9b32-7b293e86f27d" />

* **Data Sources:** Partner transaction streams (JSON), daily settlement files, account activity logs, support tickets, and PDF policy documents.
* **Ingestion:** AWS Lambda, AWS S3, Snowflake Snowpipe (streaming), and atomic SQL stored procedures (batch).
* **Storage (Data Lake & Landing):** AWS S3 staging buckets and persistent Snowflake landing tables.
* **Processing / Transformation:** dbt Core (Staging $\rightarrow$ Intermediate $\rightarrow$ Marts) on Snowflake.
* **Data Warehouse:** Snowflake Enterprise (FINTECH_PROD).
* **Orchestration:** Apache Airflow with Astronomer Cosmos (DbtDag).
* **Machine Learning & AI:** Snowpark ML (XGBoost Churn Classifier) and Snowflake Cortex AI (Cortex Analyst & Cortex Search Services)

## 📂 Data Sources & Schema Overview

The platform ingests and processes six heterogeneous data streams across batch, streaming, and unstructured formats:

| Data Stream | Type / Format | Granularity | Key Elements & Business Purpose |
| :--- | :--- | :--- | :--- |
| **Settlement Files** | Semi-structured (CSV) | Daily per merchant/batch | Settlement amounts, fees, reserve holds, and merchant bank payout accounts. |
| **Partner Transactions** | Unstructured JSON | Real-time event level | Transaction UUIDs, payment methods, authorization codes, geolocation, and device fingerprints. |
| **Support Cases** | Structured JSON | Daily updates | Ticket IDs, customer IDs, issue categories, resolution statuses, and agent notes. |
| **Exchange Rates** | Structured CSV/API | Daily FX rates | Base currency, quote currency, spot rates, and bank reference timestamps. |
| **Account Activity** | Semi-structured JSON | Event snapshot (3-day batch) | Account state changes, risk tier updates, login anomalies, and limit adjustments. |
| **Policy Documents** | Unstructured PDF | Static reference | Compliance rules, regulatory requirements, fee schedules, and dispute resolution guidelines. |

## 📥 Ingestion & Data Governance Framework

The ingestion framework enforces strict quality guardrails tailored to each data stream type:

| Source | Ingestion Pattern | Cadence | Governance & Quality Enforcement |
| :--- | :--- | :--- | :--- |
| **Settlements** | Airflow $\rightarrow$ Lambda $\rightarrow$ S3 $\rightarrow$ `COPY INTO` | Daily batch (`generate_daily_sources_dag`) | Schema drift & null/duplicate validation via `load_and_validate_settlements.sql`. |
| **Partner Transactions** | Airflow $\rightarrow$ Lambda $\rightarrow$ S3 $\rightarrow$ Snowpipe | Continuous stream (`generate_partner_transactions_dag`) | Dynamic Tables (`partner_transactions_clean_quarantine.sql`) isolate malformed records into `QUARANTINE`. |
| **Support Cases** | Airflow $\rightarrow$ Lambda $\rightarrow$ S3 $\rightarrow$ `COPY INTO` | Daily batch (`generate_daily_sources_dag`) | Schema drift & key checks via `load_and_validate_support_cases.sql`. |
| **Exchange Rates** | Airflow $\rightarrow$ Lambda $\rightarrow$ S3 $\rightarrow$ `COPY INTO` | Daily batch (`generate_daily_sources_dag`) | Key-set drift check against baseline snapshots via `load_and_validate_exchange_rates.sql`. |
| **Account Activity** | Airflow $\rightarrow$ Lambda $\rightarrow$ S3 $\rightarrow$ `COPY INTO` | Every 3 days (`generate_account_activity_dag`) | Event payload flattening and key validation via `load_and_validate_account_activity.sql`. |
| **Policy Documents** | S3 $\rightarrow$ `AI_PARSE_DOCUMENT` (OCR) | Static reference | Extracted text indexed for Cortex Search vector retrieval. |

### 🛡️ Dual Ingestion Governance Design

* **Batch Ingestion (Imperative Guardrail):** Pre-RAW stored procedures load incoming files into persistent landing tables, check column schemas against `SCHEMA_SNAPSHOTS`, and verify key non-null/uniqueness constraints. Invalid payloads raise explicit SQL exceptions, immediately halting execution, logging errors to `INGESTION_AUDIT_LOG`, and sending automated Slack alerts with the root cause.
* **Streaming Ingestion (Declarative Isolation):** Snowpipe continuously streams raw JSON payloads into `RAW.PARTNER_TRANSACTIONS`. Asynchronous Dynamic Tables automatically evaluate business completeness rules, routing valid records to `PARTNER_TRANSACTIONS_CLEAN` and corrupted payloads to `PARTNER_TRANSACTIONS_QUARANTINE` without interrupting stream ingestion. A daily sidecar task (`check_partner_transactions_health.sql`) runs `VALIDATE_PIPE_LOAD()` to audit for unparseable files skipped before row creation.

## 🛠️ Data Transformation & Modeling (dbt Medallion Architecture)

The transformation pipeline follows a multi-tiered Medallion Architecture managed via dbt Core, enforcing data quality, lineage tracking, and strict business logic modularity across layers:

| Layer | Target Schema | Materialization | Description & Business Logic |
| :--- | :--- | :--- | :--- |
| **Bronze (Staging)** | `STAGING` | Views / Incremental | Casts data types, standardizes field naming conventions, flattens raw JSON payloads, and filters out deleted or quarantined records. |
| **Silver (Intermediate)** | `INTERMEDIATE` | Ephemeral / Tables | Performs complex business logic, computes rolling customer metrics, aggregates daily merchant volume, and joins across core domain entities. |
| **Gold (Marts)** | `MARTS` | Tables / Dynamic Tables | Exposes production-ready dimensional models (Star Schema) tailored for executive dashboards, operational BI, and upstream ML feature stores. |

### 🥉 Bronze Layer (Staging)
* **`stg_settlements.sql`**: Normalizes daily merchant settlement files, converts currency metrics to standard bases, and generates audit hashes.
* **`stg_partner_transactions.sql`**: Consumes from `PARTNER_TRANSACTIONS_CLEAN`, parses nested JSON event metadata, and standardizes ISO timestamp fields.
* **`stg_support_cases.sql`**: Extracts support ticket details, calculates initial response SLAs, and categorizes issue tags.
* **`stg_account_activity.sql`**: Computes point-in-time state changes for account risk status and login anomaly flags.

### 🥈 Silver Layer (Intermediate)
* **`int_customer_daily_summary.sql`**: Rollup model capturing 30/60/90-day rolling transaction volume, failure ratios, and support ticket frequency per customer.
* **`int_merchant_risk_profile.sql`**: Joins settlement data with dispute records to monitor chargeback-to-volume ratios against regulatory thresholds.
* **`int_exchange_rate_converter.sql`**: Applies daily FX spot rates to multi-currency partner transaction streams for standardized USD reporting.

### 🥇 Gold Layer (Marts)
* **`dim_customers.sql`**: Master customer dimension table including current risk tier, lifetime value (LTV), and churn probability outputs.
* **`dim_merchants.sql`**: Consolidated merchant registry with onboarding metadata, settlement bank routing status, and risk flags.
* **`fct_transactions.sql`**: Core transaction fact table modeled for fast sub-second querying across payment routing and approval metrics.
* **`mart_customer_360.sql`**: Unified 360-degree view combining transaction history, support history, and risk scores designed specifically for consumption by **Snowflake Cortex AI** and **Sigma BI**.

### 🧪 Data Testing & Quality Assurance
* **Custom Data Assertions:** Custom dbt tests validate financial balances (e.g., ensuring `gross_amount = net_amount + fee_amount`) before promoting models to `MARTS`.

## ⚙️ Orchestration (Airflow + Astronomer Cosmos)

Data workflows are orchestrated end-to-end using Apache Airflow and Astronomer Cosmos (`fintech_dbt_pipeline_cosmos.py`):

* **DAG Structure & Scheduling:** Airflow schedules and coordinates multi-step ingestion pipelines, running specialized DAGs (`generate_daily_sources_dag`, `generate_partner_transactions_dag`, and `generate_account_activity_dag`) to trigger AWS Lambda extractors and execute Snowflake pre-RAW validation procedures.
* **Dynamic dbt Parsing (Astronomer Cosmos):** Cosmos reads the dbt `manifest.json` and automatically translates the dbt transformation DAG into native Airflow task groups (`DbtTaskGroup`). This allows each dbt model to run as an isolated Airflow task with full UI visibility and lineage tracking.
* **Task-Level Parallelism:** Independent dbt models across staging, intermediate, and marts layers execute concurrently across Airflow workers. A failure in an upstream model halts only its direct downstream dependencies, allowing unrelated pipeline branches to finish successfully.
* **Automated Failure Alerting:** Custom `on_failure_callback` hooks capture execution runtime errors, query logs, and model failures, instantly routing formatted payload alerts to Slack channels for quick debugging.
* **Audit Logging:** Task exceptions automatically capture and write detailed error metadata to `FINTECH_PROD.GOVERNANCE.PIPELINE_ERROR_LOG` for auditing and SLA tracking.

---

### 📊 Pipeline Execution & Slack Observability

#### Airflow DAG Graph View
![Airflow DAG Architecture](https://github.com/user-attachments/assets/b70d0d40-7f4f-4900-b14e-b30cef63a6d5)

#### Success Execution Flow & Automated Slack Notification
| Airflow Success Execution | Slack Success Alert |
| :---: | :---: |
| ![Airflow Success](https://github.com/user-attachments/assets/4a8f8722-0e9f-4afd-bd81-afce7940b8c7) |!<img width="1507" height="602" alt="image" src="https://github.com/user-attachments/assets/652ececd-9b12-4a72-8e35-fa0d3d0aeb8b" />
 |

#### Failure Isolation & Automated Slack Alerting
| Airflow Isolated Failure | Slack Failure Alert |
| :---: | :---: |
| ![Airflow Pipeline Failure](https://github.com/user-attachments/assets/6970b8d8-eca1-41ba-ba0a-64371bab71fa) | ![Slack Failure Message](https://github.com/user-attachments/assets/6cccd813-c238-4a12-80d3-68e2d5666cbf) |

## 🧠 Machine Learning — Churn Prediction (Snowpark ML)

Customer churn is predicted directly inside Snowflake using Snowpark ML (`churn_model.sql`):

* **Model & Feature Engineering:** XGBoost classifier trained on `MART_CUSTOMER_360` using 14 customer behavioral features (e.g., 30-day transaction volume drop-off, support ticket frequency, failed charge ratios, and FX usage).
* **Inference Pipeline:** Batch scoring runs on a scheduled basis via stored procedures (`SCORE_CHURN_MODEL()`), outputting probability scores and risk buckets directly to `CHURN_PREDICTIONS` for consumption by risk teams.

---

## 🤖 AI Layer & Natural Language Analytics (Snowflake Cortex)

An integrated Snowflake Cortex Agent (`FINTECH_ASSISTANT_AGENT`) allows non-technical business stakeholders to query structured operational metrics and unstructured enterprise documentation using plain English.

![Snowflake Cortex Agent Architecture](https://github.com/user-attachments/assets/728ed7cb-efc7-4a20-8b8f-a89a4e6a0110)

---

### 🛠️ Agent Tools & Services

| Tool Name | Tool Type | Underlying Asset / Data Source | Warehouse / Compute |
| :--- | :--- | :--- | :--- |
| **`fintech_analytics`** | Cortex Analyst (Text-to-SQL) | `FINTECH_ANALYTICS_VIEW` (Semantic View) | `CORTEX_WH` |
| **`policy_docs_search`** | Cortex Search (Vector Index) | `POLICY_DOCS_SEARCH` Service | Snowflake Managed |
| **`support_cases_search`** | Cortex Search (Vector Index) | `SUPPORT_CASES_SEARCH` (Filterable by Category/Priority/Status) | Snowflake Managed |

---

### 💬 Conversational Interface in Action

Below is an execution trace of **Cortex Analyst** dynamically generating SQL queries from natural language requests to analyze customer transaction metrics:

![Cortex Analyst Interface](https://github.com/user-attachments/assets/9150274b-721e-4667-b1c5-71e5d3552ca3)

## 🛠️ Tech Stack & Tools

* **Languages:** Python 3.10+, SQL
* **Cloud & Warehouse:** AWS (S3, Lambda), Snowflake Enterprise
* **Transformation & ML:** dbt Core 1.8+, Snowpark ML (XGBoost)
* **Orchestration:** Apache Airflow, Astronomer Cosmos
* **AI & Search:** Snowflake Cortex Analyst, Snowflake Cortex Search


```

## 📁 Repository Structure

```plaintext
.
├── dbt/                                    # dbt project root
│   ├── models/
│   │   ├── staging/                        # Staging models & live state logic
│   │   ├── intermediate/                   # Business logic & metric rollups
│   │   └── marts/                          # Production marts (Customer 360, KPIs)
│   ├── dbt_project.yml
│   └── packages.yml
├── infrastructure_and_ingestion/          # Snowflake setup & ingestion stored procedures
│   ├── 01_databaseschema_creation.sql      # DB, Schema, and Table DDL
│   ├── load_and_validate_settlements.sql   # Batch validation stored procedure
│   ├── partner_transactions_clean_quarantine.sql # Dynamic tables for streaming isolation
│   └── check_partner_transactions_health.sql     # Streaming pipe health check task
├── orchestration/                          # Airflow workflows
│   └── airflow-cosmos/
│       ├── dags/
│       │   ├── fintech_dbt_pipeline_cosmos.py # Cosmos dbt DAG execution
│       │   ├── generate_daily_sources_dag.py  # Daily batch ingestion DAG
│       │   └── generate_partner_transactions_dag.py # Streaming DAG
│       ├── churn_model.sql                 # Snowpark ML training/scoring SQL
│       ├── setup.ps1                       # Local sync setup script
│       └── Dockerfile
├── snowflake/
│   └── cortex/                             # AI & Semantic Layer DDLs
│       ├── cortex_analyst.sql              # Cortex Analyst semantic view DDL
│       ├── cortex_search_policydocs.sql    # Policy vector search DDL
│       └── cortex_search_supportcases.sql  # Support ticket vector search DDL
└── README.md

## 🚀 Getting Started

### 📋 Prerequisites

Before running the platform, ensure you have the following installed and configured:

- Docker Desktop & Astro CLI (required for local Airflow execution)
- Python 3.10+ (local runtime environment)
- Snowflake Account (Enterprise Edition with Cortex AI features enabled)
- AWS CLI (configured with read/write access to your staging S3 buckets)

### 💻 Local Installation & Setup

### Step 1 — Clone the repository

```bash
git clone https://github.com/sriramsripada20s/banking-analytics-platform-aws-snowflake.git
cd banking-analytics-platform-aws-snowflake
```

### Step 2 — Set up the Python environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r orchestration/airflow-cosmos/requirements.txt
```

### Step 3 — Provision Snowflake resources and governance

Run the following SQL scripts in Snowsight (or via SnowSQL), in order:

1. **Database & schema creation** — `infrastructure_and_ingestion/01_databaseschema_creation.sql`
2. **Batch validation procedures & streaming isolation tables**:
   - `infrastructure_and_ingestion/load_and_validate_settlements.sql`
   - `infrastructure_and_ingestion/partner_transactions_clean_quarantine.sql`
   - `infrastructure_and_ingestion/check_partner_transactions_health.sql`
3. **Cortex AI & search services**:
   - `snowflake/cortex/cortex_analyst.sql`
   - `snowflake/cortex/cortex_search_policydocs.sql`
   - `snowflake/cortex/cortex_search_supportcases.sql`

### Step 4 — Launch the Airflow environment

```bash
cd orchestration/airflow-cosmos
astro dev start
```

Access the Airflow UI at `http://localhost:8080` (default login: `admin` / `admin`).

### Step 5 — Run dbt transformations directly (optional, outside Airflow)

```bash
cd ../../dbt
dbt deps
dbt run
dbt test
```
