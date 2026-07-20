# banking-analytics-platform-aws-snowflake-dbt-airflow
End-to-end fintech data platform on AWS + Snowflake — multi-pattern ingestion (batch, event-driven, API), dbt transformations, and Cortex AI-powered analytics, built to demonstrate production data engineering patterns from raw data to a governed semantic layer....

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









