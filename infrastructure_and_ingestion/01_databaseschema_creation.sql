USE ROLE ACCOUNTADMIN;

/* ================================================================
   SECTION 1 — DATABASE & SCHEMAS
   ================================================================ */

CREATE DATABASE IF NOT EXISTS FINTECH_PROD;
USE DATABASE FINTECH_PROD;
 
CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS INTERMEDIATE;
CREATE SCHEMA IF NOT EXISTS CORE;
CREATE SCHEMA IF NOT EXISTS MARTS;
CREATE SCHEMA IF NOT EXISTS ML;
CREATE SCHEMA IF NOT EXISTS SEMANTIC;
CREATE SCHEMA IF NOT EXISTS GOVERNANCE;

CREATE TABLE IF NOT EXISTS GOVERNANCE.LOAD_AUDIT (
    load_id             STRING DEFAULT UUID_STRING(),
    source_system       STRING,
    target_table         STRING,
    file_name            STRING,
    row_count            NUMBER,
    load_status           STRING,
    load_started_at        TIMESTAMP_NTZ,
    load_completed_at       TIMESTAMP_NTZ,
    error_message          STRING
);
 
CREATE TABLE IF NOT EXISTS GOVERNANCE.DATA_QUALITY_RESULTS (
    test_run_id      STRING DEFAULT UUID_STRING(),
    model_name       STRING,
    test_name        STRING,
    test_status       STRING,
    failures_count     NUMBER,
    executed_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
 
CREATE TABLE IF NOT EXISTS GOVERNANCE.PIPELINE_RUNS (
    run_id          STRING DEFAULT UUID_STRING(),
    pipeline_name    STRING,
    trigger_type      STRING,
    status             STRING,
    started_at           TIMESTAMP_NTZ,
    ended_at              TIMESTAMP_NTZ
);


