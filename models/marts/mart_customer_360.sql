/* ================================================================
   mart_customer_360
   GRAIN: one row per customer — the BI/Cortex-facing customer view

   PURPOSE: Customer 360 view for BI and Cortex Analyst — one row per customer,
      combining identity, account, transaction, and support signals.
      Answers: "which customers are becoming inactive," "balance trend
   ================================================================ */

select
    customer_id,
    first_name,
    last_name,
    customer_status,
    risk_rating,
    customer_since,
    account_age_days,

    total_accounts,
    open_accounts,
    frozen_accounts,
    closed_accounts,
    total_balance,
    avg_account_balance,

    transaction_count_all_time,
    transaction_count_30d,
    transaction_amount_30d,
    failed_payment_count,
    last_transaction_date,
    days_since_last_transaction,

    support_case_count,
    high_priority_case_count,
    open_case_count,

    engagement_segment,
    has_risk_flag

from {{ ref('int_customer_activity_summary') }}