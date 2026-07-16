/* ================================================================
   mart_merchant_performance
   GRAIN: one row per merchant — BI/Cortex-facing merchant view

   Answers: "which merchants have abnormal decline rates," merchant
   performance/risk monitoring dashboards.
   ================================================================ */

select
    merchant_id,
    merchant_name,
    merchant_category_code,
    industry,
    country,
    risk_category,

    total_transaction_count,
    total_transaction_amount,
    avg_transaction_amount,
    declined_count,
    transaction_count_30d,
    declined_count_30d,
    last_transaction_date,

    decline_rate_pct,
    decline_rate_30d_pct,
    is_high_decline_merchant

from {{ ref('int_merchant_risk_summary') }}