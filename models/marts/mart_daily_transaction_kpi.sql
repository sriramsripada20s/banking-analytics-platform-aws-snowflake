/* ================================================================
   mart_daily_transaction_kpi
   GRAIN: one row per transaction_date — daily executive KPI rollup

   ANSWERS: "what's our daily transaction volume and value?" directly.

   INPUT: int_transactions_unified only — no other joins needed,
   everything required (amount, status, date) already lives there.
   ================================================================ */

select
    transaction_date,

    count(*) as total_transaction_count,
    sum(amount) as total_transaction_amount,
    avg(amount) as avg_transaction_amount,

    sum(case when is_successful then 1 else 0 end) as successful_count,
    sum(case when is_declined_or_failed then 1 else 0 end) as declined_or_failed_count,
    round(
        sum(case when is_declined_or_failed then 1 else 0 end) / count(*) * 100, 2
    ) as decline_rate_pct,

    sum(case when transaction_source = 'SETTLEMENT' then 1 else 0 end) as settlement_count,
    sum(case when transaction_source = 'PARTNER_EVENT' then 1 else 0 end) as partner_event_count,

    sum(case when is_weekend then amount else 0 end) as weekend_amount,
    sum(case when is_late_night then 1 else 0 end) as late_night_transaction_count

from {{ ref('int_transactions_unified') }}
group by transaction_date