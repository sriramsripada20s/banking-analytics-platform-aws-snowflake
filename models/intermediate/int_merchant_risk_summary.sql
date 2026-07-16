/* ================================================================
   int_merchant_risk_summary
   GRAIN: one row per merchant_id (every merchant, active or not)

   ANSWERS:
   - Which merchants have abnormal decline rates?  -> decline_rate,
     is_high_decline_merchant
   - Merchant risk/fraud monitoring                 -> risk_category
     combined with actual observed transaction behavior

   INPUTS:
   - stg_merchants            -> base merchant attributes
   - int_transactions_unified -> settlements + partner events,
                                  outcome flags pre-built

   JOINS: LEFT, merchants as anchor. A merchant with zero transactions
   yet (new merchant, or one nobody's transacted with) is still a
   real merchant and must still appear — same LEFT JOIN principle as
   every other intermediate model in this project.
   ================================================================ */

with merchants as (
    select * from {{ ref('stg_merchants') }}
),

transactions as (
    select * from {{ ref('int_transactions_unified') }}
),

merchant_txn_summary as (
    select
        merchant_id,
        count(*) as total_transaction_count,
        sum(amount) as total_transaction_amount,
        avg(amount) as avg_transaction_amount,

        sum(case when is_declined_or_failed then 1 else 0 end) as declined_count,
        sum(case when is_successful then 1 else 0 end) as successful_count,

        sum(case when transaction_date >= dateadd('day', -30, current_date()) then 1 else 0 end) as transaction_count_30d,
        sum(case when transaction_date >= dateadd('day', -30, current_date())
                  and is_declined_or_failed then 1 else 0 end) as declined_count_30d,

        max(transaction_date) as last_transaction_date
    from transactions
    group by merchant_id
)

select
    m.merchant_id,
    m.merchant_name,
    m.merchant_category_code,
    m.industry,
    m.country,
    m.risk_category,

    coalesce(t.total_transaction_count, 0) as total_transaction_count,
    coalesce(t.total_transaction_amount, 0) as total_transaction_amount,
    coalesce(t.avg_transaction_amount, 0) as avg_transaction_amount,
    coalesce(t.declined_count, 0) as declined_count,
    coalesce(t.transaction_count_30d, 0) as transaction_count_30d,
    coalesce(t.declined_count_30d, 0) as declined_count_30d,
    t.last_transaction_date,

    -- the core feature this model exists to produce
    case
        when coalesce(t.total_transaction_count, 0) = 0 then null
        else round(t.declined_count / t.total_transaction_count * 100, 2)
    end as decline_rate_pct,

    case
        when coalesce(t.transaction_count_30d, 0) = 0 then null
        else round(t.declined_count_30d / t.transaction_count_30d * 100, 2)
    end as decline_rate_30d_pct,

    -- flag: meaningfully abnormal decline rate, requiring a minimum
    -- volume so one or two declines out of three transactions doesn't
    -- falsely trigger this (statistically meaningless at low volume)
    (coalesce(t.total_transaction_count, 0) >= 20
        and (t.declined_count / nullif(t.total_transaction_count, 0)) > 0.15
    ) as is_high_decline_merchant

from merchants m
left join merchant_txn_summary t on m.merchant_id = t.merchant_id