/* ================================================================
   int_customer_activity_summary
   GRAIN: one row per customer_id (every customer, active or not)

   ANSWERS:
   - Which customers are becoming inactive?      -> engagement_segment
   - Balance trend by customer segment?           -> total_balance, avg_account_balance
   - Support issues correlated with churn?         -> support_case_count vs. engagement_segment

   INPUTS:
   - stg_customers            -> base customer attributes
   - int_accounts_enriched    -> account + live balance (already joined upstream)
   - int_transactions_unified -> settlements + partner events, features pre-built
   - stg_support_cases        -> case-level detail, no reuse elsewhere yet

   JOINS: all LEFT, customers as anchor. A customer with zero
   activity is still a real customer and must still appear —
   INNER would silently delete exactly the population this model
   exists to find (dormant/at-risk customers).

   OUT OF SCOPE: merchant/transaction-level detail (see
   int_merchant_risk_summary), BI formatting (mart layer's job),
   churn PREDICTION (this is a rule-based signal, not a model —
   an ML model would consume this table as a feature source).
   ================================================================ */

with customers as (
    select * from {{ ref('stg_customers') }}
),

accounts as (
    select * from {{ ref('int_accounts_enriched') }}
),

transactions as (
    select * from {{ ref('int_transactions_unified') }}
),

support_cases as (
    select * from {{ ref('stg_support_cases') }}
),

-- one row per customer: account-level rollups
account_summary as (
    select
        customer_id,
        count(*) as total_accounts,
        sum(case when account_status = 'OPEN' then 1 else 0 end) as open_accounts,
        sum(case when account_status = 'FROZEN' then 1 else 0 end) as frozen_accounts,
        sum(case when account_status = 'CLOSED' then 1 else 0 end) as closed_accounts,
        sum(current_balance) as total_balance_across_accounts,
        avg(current_balance) as avg_account_balance,
        max(last_updated_at) as most_recent_account_update
    from accounts
    group by customer_id
),

-- one row per customer: transaction behavior rollups (via account_id join)
transaction_summary as (
    select
        a.customer_id,
        count(t.transaction_id) as transaction_count_all_time,
        sum(case when t.transaction_date >= dateadd('day', -30, current_date()) then 1 else 0 end) as transaction_count_30d,
        sum(case when t.transaction_date >= dateadd('day', -30, current_date()) then t.amount else 0 end) as transaction_amount_30d,
        sum(case when t.is_declined_or_failed then 1 else 0 end) as failed_payment_count,
        max(t.transaction_date) as last_transaction_date,
        datediff('day', max(t.transaction_date), current_date()) as days_since_last_transaction
    from accounts a
    left join transactions t on a.account_id = t.account_id
    group by a.customer_id
),

-- one row per customer: support case rollups
support_summary as (
    select
        customer_id,
        count(*) as support_case_count,
        sum(case when priority = 'HIGH' then 1 else 0 end) as high_priority_case_count,
        sum(case when status = 'OPEN' then 1 else 0 end) as open_case_count
    from support_cases
    group by customer_id
)

select
    c.customer_id,
    c.first_name,
    c.last_name,
    c.customer_status,
    c.risk_rating,
    c.created_at as customer_since,
    datediff('day', c.created_at, current_date()) as account_age_days,

    coalesce(acc.total_accounts, 0)                       as total_accounts,
    coalesce(acc.open_accounts, 0)                         as open_accounts,
    coalesce(acc.frozen_accounts, 0)                        as frozen_accounts,
    coalesce(acc.closed_accounts, 0)                         as closed_accounts,
    coalesce(acc.total_balance_across_accounts, 0)            as total_balance,
    coalesce(acc.avg_account_balance, 0)                        as avg_account_balance,

    coalesce(txn.transaction_count_all_time, 0)   as transaction_count_all_time,
    coalesce(txn.transaction_count_30d, 0)         as transaction_count_30d,
    coalesce(txn.transaction_amount_30d, 0)         as transaction_amount_30d,
    coalesce(txn.failed_payment_count, 0)            as failed_payment_count,
    txn.last_transaction_date,
    coalesce(txn.days_since_last_transaction, 9999) as days_since_last_transaction,

    coalesce(sup.support_case_count, 0)       as support_case_count,
    coalesce(sup.high_priority_case_count, 0)  as high_priority_case_count,
    coalesce(sup.open_case_count, 0)            as open_case_count,

    -- churn/engagement signal — the direct feature the original use
    -- case's "which customers are becoming inactive" question needs
    case
        when txn.days_since_last_transaction is null then 'NEVER_TRANSACTED'
        when txn.days_since_last_transaction <= 30 then 'ACTIVE'
        when txn.days_since_last_transaction <= 90 then 'AT_RISK'
        else 'DORMANT'
    end as engagement_segment,

    -- simple composite risk flag — combines account risk rating with
    -- behavioral signals, a reasonable starting feature for a churn model
    (c.risk_rating = 'HIGH'
        or coalesce(acc.frozen_accounts, 0) > 0
        or coalesce(sup.high_priority_case_count, 0) > 0
    ) as has_risk_flag

from customers c
left join account_summary acc on c.customer_id = acc.customer_id
left join transaction_summary txn on c.customer_id = txn.customer_id
left join support_summary sup on c.customer_id = sup.customer_id