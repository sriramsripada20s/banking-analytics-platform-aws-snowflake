/* ================================================================
   int_transactions_unified
   GRAIN: one row per transaction (settlements + partner events combined)

   ANSWERS:
   - What's our daily transaction volume/value?  -> transaction_date,
     amount, status
   - Foundation for all downstream fraud/decline/customer features

   INPUTS:
   - stg_settlements            -> nightly batch transactions
   - stg_partner_transactions   -> real-time card authorization events

   JOINS: none — this is a UNION ALL of two same-grain sources, not a
   join. transaction_source distinguishes origin; ingestion_channel
   shows how the pipeline loaded it; transaction_channel is the
   customer-facing channel (partner events only, null for settlements).

   FEATURES BUILT HERE (not left for later models to recompute):
   time-based (is_weekend, is_late_night), amount_bucket, and outcome
   flags (is_declined_or_failed, is_successful) — every downstream
   model that aggregates transactions reuses these instead of
   ================================================================ */

with settlements as (
    select
        settlement_id as transaction_id,
        account_id,
        merchant_id,
        transaction_timestamp,
        amount,
        currency,
        settlement_status as status,
        'SETTLEMENT' as transaction_source,
        'BATCH' as ingestion_channel,
        null as transaction_channel
    from {{ ref('stg_settlements') }}
),

partner_txns as (
    select
        transaction_id,
        account_id,
        merchant_id,
        event_timestamp as transaction_timestamp,
        amount,
        currency,
        authorization_status as status,
        'PARTNER_EVENT' as transaction_source,
        'SNOWPIPE_REALTIME' as ingestion_channel,
        channel as transaction_channel
    from {{ ref('stg_partner_transactions') }}
),
--A UNION ALL statement physically stacks tables on top of each other. To make a stack work, both tables must have the exact same number of columns in the exact same order
unified as (
    select * from settlements
    union all
    select * from partner_txns
)

select
    transaction_id,
    account_id,
    merchant_id,
    transaction_timestamp,
    amount,
    currency,
    status,
    transaction_source,       -- SETTLEMENT or PARTNER_EVENT: which source table this came from
    ingestion_channel,        -- BATCH or SNOWPIPE_REALTIME: how the pipeline loaded it
    transaction_channel,      -- ONLINE / IN_STORE / MOBILE_APP / ATM (partner txns only, null for settlements)

    -- time-based features
    date(transaction_timestamp)                as transaction_date,
    hour(transaction_timestamp)                 as transaction_hour,
    dayofweek(transaction_timestamp)            as day_of_week,
    dayofweek(transaction_timestamp) in (0, 6)  as is_weekend,
    hour(transaction_timestamp) between 0 and 5 as is_late_night,

    -- amount features
    case
        when amount < 25 then 'MICRO'
        when amount < 200 then 'SMALL'
        when amount < 1000 then 'MEDIUM'
        else 'LARGE'
    end as amount_bucket,

    -- outcome flags
    status in ('DECLINED', 'FAILED') as is_declined_or_failed,
    status in ('APPROVED', 'SETTLED') as is_successful

from unified