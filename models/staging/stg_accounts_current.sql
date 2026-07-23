-- stg_accounts_current.sql (rewritten)
with ranked_activity as (
    select
        raw_payload:account_id::string as account_id,
        raw_payload:customer_id::string as customer_id,
        raw_payload:account_status::string as account_status,
        raw_payload:current_balance::number(12,2) as current_balance,
        raw_payload:currency::string as currency,
        raw_payload:updated_at::timestamp_ntz as updated_at,
        row_number() over (
            partition by raw_payload:account_id::string
            order by raw_payload:updated_at::timestamp_ntz desc
        ) as rn
    from {{ source('raw', 'account_activity') }}
)

select account_id, customer_id, account_status, current_balance, currency, updated_at
from ranked_activity
where rn = 1
