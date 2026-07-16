with accounts as (
    select * from {{ ref('stg_accounts') }}
),

current_state as (
    select * from {{ ref('stg_accounts_current') }}
)

select
    accounts.account_id,
    accounts.customer_id,
    accounts.account_type,
    accounts.currency,
    accounts.created_at,

    -- we want the latest records so we first take the latest from account current if not found we fill it from accounts 
    coalesce(current_state.account_status, accounts.account_status_at_creation) as account_status,
    coalesce(current_state.current_balance, accounts.balance_at_creation)       as current_balance,
    coalesce(current_state.updated_at, accounts.created_at)                    as last_updated_at,

    current_state.account_id is not null as has_activity_history

from accounts
left join current_state
    on accounts.account_id = current_state.account_id