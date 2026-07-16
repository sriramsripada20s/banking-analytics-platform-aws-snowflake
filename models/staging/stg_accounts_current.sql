with source as (
    select * from {{ source('core', 'accounts_current') }}
)

select
    account_id,
    customer_id,
    account_status,
    current_balance,
    currency,
    updated_at,
    _merged_at
from source