with source as (
    select * from {{ source('raw', 'settlements') }}
)

select
    settlement_id,
    transaction_id,
    account_id,
    merchant_id,
    transaction_timestamp,
    settlement_date,
    amount,
    currency,
    settlement_status,
    fee_amount,
    _loaded_at,
    _source_file
from source