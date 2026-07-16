with source as (
    select * from {{ source('raw', 'account_activity') }}
)

select
    raw_payload:account_id::string        as account_id,
    raw_payload:customer_id::string       as customer_id,
    raw_payload:account_status::string    as account_status,
    raw_payload:current_balance::number(12,2) as balance,
    raw_payload:currency::string          as currency,
    raw_payload:updated_at::timestamp_ntz as activity_timestamp,
    _loaded_at,
    _source_file
from source