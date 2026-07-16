with source as (
    select * from {{ source('raw', 'partner_transactions') }}
)

select
    raw_payload:event_id::string                     as event_id,
    raw_payload:transaction_id::string                as transaction_id,
    raw_payload:account_id::string                    as account_id,
    raw_payload:merchant_id::string                   as merchant_id,
    raw_payload:event_timestamp::timestamp_ntz         as event_timestamp,
    raw_payload:amount::number(12,2)                   as amount,
    raw_payload:currency::string                        as currency,
    raw_payload:authorization_status::string             as authorization_status,
    raw_payload:channel::string                          as channel,
    raw_payload:device_id::string                        as device_id,
    _loaded_at,
    _source_file
from source