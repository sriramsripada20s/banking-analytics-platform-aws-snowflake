with source as (
    select * from {{ source('raw', 'support_cases') }}
)

select
    raw_payload:case_id::string           as case_id,
    raw_payload:customer_id::string       as customer_id,
    raw_payload:category::string          as category,
    raw_payload:priority::string          as priority,
    raw_payload:status::string            as status,
    raw_payload:narrative::string         as narrative,
    raw_payload:opened_at::timestamp_ntz  as opened_at,
    _loaded_at,
    _source_file
from source