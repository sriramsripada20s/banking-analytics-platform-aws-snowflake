with source as (
    select * from {{ source('raw','accounts_api_raw') }}
),

flattened as (
    select
        f.value:account_id::string            as account_id,
        f.value:customer_id::string           as customer_id,
        f.value:account_type::string          as account_type,
        f.value:account_status::string        as account_status_at_creation,
        f.value:current_balance::number(12,2) as balance_at_creation,
        f.value:currency::string              as currency,
        f.value:created_at::timestamp_ntz     as created_at,
        f.value:updated_at::timestamp_ntz     as file_updated_at,
        source._loaded_at,
        source._source_file
    from source,
        lateral flatten(input => source.raw_payload:records) f
)

SELECT * FROM flattened
qualify row_number() OVER(
    PARTITION BY account_id 
    ORDER BY file_updated_at DESC, _loaded_at DESC
    ) = 1