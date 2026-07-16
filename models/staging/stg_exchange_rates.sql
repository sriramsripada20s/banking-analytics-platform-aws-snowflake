with source as (
    select * from {{ source('raw', 'external_exchange_rates') }}
),

flattened as (
    select
        source.raw_payload:base_currency::string        as base_currency,
        source.raw_payload:ingested_at::timestamp_ntz    as ingested_at,
        source.raw_payload:payload:date::date            as rate_date,
        f.key::string                                     as quote_currency,
        f.value::number(12,6)                              as exchange_rate,
        source._loaded_at,
        source._source_file
    from source,
         lateral flatten(input => source.raw_payload:payload:rates) f
)

select * from flattened