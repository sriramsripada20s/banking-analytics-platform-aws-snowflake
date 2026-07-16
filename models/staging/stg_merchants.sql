with source as (
    select * from {{ source('raw', 'merchants_api_raw') }}
),

flattened as (
    select
        f.value:merchant_id::string              as merchant_id,
        f.value:merchant_name::string             as merchant_name,
        f.value:merchant_category_code::string    as merchant_category_code,
        f.value:industry::string                  as industry,
        f.value:country::string                   as country,
        f.value:risk_category::string             as risk_category,
        source._loaded_at,
        source._source_file
    from source,
         lateral flatten(input => source.raw_payload:records) f
)

select *
from flattened
qualify row_number() over (
    partition by merchant_id
    order by _loaded_at desc
) = 1