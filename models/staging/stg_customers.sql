with source as (
    select * from {{ source('raw', 'customers_api_raw') }}
),

flattened as (
    select
        f.value:customer_id::string           as customer_id,
        f.value:first_name::string            as first_name,
        f.value:last_name::string             as last_name,
        f.value:email::string                 as email,
        f.value:phone::string                 as phone,
        f.value:city::string                  as city,
        f.value:state::string                 as state,
        f.value:country::string               as country,
        f.value:customer_status::string       as customer_status,
        f.value:risk_rating::string           as risk_rating,
        f.value:created_at::timestamp_ntz     as created_at,
        f.value:updated_at::timestamp_ntz     as updated_at,
        source._loaded_at,
        source._source_file
    from source,
         lateral flatten(input => source.raw_payload:records) f
)

select *
from flattened
qualify row_number() over (
    partition by customer_id
    order by updated_at desc, _loaded_at desc
) = 1