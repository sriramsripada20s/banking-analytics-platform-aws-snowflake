with summary_count as (
    select count(*) as row_count from {{ ref('int_merchant_risk_summary') }}
),
source_count as (
    select count(*) as row_count from {{ ref('stg_merchants') }}
)
select summary_count.row_count as summary_row_count, source_count.row_count as source_row_count
from summary_count, source_count
where summary_count.row_count != source_count.row_count