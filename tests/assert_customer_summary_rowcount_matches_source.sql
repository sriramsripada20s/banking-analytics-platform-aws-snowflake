/* Singular test — this test counts rows in two different tables and complains if the counts don't match.*/

with summary_count as (
    select count(*) as row_count from {{ ref('int_customer_activity_summary') }}
),

source_count as (
    select count(*) as row_count from {{ ref('stg_customers') }}
)

select
    summary_count.row_count as summary_row_count,
    source_count.row_count as source_row_count
from summary_count, source_count
where summary_count.row_count != source_count.row_count