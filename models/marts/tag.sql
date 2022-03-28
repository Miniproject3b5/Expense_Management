with source_tag as (
    select * from {{ source('EXPENSE_MANAGEMENT_DB','TAGS') }}
),

final as (
    select * from source_tag 
)

select * from final