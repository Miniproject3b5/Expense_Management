with source_location as (
    select * from {{ source('EXPENSE_MANAGEMENT_DB','LOCATION') }}
),

final as (
    select * from source_location
)

select * from final