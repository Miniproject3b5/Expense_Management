with source_customer as (
    select * from {{ source('EXPENSE_MANAGEMENT_DB','CUSTOMER') }}
),

final as (
    select * from source_customer 
)

select * from final