with source_bank as (
    select * from {{ source('EXPENSE_MANAGEMENT_DB','BANK_DETAILS') }}
),

final as (
    select * from source_bank 
)

select * from final