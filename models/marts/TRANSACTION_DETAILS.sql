with source_transaction as (
    select * from {{ source('EXPENSE_MANAGEMENT_DB','TRANSACTION_DETAILS') }}
),

final as (
    select * from source_transaction
)

select * from final