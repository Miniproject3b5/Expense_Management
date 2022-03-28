with source_card as (
    select * from {{ source('EXPENSE_MANAGEMENT_DB','CARD_DETAILS') }}
),

final as (
    select * from source_card 
)

select * from final