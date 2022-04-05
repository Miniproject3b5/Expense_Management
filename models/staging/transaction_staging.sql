{{ 
config(
      prehook = "truncate table {{this}}" 
      ) 
}}
with source_transaction as (
     select * from {{source('EXPENSE_MANAGEMENT_DB','transaction_check2') }}
),

final as (
    select 
VENDOR_NAME,
TRANSACTION_TYPE,
TRANSACTION_TIMESTAMP,
TRANSACTION_SOURCE,
TRANSACTION_ID,
TRANSACTION_AMOUNT,
TAG_ID,
SOURCE_NUMBER,
PROCESSED_TIMESTAMP,
CUSTOMER_ID,
CATEGORY from source_transaction
     where metadata$action = 'INSERT'
)

select * from final 