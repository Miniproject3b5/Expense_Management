{{ 
config(
      prehook  = "truncate table {{this}}" 
      ) 
}}

with source_bank1 as (
     select * from {{source('EXPENSE_MANAGEMENT_DB','bank_check2') }}
),

final as (
    select distinct bank_id , 
    customer_id ,
     bank_name , 
     bank_account_number , 
     branch , 
     IFSC_CODE , created_date ,
     last_modified from source_bank1 
     where metadata$action = 'INSERT'
)

select * from final 