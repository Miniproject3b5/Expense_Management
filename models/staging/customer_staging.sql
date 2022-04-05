{{ 
config(
      prehook = "truncate table {{this}}" 
      ) 
}}
with source_customer1 as (
     select * from {{source('EXPENSE_MANAGEMENT_DB','customer_check2') }}
),

final as (
    select customer_id ,
     customer_name ,
      date_of_birth , 
      gender  ,
      income ,
      PROFESSION ,
      phone_number ,
      email_id ,
      location_id ,
      created_date  ,
      last_modified from source_customer1
      where metadata$action = 'INSERT'
)

select distinct * from final