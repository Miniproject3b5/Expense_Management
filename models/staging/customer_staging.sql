{{ 
config(
      prehook = "truncate table {{this}}" 
      ) 
}}
with source_customer1 as (
     select * from {{source('PC_DBT_DB','customer_check2') }}
),

final as (
    select customer_id ,
     customer_name ,
      date_of_birth , 
      gender  ,
      income ,
      phone_number ,
      email_id ,
      location_id ,
      created_date  ,
      last_modified from source_customer1
      where metadata$action = 'insert'
)

select * from final