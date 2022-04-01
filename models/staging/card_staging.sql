{{ 
config(
      prehook = "truncate table {{this}}" 
      ) 
}}
with source_card1 as (
    select * from {{source('PC_DBT_DB','card_check2') }}
),

final as (
    select card_id ,
    customer_id ,
     card_number , 
     card_type , 
     card_vendor , 
     validity , bank_name ,created_date ,last_modified  from source_card1
     where metadata$action = 'insert'
)

select * from final