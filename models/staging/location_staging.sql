{{ 
config(
      prehook = "truncate table {{this}}" 
      ) 
}}
with source_location as (
     select * from {{source('PC_DBT_DB','location_check2') }}
),

final as (
    select location_id , city , state , pincode  from source_location
    where metadata$action = 'insert'
)

select * from final