{{ 
config(
      materialized='incremental',
      unique_key= 'LOCATION_ID',
      tags = 'marts' 
      ) 
}}
{% if not is_incremental() %}
  -- this filter will only be applied on an incremental run
    
with location_source as (

    select * from {{ ref('location_staging') }}
      
),

final as (
    select * from location_source
)

select * from final
 
{% endif %}
{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
    
with location_source as (

    select * from {{ ref('location_staging') }}
      
),

final as (
    select * from location_source
)

select * from final
 
{% endif %}