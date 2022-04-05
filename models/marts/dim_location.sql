-- depends_on: {{ ref('location_staging') }}
{{ 
config(
      materialized='incremental',
      unique_key= 'LOCATION_ID',
      tags = 'marts' 
      ) 
}}
{% if not is_incremental() %}
  -- this filter will only be applied on an non incremental run
    
with location_source as (

    select distinct * from {{source('EXPENSE_MANAGEMENT_DB','LOCATION') }}
      
),

final as (
    select * from location_source
)

select distinct * from final
 
{% endif %}




{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
    
with location_source as (

    select * from {{ ref('location_staging') }}
      
),

final as (
    select * from location_source
)

select distinct * from final
 
{% endif %}