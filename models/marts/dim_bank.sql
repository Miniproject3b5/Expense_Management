{{ 
config (
      materialized='incremental',
      unique_key= 'bank_id',
      tags = 'marts' )
      
}}
{% if not is_incremental() %}
  -- this filter will only be applied on an incremental run
    
with bank_source as (

    select * from {{ ref('bank_staging') }}
      
),

final as (
    select * from bank_source
)

select * from final
 
{% endif %}


{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
    
with bank_source as (

    select * from {{ ref('bank_staging') }}
      
),

final as (
    select * from bank_source
)

select * from final
 
{% endif %}