{{ 
config(
      materialized='incremental',
      unique_key= 'card_id',
      tags = 'marts'
      ) 
}}
{% if not is_incremental() %}
  -- this filter will only be applied on an incremental run
    
with card_source as (

    select * from {{ ref('card_staging') }}
      
),

final as (
    select * from card_source
)

select * from final
 
{% endif %}

{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
    
with card_source as (

    select * from {{ ref('card_staging') }}
      
),

final as (
    select * from card_source
)

select * from final
 
{% endif %}