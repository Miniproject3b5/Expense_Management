  -- depends_on: {{ ref('card_staging') }}
{{ 
config(
      materialized='incremental',
      unique_key= 'card_id',
      tags = 'marts'
      ) 
}}
{% if not is_incremental() %}
  -- this filter will only be applied on an non incremental run
    
with card_source as (

    select * from {{source('EXPENSE_MANAGEMENT_DB','CARD_DETAILS') }}
      
),

final as (
    select CARD_ID, CARD_VENDOR, CARD_TYPE ,CARD_NUMBER , VALIDITY , CREATED_DATE from card_source
)

select distinct * from final
 
{% endif %}
{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
    
with card_source as (

    select * from {{ ref('card_staging') }}
      
),

final as (
    select CARD_ID, CARD_VENDOR, CARD_TYPE ,CARD_NUMBER , VALIDITY , CREATED_DATE  from card_source
)

select distinct * from final
 
{% endif %}