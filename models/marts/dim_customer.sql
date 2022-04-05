-- depends_on: {{ ref('customer_staging') }}
{{ 
config(
      materialized='incremental',
      unique_key= 'CUSTOMER_ID',
      tags = 'marts'
      ) 
}}
{% if not is_incremental() %}
  -- this filter will only be applied on an incremental run
    
with customer_source as (

    select * from {{source('EXPENSE_MANAGEMENT_DB','CUSTOMER') }}
      
),

final as (
    select * from customer_source
)

select * from final
 
{% endif %}
{% if is_incremental() %}
  -- this filter will only be applied on an non incremental run
    
with customer_source as (

    select * from {{ ref('customer_staging') }}
      
),
final as (
    select * from customer_source
)

select * from final
 
{% endif %}