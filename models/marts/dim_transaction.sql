
{{ 
config(
      materialized ='incremental',
      unique_key= 'TRANSACTION_ID',
      tags = 'marts'
      ) 
}}
{% if not is_incremental() %}
  -- this filter will only be applied on an incremental run
    
with transaction_source as (

    select * from {{ ref('transaction_staging') }}
      
),


final as (
    select * from transaction_source
)

select * from final
 
{% endif %} 


 
{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
    
with transaction_source as (

    select * from {{ ref('transaction_staging') }}
      
),


final as (
    select * from transaction_source
)

select * from final
 
{% endif %} 

