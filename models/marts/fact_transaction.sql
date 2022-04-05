-- depends_on: {{ ref('transaction_staging') }}
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

    select {{ dbt_utils.surrogate_key([
                'SOURCE_NUMBER', 
                'CUSTOMER_ID', 
                'TRANSACTION_SOURCE',     
            ]) }} as customersid  , TRANSACTION_ID,TRANSACTION_AMOUNT as AMOUNT , VENDOR_NAME , 
            CATEGORY , TRANSACTION_TYPE, 
             TRANSACTION_TIMESTAMP,PROCESSED_TIMESTAMP  from {{source('EXPENSE_MANAGEMENT_DB','TRANSACTION_DETAILS') }}
      
),


final as (
    select distinct * from transaction_source
)

select distinct * from final
 
{% endif %} 


 
{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
    
with transaction_source as (

     select {{ dbt_utils.surrogate_key([
                'SOURCE_NUMBER', 
                'CUSTOMER_ID', 
                'TRANSACTION_SOURCE',     
            ]) }} as customersid , TRANSACTION_ID ,TRANSACTION_AMOUNT as AMOUNT , VENDOR_NAME , CATEGORY , TRANSACTION_TYPE, TRANSACTION_TIMESTAMP,PROCESSED_TIMESTAMP from {{ ref('transaction_staging') }}
      
),


final as (
    select distinct * from transaction_source
)

select distinct * from final
 
{% endif %} 

