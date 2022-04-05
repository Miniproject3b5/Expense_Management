-- depends_on: {{ ref('transaction_staging') }}
{{ 
config(
      materialized='incremental',
      unique_key= 'CUSTOMER_ID',
      tags = 'marts'
      ) 
}}
{% if not is_incremental() %}
with transaction_source as (

  select * from {{source('EXPENSE_MANAGEMENT_DB','TRANSACTION_DETAILS') }}
      
),
final as
(
    select  {{ dbt_utils.surrogate_key([
                'TRANSACTION_ID', 
                'customer_id',    ]) }} as ID , customer_id , TRANSACTION_ID from transaction_source 
    
)
select distinct * from final

{% endif %}
{% if is_incremental() %}
with transaction_source as (

    select * from {{ ref('transaction_staging') }}
      
),
final as
(
    select  {{ dbt_utils.surrogate_key([
                'TRANSACTION_ID', 
                'CUSTOMER_ID',    ]) }} as ID , CUSTOMER_ID , TRANSACTION_ID from transaction_source 
    
)
select distinct * from final

{% endif %}
