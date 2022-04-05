 -- depends_on: {{ ref('bank_staging') }}
{{ 
config (
      materialized='incremental',
      unique_key= 'bank_id',
      tags = 'marts' )
      
}}
{% if not is_incremental() %}
  -- this filter will only be applied on an non incremental run
    
with bank_source as (

    select * from {{source('EXPENSE_MANAGEMENT_DB','BANK_DETAILS') }}
      
),

final as (
    select BANK_ID,BANK_NAME,BANK_ACCOUNT_NUMBER , BRANCH, IFSC_CODE , CREATED_DATE from bank_source
)

select * from final
 
{% endif %}



{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
    
with bank_source as (

    select * from {{ ref('bank_staging') }}
      
),

final as (
    select BANK_ID,BANK_NAME,BANK_ACCOUNT_NUMBER , BRANCH, IFSC_CODE , CREATED_DATE from bank_source
)

select * from final
 
{% endif %}