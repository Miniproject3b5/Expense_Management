 -- depends_on: {{ ref('bank_staging') }}
 -- depends_on: {{ ref('card_staging') }}
{{ 
config(
      materialized ='incremental',
      unique_key= 'customersid',
      tags = 'marts'
      ) 
}}
{% if not is_incremental() %}
with 
bank1 as 
(
    select  bank_id as source_id , customer_id ,CREATED_DATE as created_at  ,'Bank' as source_type 
    from {{source('EXPENSE_MANAGEMENT_DB','BANK_DETAILS') }}
), 
card1 as 
(
    select card_id as source_id , customer_id , CREATED_DATE as created_at , 'Card' as source_type
     from {{source('EXPENSE_MANAGEMENT_DB','CARD_DETAILS') }}
),
aggregated  
as 
(
    select * from bank1  union select * from card1
),
final as 
(
    select 
        {{ dbt_utils.surrogate_key([
                'source_id', 
                'customer_id', 
                'source_type', 
                
            ]) }} as customersid,
            * from aggregated  
)
select distinct * from final
{% endif %} 

{% if is_incremental() %}
with 
bank1 as 
(
    select  bank_id as source_id , customer_id ,CREATED_DATE as created_at  ,'Bank' as source_type
     from {{ ref('bank_staging')}}
), 
card1 as 
(
    select card_id as source_id , customer_id , CREATED_DATE as created_at , 'Card' as source_type 
    from {{ ref('card_staging') }}
),
aggregated  
as 
(
    select * from bank1  union select * from card1
),
final as 
(
    select 
        {{ dbt_utils.surrogate_key([
                'source_id', 
                'customer_id', 
                'source_type', 
                
            ]) }} as customersid,
            * from aggregated  
)
select distinct * from final
{% endif %} 