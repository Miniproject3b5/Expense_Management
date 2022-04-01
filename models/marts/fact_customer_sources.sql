
with 
bank1 as 
 (
    select  bank_id as source_id , customer_id ,CREATED_DATE as created_at  ,'Bank' as source_type from {{ ref('bank_staging')}}
), 
card1 as 
(
    select card_id as source_id , customer_id , CREATED_DATE as created_at , 'Card' as source_type from {{ ref('card_staging') }}
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
select * from final