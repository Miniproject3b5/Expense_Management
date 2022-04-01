with transaction_source as (

    select * from {{ ref('transaction_staging') }}
      
),
final as
(
    select customer_id , transaction_id from transaction_source 
    
)
select * from final