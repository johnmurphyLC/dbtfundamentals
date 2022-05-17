with payments as (

select
    id as customer_id
,   orderid as order_id
,   paymentmethod as payment_method
,   status as payment_status
,   amount / 100 as payment_amount
,   created as created_at
from  raw.stripe.payment

)

select * from payments