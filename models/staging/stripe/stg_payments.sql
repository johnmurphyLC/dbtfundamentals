with payments as (

select
    ID as customer_id
,   ORDERID as order_id
,   PAYMENTMETHOD as Payment_Method
,   STATUS as Payment_Status
,   Amount / 100 as Payment_Amount
,   Created as Created_At
from  RAW.STRIPE.PAYMENT

)

select * from payments