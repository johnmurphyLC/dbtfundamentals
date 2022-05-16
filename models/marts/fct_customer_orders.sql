

-- Import CTEs

with
customers as (

    select * from {{ ref('stg_jaffle_shop_customers') }}
),

orders as (

    select *from {{ref('stg_jaffle_shop_orders')}}

),

payments as (

    select * from {{ref('stg_stripe_payments')}}


),

-- Logical CTEs

completed_payments as (

    select
        order_id,
        max(payment_created_at) as payment_finalized_date,
        sum(payment_amount) as total_amount_paid
    from payments
    where payment_status <> 'fail'
    group by 1
),

paid_orders as (

    select * from {{ref('int_orders')}}

)
,

-- Final CTE
final as (

select
    paid_orders.*,
    row_number() over (order by order_id) as transaction_seq,
    row_number() over (partition by customer_id order by order_id) as customer_sales_seq,
    -- nvsr = new vs returning customer
    case 
        when (
        rank() over (
            partition by customer_id
            order by order_placed_at, order_id
            ) = 1
        ) then 'new'
        else 'return' 
    end as nvsr,

    -- customer lifetime value
    sum(total_amount_paid) over (
        partition by customer_id
        order by order_placed_at
    ) as customer_lifetime_value,
    
    -- fdos = first day of sale
    first_value(order_placed_at) over (
        partition by customer_id
        order by order_placed_at
    ) as fdos
from
    paid_orders
)

-- Simple Select Statement
select * from final