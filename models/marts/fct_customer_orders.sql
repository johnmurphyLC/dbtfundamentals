

-- Import CTEs

with
customers as (

    select * from {{ source('jaffle_shop', 'customers') }}
),

orders as (

    select * from {{ source('jaffle_shop', 'orders') }}

),

payments as (

    select * from {{ source('stripe', 'payment') }}
),

-- Logical CTEs

completed_payments as (
    select 
        orderid as order_id, 
        max(created) as payment_finalized_date, 
        sum(amount) / 100.0 as total_amount_paid
    from payments
    where status <> 'fail'
    group by 1
),

paid_orders as (
    select
        orders.id as order_id,
        orders.user_id as customer_id,
        orders.order_date as order_placed_at,
        orders.status as order_status,
        completed_payments.total_amount_paid,
        completed_payments.payment_finalized_date,
        customers.first_name as customer_first_name,
        customers.last_name as customer_last_name
    from orders
    left join completed_payments on orders.id = completed_payments.order_id
    left join customers 
            on orders.user_id = customers.id 
),

x as (

    select
        paid_orders.order_id,
        sum(p2.total_amount_paid) as customer_lifetime_value
    from paid_orders 
    left join 
        paid_orders as p2 on 
            paid_orders.customer_id = p2.customer_id and 
            paid_orders.order_id >= p2.order_id
    group by 1
    order by paid_orders.order_id
),
-- Final CTE
-- Simple Select Statement

customer_orders as (
    select 
        c.id as customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date, 
        count(orders.id) as number_of_orders
    from 
        customers as c
    left join 
        orders on orders.user_id = c.id 
    group by 1
    )

select
    paid_orders.*,
    row_number() over (order by paid_orders.order_id) as transaction_seq,
    row_number() over (partition by customer_id order by paid_orders.order_id) as customer_sales_seq,
    -- nvsr = new vs returning customer
    case 
        when (
        rank() over (
            partition by customer_id
            order by order_placed_at, paid_orders.order_id
            ) = 1
        ) then 'new'
        else 'return' 
    end as nvsr,
    x.customer_lifetime_value,
    -- fdos = first day of sale
    first_value(paid_orders.order_placed_at) over (
        partition by paid_orders.customer_id
        order by paid_orders.order_placed_at
    ) as fdos
from
    paid_orders
    left join
        customer_orders using (customer_id)
    left outer join 
        x on x.order_id = paid_orders.order_id
order by order_id