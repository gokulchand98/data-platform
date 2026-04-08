/*
  dim_customers
  -------------
  One row per customer enriched with order behaviour metrics.
  Analysts use this to segment customers by value, activity, and geography.
*/

with customers as (

    select * from {{ ref('stg_customers') }}

),

orders as (

    select * from {{ ref('stg_orders') }}

),

customer_orders as (

    select
        customer_id,
        count(*)                                    as total_orders,
        sum(total_amount)                           as lifetime_value,
        min(ordered_at)                             as first_order_at,
        max(ordered_at)                             as most_recent_order_at,
        sum(
            case when status = 'cancelled'
            then 1 else 0 end
        )                                           as total_cancelled_orders,
        round(
            sum(case when status = 'cancelled' then 1 else 0 end)::numeric
            / nullif(count(*), 0) * 100, 2
        )                                           as cancellation_rate_pct

    from orders
    group by customer_id

),

final as (

    select
        c.customer_id,
        c.email,
        c.full_name,
        c.country_code,
        c.created_at                                as customer_since,

        -- order behaviour
        coalesce(o.total_orders, 0)                 as total_orders,
        coalesce(o.lifetime_value, 0.00)            as lifetime_value,
        coalesce(o.total_cancelled_orders, 0)       as total_cancelled_orders,
        coalesce(o.cancellation_rate_pct, 0.00)     as cancellation_rate_pct,
        o.first_order_at,
        o.most_recent_order_at,

        -- customer classification
        case
            when coalesce(o.lifetime_value, 0) >= 1000 then 'high_value'
            when coalesce(o.lifetime_value, 0) >= 300  then 'mid_value'
            when coalesce(o.lifetime_value, 0) >  0    then 'low_value'
            else 'no_orders'
        end                                         as customer_segment

    from customers c
    left join customer_orders o using (customer_id)

)

select * from final