/*
  finance_customer_ltv
  --------------------
  One row per customer — lifetime value and revenue contribution.
  Used by finance and marketing for customer value analysis.
*/

with customers as (

    select * from {{ ref('dim_customers') }}
    where total_orders > 0

),

final as (

    select
        customer_id,
        full_name,
        email,
        country_code,
        customer_segment,
        customer_since,
        total_orders,
        lifetime_value,
        cancellation_rate_pct,
        first_order_at,
        most_recent_order_at,

        -- days between first and most recent order
        extract(
            day from most_recent_order_at - first_order_at
        )::int                                      as customer_lifespan_days,

        -- average revenue per order
        round(
            lifetime_value / nullif(total_orders, 0), 2
        )                                           as avg_revenue_per_order,

        -- revenue rank across all customers
        rank() over (
            order by lifetime_value desc
        )                                           as revenue_rank

    from customers

)

select * from final
order by lifetime_value desc