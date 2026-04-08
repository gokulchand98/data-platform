/*
  finance_daily_revenue
  ---------------------
  One row per day — aggregated revenue metrics for finance reporting.
  Excludes cancelled orders from revenue figures.
*/

with orders as (

    select * from {{ ref('fct_orders') }}
    where status != 'cancelled'

),

daily as (

    select
        order_date,
        order_year,
        order_month,
        currency,

        count(distinct order_id)                    as total_orders,
        count(distinct customer_id)                 as unique_customers,
        sum(total_amount)                           as gross_revenue,
        round(avg(total_amount), 2)                 as avg_order_value,
        sum(total_units)                            as total_units_sold,
        min(total_amount)                           as min_order_value,
        max(total_amount)                           as max_order_value

    from orders
    group by order_date, order_year, order_month, currency

)

select * from daily
order by order_date desc