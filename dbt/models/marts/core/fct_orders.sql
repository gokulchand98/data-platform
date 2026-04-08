/*
  fct_orders
  ----------
  One row per order — the central fact table of the platform.
  Joins orders with customers and items to produce a fully enriched grain.
  This is the most queried table in the platform.
*/

with orders as (

    select * from {{ ref('stg_orders') }}

),

customers as (

    select * from {{ ref('stg_customers') }}

),

order_items_agg as (

    -- aggregate items to order grain before joining
    select
        order_id,
        count(*)                                    as total_line_items,
        sum(quantity)                               as total_units,
        sum(line_total)                             as calculated_total

    from {{ ref('stg_order_items') }}
    group by order_id

),

final as (

    select
        o.order_id,
        o.customer_id,
        c.full_name                                 as customer_name,
        c.email                                     as customer_email,
        c.country_code,

        -- order details
        o.status,
        o.currency,
        o.total_amount,
        o.order_date,
        o.order_year,
        o.order_month,
        o.order_day_of_week,
        o.ordered_at,
        o.updated_at,

        -- item rollup
        coalesce(i.total_line_items, 0)             as total_line_items,
        coalesce(i.total_units, 0)                  as total_units,
        coalesce(i.calculated_total, 0.00)          as calculated_total,

        -- data quality flag — alerts if order total doesn't match item sum
        case
            when abs(o.total_amount - coalesce(i.calculated_total, 0)) > 0.01
            then true else false
        end                                         as has_amount_discrepancy,

        -- order value tier
        case
            when o.total_amount >= 500  then 'high'
            when o.total_amount >= 100  then 'medium'
            when o.total_amount >  0    then 'low'
            else 'zero'
        end                                         as order_value_tier

    from orders o
    left join customers      c using (customer_id)
    left join order_items_agg i using (order_id)

)

select * from final