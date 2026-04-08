/*
  dim_products
  ------------
  One row per product enriched with sales performance metrics.
  Analysts use this to identify top sellers, slow movers, and stock risk.
*/

with products as (

    select * from {{ ref('stg_products') }}

),

order_items as (

    select * from {{ ref('stg_order_items') }}

),

product_sales as (

    select
        product_id,
        count(distinct order_id)                    as total_orders,
        sum(quantity)                               as total_units_sold,
        sum(line_total)                             as total_revenue,
        round(avg(unit_price), 2)                   as avg_selling_price,
        min(unit_price)                             as min_selling_price,
        max(unit_price)                             as max_selling_price

    from order_items
    group by product_id

),

final as (

    select
        p.product_id,
        p.product_name,
        p.category,
        p.price                                     as list_price,
        p.stock_qty,
        p.is_in_stock,
        p.updated_at,

        -- sales performance
        coalesce(s.total_orders, 0)                 as total_orders,
        coalesce(s.total_units_sold, 0)             as total_units_sold,
        coalesce(s.total_revenue, 0.00)             as total_revenue,
        coalesce(s.avg_selling_price, p.price)      as avg_selling_price,
        coalesce(s.min_selling_price, p.price)      as min_selling_price,
        coalesce(s.max_selling_price, p.price)      as max_selling_price,

        -- product classification
        case
            when coalesce(s.total_units_sold, 0) >= 100 then 'top_seller'
            when coalesce(s.total_units_sold, 0) >= 20  then 'mid_seller'
            when coalesce(s.total_units_sold, 0) >  0   then 'slow_mover'
            else 'no_sales'
        end                                         as sales_tier,

        -- stock risk flag
        case
            when p.stock_qty = 0                        then 'out_of_stock'
            when p.stock_qty < 10                       then 'low_stock'
            else 'adequate'
        end                                         as stock_status

    from products p
    left join product_sales s using (product_id)

)

select * from final