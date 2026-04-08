/*
  stg_order_items
  ---------------
  Cleans and standardizes the raw order_items table.
  Rules applied here:
    - quantity coalesced to 0 on null
    - unit_price coalesced to 0.00 on null
    - line_total derived as quantity * unit_price
    - computed once here, reused in all downstream revenue models
*/

with source as (

    select * from {{ source('ecommerce', 'order_items') }}

),

cleaned as (

    select
        id                                            as order_item_id,
        order_id,
        product_id,
        coalesce(quantity,   0)                       as quantity,
        coalesce(unit_price, 0.00)                    as unit_price,

        -- derived — the single source of truth for line revenue
        coalesce(quantity, 0) * coalesce(unit_price, 0.00)  as line_total

    from source

)

select * from cleaned