/*
  stg_products
  ------------
  Cleans and standardizes the raw products table.
  Rules applied here:
    - category lowercased and trimmed
    - price coalesced to 0.00 on null
    - stock_qty coalesced to 0 on null
    - is_in_stock derived boolean for convenience
*/

with source as (

    select * from {{ source('ecommerce', 'products') }}

),

cleaned as (

    select
        id                                        as product_id,
        trim(name)                                as product_name,
        lower(trim(category))                     as category,
        coalesce(price, 0.00)                     as price,
        coalesce(stock_qty, 0)                    as stock_qty,
        updated_at,

        -- derived
        case
            when coalesce(stock_qty, 0) > 0 then true
            else false
        end                                       as is_in_stock

    from source

)

select * from cleaned