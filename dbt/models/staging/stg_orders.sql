/*
  stg_orders
  ----------
  Cleans and standardizes the raw orders table.
  Rules applied here:
    - status lowercased and trimmed — source app has inconsistent casing
    - total_amount nulls coalesced to 0.00
    - currency uppercased
    - columns renamed to be self-documenting
*/

with source as (

    select * from {{ source('ecommerce', 'orders') }}

),

cleaned as (

    select
        id                                        as order_id,
        customer_id,
        lower(trim(status))                       as status,
        upper(trim(currency))                     as currency,
        coalesce(total_amount, 0.00)              as total_amount,
        ordered_at,
        updated_at,

        -- derived columns added at staging layer
        date_trunc(ordered_at, DAY)                   as order_date,
        extract(year  from ordered_at)                as order_year,
        extract(month from ordered_at)                as order_month,
        extract(dayofweek from ordered_at)            as order_day_of_week

    from source

)

select * from cleaned