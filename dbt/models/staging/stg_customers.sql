/*
  stg_customers
  -------------
  Cleans and standardizes the raw customers table.
  Rules applied here:
    - snake_case column names throughout
    - email lowercased — source app allows mixed case on signup
    - country_code uppercased for consistency
    - nulls in country defaulted to 'UNKNOWN'
    - no joins, no aggregations — staging is cleaning only
*/

with source as (

    select * from {{ source('ecommerce', 'customers') }}

),

cleaned as (

    select
        id                                    as customer_id,
        lower(trim(email))                    as email,
        trim(full_name)                       as full_name,
        upper(coalesce(country, 'UNKNOWN'))   as country_code,
        created_at                            as created_at,
        updated_at                            as updated_at

    from source

)

select * from cleaned