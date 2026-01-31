{{ config(materialized='table', schema='SILVER') }}

with source_data as (
  select * from {{ source('bronze', 'cust_az12') }}
),
cleaned as (
  select 
    trim(upper(CID)) as customer_id,
    try_to_date(BDATE) as birth_date,
    trim(upper(GEN)) as gender,
    _LOAD_TS,
    _FILE_NAME
  from source_data
)
select * from cleaned