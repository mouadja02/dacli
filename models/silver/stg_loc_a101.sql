{{ config(materialized='table') }}

with source_data as (
  select * from {{ source('bronze', 'loc_a101') }}
),
cleaned as (
  select 
    trim(upper(CID)) as customer_id,
    trim(upper(CNTRY)) as country_code,
    _LOAD_TS,
    _FILE_NAME
  from source_data
)
select * from cleaned