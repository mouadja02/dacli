{{ config(materialized='table') }}

select 
  trim(prd_id) as product_id,
  trim(upper(prd_key)) as product_key,
  initcap(prd_nm) as product_name,
  try_to_number(prd_cost) as product_cost,
  trim(upper(prd_line)) as product_line,
  prd_start_dt,
  prd_end_dt,
  _LOAD_TS,
  _FILE_NAME
from {{ source('bronze', 'prd_info') }}