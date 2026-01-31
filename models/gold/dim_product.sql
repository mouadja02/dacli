{{ config(materialized='table', schema='GOLD') }}

select 
  product_id,
  product_key,
  product_name,
  product_cost,
  product_line,
  prd_start_dt as product_start_date,
  prd_end_dt as product_end_date,
  _LOAD_TS
from {{ ref('stg_prd_info') }}