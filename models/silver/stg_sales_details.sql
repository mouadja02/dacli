{{ config(materialized='table') }}

select 
  trim(sls_ord_num) as order_number,
  trim(sls_prd_key) as product_key,
  trim(sls_cust_id) as customer_id,
  try_to_date(sls_order_dt, 'YYYYMMDD') as order_date,
  try_to_date(sls_ship_dt, 'YYYYMMDD') as ship_date,
  try_to_date(sls_due_dt, 'YYYYMMDD') as due_date,
  try_to_number(sls_sales) as sales_amount,
  try_to_number(sls_quantity) as quantity,
  try_to_number(sls_price) as unit_price,
  _LOAD_TS,
  _FILE_NAME
from {{ source('bronze', 'sales_details') }}