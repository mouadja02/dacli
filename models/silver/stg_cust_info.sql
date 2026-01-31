{{ config(materialized='table') }}

select 
  trim(cst_id) as customer_id,
  trim(upper(cst_key)) as customer_key,
  initcap(cst_firstname) as first_name,
  initcap(cst_lastname) as last_name,
  trim(upper(cst_marital_status)) as marital_status,
  trim(upper(cst_gndr)) as gender,
  cst_create_date,
  _LOAD_TS,
  _FILE_NAME
from {{ source('bronze', 'cust_info') }}