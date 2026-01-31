{{ config(materialized='table', schema='GOLD') }}

with cust_info as (
  select * from {{ ref('stg_cust_info') }}
),
cust_birth as (
  select * from {{ ref('stg_cust_az12') }}
),
cust_loc as (
  select * from {{ ref('stg_loc_a101') }}
)
select 
  coalesce(ci.customer_id, cb.customer_id, cl.customer_id) as customer_id,
  ci.customer_key,
  ci.first_name,
  ci.last_name,
  ci.marital_status,
  coalesce(ci.gender, cb.gender) as gender,
  cb.birth_date,
  cl.country_code,
  ci.cst_create_date as customer_create_date,
  max(_LOAD_TS) as load_ts
from cust_info ci
full outer join cust_birth cb on trim(ci.customer_id) = trim(cb.customer_id)
full outer join cust_loc cl on trim(coalesce(ci.customer_id, cb.customer_id)) = trim(cl.customer_id)
group by 1,2,3,4,5,6,7,8,9