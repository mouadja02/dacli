{{ config(materialized='table', schema='GOLD') }}

select 
  sd.order_number,
  sd.product_key,
  sd.customer_id,
  sd.order_date,
  sd.ship_date,
  sd.due_date,
  sd.sales_amount,
  sd.quantity,
  sd.unit_price,
  dc.customer_key,
  dc.first_name,
  dc.last_name,
  dc.country_code,
  dp.product_name,
  dp.product_line,
  _LOAD_TS
from {{ ref('stg_sales_details') }} sd
left join {{ ref('dim_customer') }} dc on sd.customer_id = dc.customer_id
left join {{ ref('dim_product') }} dp on sd.product_key = dp.product_key