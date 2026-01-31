{{ config(materialized='table', schema='GOLD') }}

select 
  category_id,
  category,
  sub_category,
  maintenance_flag,
  _LOAD_TS
from {{ ref('stg_px_cat_g1v2') }}