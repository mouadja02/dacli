{{ config(materialized='table') }}

select 
  trim(upper(ID)) as category_id,
  trim(upper(CAT)) as category,
  trim(upper(SUBCAT)) as sub_category,
  trim(upper(MAINTENANCE)) as maintenance_flag,
  _LOAD_TS,
  _FILE_NAME
from {{ source('bronze', 'px_cat_g1v2') }}