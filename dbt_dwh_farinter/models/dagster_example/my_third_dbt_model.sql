
-- Use the `ref` function to select from other models

/*



*/
{{
  config(
    materialized = 'view'
) }}

select *, 'prueba' as Column_Prueba
from {{ ref('my_second_dbt_model') }}
where id = 1
