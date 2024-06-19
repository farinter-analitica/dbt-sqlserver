
-- Use the `ref` function to select from other models
{{
  config(
    materialized = 'view',
    meta={
            'dagster': {
                'group': 'dbt_second_model'
            }
        }
) }}

select *, 'prueba' as Column_Prueba
from {{ ref('my_second_dbt_model') }}
where id = 1
