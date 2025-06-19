
-- Use the `ref` function to select from other models
{{
  config(
    materialized = 'view',
    meta={
            'dagster': {
                'group': 'dbt_first_model'
            }
        }
) }}

select *
from {{ ref('my_first_dbt_model') }}
where id = 1
