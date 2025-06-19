
{% snapshot my_dbt_scd_example_check %}

    {{
        config(
          target_schema='dbt_scd',
          strategy='check',
          unique_key='id',
          check_cols=['prueba'],
        )
    }}
--Sources cannot have identity columns
    SELECT
    id, fecha, prueba, int
    FROM  {{ source('ADM_FARINTER', 'tb_pruebas') }}

{% endsnapshot %}

