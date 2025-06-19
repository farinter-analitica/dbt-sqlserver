


{% snapshot my_dbt_scd_example_timestamp %}

    {{
        config(
          target_schema='dbt_scd',
          strategy='timestamp',
          unique_key='id',
          updated_at='fecha',
        )
    }}
--Sources cannot have identity columns
    SELECT
    id, fecha, prueba, int as new_int, 0 as another_int
    FROM  {{ source('ADM_FARINTER', 'tb_pruebas') }}

{% endsnapshot %}
