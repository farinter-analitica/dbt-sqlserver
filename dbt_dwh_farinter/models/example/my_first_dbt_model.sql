
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
    This will override dbt_project.yml specifications
    {{ 
    config(materialized='view' ,
    meta={
            'dagster': {
                'group': 'dbt_first_model'
            }
        }
) }}

*/

{{ 
    config(materialized='view' 
) }}

with source_data as (

    select 1 as id
    union all
    SELECT TOP (1000) [Empleado_Id]
    FROM [DL_FARINTER].[dbo].[DL_Kielsa_Empleado]
    WHERE [Emp_Id]=1

)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

 where id is not null
