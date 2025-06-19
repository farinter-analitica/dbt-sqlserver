{%- set unique_key_list = ["Comision_Id","Articulo_Id","Suc_Id","Emp_Id"] -%}

{{ 
    config(
        as_columnstore=true,
        tags=["periodo/diario", "automation/periodo_por_hora"],
        materialized="incremental",
        incremental_strategy="farinter_merge",
        unique_key=unique_key_list,
        on_schema_change="append_new_columns",
        merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
        merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
        post_hook=[
            "{{ dwh_farinter_remove_incremental_temp_table() }}",
            "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
            "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Fecha_Actualizado']) }}",
        ]
    ) 
}}

{%- if is_incremental() %}
    {%- set last_date = run_single_value_query_on_relation_and_return(
        query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -1, max(Fecha_Actualizado)), 112), '19000101')  from  """ ~ this, 
        relation_not_found_value='19000101'|string)|string %}
{%- else %}
    {%- set last_date = '19000101' %}
{%- endif %}

WITH Comision_Atributos AS (
    SELECT 
        CD.Emp_Id,
        CD.Comision_Id,
        CD.Suc_Id,
        CD.Articulo_Id,
        CD.Comision_Monto as Detalle_Comision_Monto,
        CD.Consecutivo as Detalle_Consecutivo,
        CE.Comision_Fecha_Inicial,
        CE.Comision_Fecha_Final,
        CE.Comision_Nombre,
        CE.Comision_Estado,
        (SELECT MAX(fecha) FROM (VALUES 
            (ISNULL(CE.Fecha_Actualizado, '19000101')),
            (ISNULL(CD.Fecha_Actualizado, '19000101'))
        ) AS MaxFecha(fecha)) AS Fecha_Actualizado
    FROM {{ref('DL_Kielsa_Comision_Encabezado')}} CE 
    INNER JOIN {{ref('DL_Kielsa_Comision_Detalle')}} CD 
        ON CE.Emp_Id = CD.Emp_Id 
        AND CE.Comision_Id = CD.Comision_Id
    {% if is_incremental() %}
    WHERE (SELECT MAX(fecha) FROM (VALUES 
            (ISNULL(CE.Fecha_Actualizado, '19000101')),
            (ISNULL(CD.Fecha_Actualizado, '19000101'))
        ) AS MaxFecha(fecha)) >= '{{ last_date }}'
    {% endif %}
)
SELECT *,
    
    -- Campos para concatenación de IDs
    {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Suc_Id', 'Articulo_Id', 'Comision_Id'], input_length=49, table_alias='')}} AS EmpSucArtCom_Id,
    {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Suc_Id', 'Comision_Id'], input_length=49, table_alias='')}} AS EmpSucCom_Id,
    {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Comision_Id'], input_length=49, table_alias='')}} AS EmpCom_Id,
    {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Suc_Id'], input_length=49, table_alias='')}} AS EmpSuc_Id,
    {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Articulo_Id'], input_length=49, table_alias='')}} AS EmpArt_Id

FROM Comision_Atributos
