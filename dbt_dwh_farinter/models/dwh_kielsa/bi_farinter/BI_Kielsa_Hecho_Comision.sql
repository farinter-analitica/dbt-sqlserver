{%- set unique_key_list = ["Comision_Fecha","Vendedor_Id","Articulo_Id","Suc_Id","Emp_Id"] -%}

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
        query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -7, max(Fecha_Actualizado)), 112), '19000101')  from  """ ~ this, 
        relation_not_found_value='19000101'|string)|string %}
{%- else %}
    {%- set last_date = '20240101' %}
{%- endif %}

WITH ComisionBitacora AS (
    SELECT 
        CB.Emp_Id,
        CB.Suc_Id,
        CB.Comision_Fecha,
        CB.Vendedor_Id,
        CB.Articulo_Id,
        CB.Comision_CantArticulo,
        CB.Comision_Monto,
        CB.Fecha_Actualizado
    FROM {{ ref('DL_Kielsa_Comision_Bitacora') }} CB
    {% if is_incremental() %}
    WHERE CB.Comision_Fecha >= '{{ last_date }}'
    {% endif %}
),
Staging AS (
-- Consulta principal que une las tres tablas
SELECT 
    -- Campos de identificación
    CB.Emp_Id,
    CB.Suc_Id,
    CB.Comision_Fecha,
    CB.Vendedor_Id,
    CB.Articulo_Id,
    
    -- Campos de la bitácora de comisiones
    CB.Comision_CantArticulo,
    CB.Comision_Monto,
    CB.Comision_CantArticulo * CB.Comision_Monto AS Comision_Total,
    
    -- Campos del encabezado de comisiones
    CD.Comision_Id,
    CD.Comision_Nombre,
    CD.Comision_Fecha_Inicial,
    CD.Comision_Fecha_Final,
    --CD.Comision_Estado,
    
    -- Campos del detalle de comisiones
    CD.Detalle_Comision_Monto,
    CD.Detalle_Consecutivo,
    
    -- Campos de auditoría
    ROW_NUMBER() OVER (PARTITION BY CB.Comision_Fecha, CB.Vendedor_Id, CB.Articulo_Id, CB.Suc_Id , CB.Emp_Id
        ORDER BY CD.Comision_Fecha_Final DESC) AS rn,
    CD.EmpSucArtCom_Id,
    GETDATE() AS Fecha_Carga,
    (SELECT MAX(fecha) FROM (VALUES 
        (CB.Fecha_Actualizado),
        (CD.Fecha_Actualizado)
    ) AS MaxFecha(fecha)) AS Fecha_Actualizado
FROM ComisionBitacora CB
LEFT JOIN {{ ref('BI_Kielsa_Dim_Comision_Detalle') }} CD 
    ON CB.Emp_Id = CD.Emp_Id 
    AND CB.Comision_Fecha >= CD.Comision_Fecha_Inicial 
    AND CB.Comision_Fecha <= CD.Comision_Fecha_Final
    AND CB.Suc_Id = CD.Suc_Id 
    AND CB.Articulo_Id = CD.Articulo_Id
)
SELECT *,
    
    -- Campos para concatenación de IDs
    {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Suc_Id'], input_length=49, table_alias='')}} AS EmpSuc_Id,
    {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Articulo_Id'], input_length=49, table_alias='')}} AS EmpArt_Id,
    {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Vendedor_Id'], input_length=49, table_alias='')}} AS EmpVen_Id

FROM Staging
WHERE rn = 1