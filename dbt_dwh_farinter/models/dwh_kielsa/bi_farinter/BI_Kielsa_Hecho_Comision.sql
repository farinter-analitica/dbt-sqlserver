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
        query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -7, max(Fecha_Actualizado)), 112), '19000101') as fecha_a from  """ ~ this, 
        relation_not_found_value='19000101'|string)|string %}
{%- else %}
    {%- set last_date = '20240101' %}
{%- endif %}

WITH ComisionBitacora AS (
    SELECT
        ISNULL(CB.Emp_Id, 0) AS Emp_Id,
        ISNULL(CB.Suc_Id, 0) AS Suc_Id,
        ISNULL(CB.Comision_Fecha, '19000101') AS Comision_Fecha,
        ISNULL(CB.Vendedor_Id, 0) AS Vendedor_Id,
        ISNULL(CB.Articulo_Id, 'X') AS Articulo_Id,
        ISNULL(CB.Comision_CantArticulo, 0.0) AS Comision_CantArticulo,
        ISNULL(CB.Comision_Monto, 0.0) AS Comision_Monto,
        CB.Fecha_Actualizado
    FROM {{ ref('DL_Kielsa_Comision_Bitacora') }} AS CB
    {% if is_incremental() %}
        WHERE CB.Fecha_Actualizado >= '{{ last_date }}'
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
        CD.Comision_Id,

        -- Campos del encabezado de comisiones
        CD.Comision_Nombre,
        CD.Comision_Fecha_Inicial,
        CD.Comision_Fecha_Final,
        CD.Detalle_Comision_Monto,
        --CD.Comision_Estado,

        -- Campos del detalle de comisiones
        CD.Detalle_Consecutivo,
        CD.EmpSucArtCom_Id,

        -- Campos de auditoría
        CAST(CB.Comision_CantArticulo * CB.Comision_Monto AS DECIMAL(18, 6)) AS Comision_Total,
        GETDATE() AS Fecha_Carga,
        CASE
            WHEN CB.Fecha_Actualizado > CD.Fecha_Actualizado THEN CB.Fecha_Actualizado
            ELSE CD.Fecha_Actualizado
        END AS Fecha_Actualizado
    FROM ComisionBitacora AS CB
    LEFT JOIN {{ ref('BI_Kielsa_Dim_Comision_Detalle') }} AS CD
        ON
            CB.Emp_Id = CD.Emp_Id
            AND CB.Suc_Id = CD.Suc_Id
            AND CB.Articulo_Id = CD.Articulo_Id
            AND CB.Comision_Fecha >= CD.Comision_Fecha_Inicial
            AND CB.Comision_Fecha <= CD.Comision_Fecha_Final
            AND CD.Mantener_Registro = 1  -- Solo registros válidos y únicos (sin duplicados)
)

SELECT
    S.*,
    ART.Articulo_Codigo_Padre AS Articulo_Padre_Id,
    CAST(CASE
        WHEN ART.Indicador_PadreHijo = 'H' THEN S.Comision_CantArticulo / ISNULL(ART.Factor_Denominador, 1.0)
        ELSE
            S.Comision_CantArticulo
    END AS DECIMAL(18, 6)) AS Cantidad_Padre,
    -- Campos para concatenación de IDs
    {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Suc_Id'], input_length=49, table_alias='S') }} AS EmpSuc_Id,
    {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Articulo_Id'], input_length=49, table_alias='S') }} AS EmpArt_Id,
    {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Vendedor_Id'], input_length=49, table_alias='S') }} AS EmpVen_Id

FROM Staging AS S
LEFT JOIN {{ ref ('BI_Kielsa_Dim_Articulo') }} AS ART
    ON S.Emp_Id = ART.Emp_Id AND S.Articulo_Id = ART.Articulo_Id
-- Ya no necesitamos WHERE S.rn = 1 porque el join ya filtra por Es_Ultimo_Rango = 1
