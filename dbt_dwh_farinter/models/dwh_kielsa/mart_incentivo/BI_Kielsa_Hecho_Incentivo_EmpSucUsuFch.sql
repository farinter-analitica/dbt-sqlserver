{%- set unique_key_list = ["Fecha_Id","Usuario_Id","Vendedor_Id","Suc_Id","Emp_Id"] -%}

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
    {%- set last_date = '20250101' %}
{%- endif %}

WITH FacturasAgrupadaVen AS (
    SELECT * FROM {{ ref('dlv_kielsa_stg_factura_encabezado_emp_suc_ven_fch') }}
    WHERE Fecha_Id >= '{{ last_date }}'
),

FacturasAgrupadaSuc AS (
    SELECT * FROM {{ ref('dlv_kielsa_stg_factura_encabezado_emp_suc_fch') }}
    WHERE Fecha_Id >= '{{ last_date }}'
)

SELECT
    BI.regla_id AS [Regla_Id],
    BI.rol_id AS [Rol_Id],
    BI.rol_nombre AS [Rol_Nombre],
    BI.codigo_tipo AS [Codigo_Tipo],
    BI.tipo_aplicacion AS [Tipo_Aplicacion],
    BI.part_comision AS [Part_Comision],
    BI.part_regalia AS [Part_Regalia],
    BI.valor_por_receta_seguro AS [Valor_Por_Receta_Seguro],
    BI.EmpSuc_Id,
    BI.EmpVen_Id,
    BI.EmpUsu_Id,
    ISNULL(CAL.Fecha_Id, '19000101') AS [Fecha_Id],
    ISNULL(BI.emp_id, 0) AS [Emp_Id],
    ISNULL(BI.suc_id, 0) AS [Suc_Id],
    ISNULL(BI.vendedor_id, 0) AS [Vendedor_Id],
    ISNULL(BI.usuario_id, 0) AS [Usuario_Id],
    COALESCE(FAV.Cantidad_Facturas_Aseguradas, FAS.Cantidad_Facturas_Aseguradas, 0) AS Cantidad_Facturas_Aseguradas,
    COALESCE(FAV.Cantidad_Clientes_Asegurados, FAS.Cantidad_Clientes_Asegurados, 0) AS Cantidad_Clientes_Asegurados,
    CAST(CASE
        WHEN BI.valor_por_receta_seguro IS NOT NULL AND BI.valor_por_receta_seguro > 0
            THEN COALESCE(FAV.Cantidad_Facturas_Aseguradas, FAS.Cantidad_Facturas_Aseguradas, 0) * BI.valor_por_receta_seguro
        ELSE 0.0
    END AS DECIMAL(18, 6)) AS Incentivo_Recetas_Seguro,
    {% if is_incremental() -%}
        GETDATE()
    {% else -%}
        CAST(CAL.Fecha_Id AS DATETIME)
    {%- endif %} AS Fecha_Actualizado
FROM {{ ref('dlv_kielsa_incentivo_base_aplicacion') }} AS BI
INNER JOIN {{ ref('BI_Dim_Calendario_Dinamico') }} AS CAL
    ON
        BI.fecha_desde <= CAL.Fecha_Id
        AND (BI.fecha_hasta IS NULL OR BI.fecha_hasta >= CAL.Fecha_Id)
LEFT JOIN FacturasAgrupadaVen AS FAV
    ON
        BI.emp_id = FAV.Emp_Id
        AND BI.suc_id = FAV.Suc_Id
        AND CAL.Fecha_Id = FAV.Fecha_Id
        AND BI.vendedor_id = FAV.Vendedor_Id
        AND BI.tipo_aplicacion IN ('individual_por_codigo')
LEFT JOIN FacturasAgrupadaSuc AS FAS
    ON
        BI.emp_id = FAS.Emp_Id
        AND BI.suc_id = FAS.Suc_Id
        AND CAL.Fecha_Id = FAS.Fecha_Id
        AND BI.tipo_aplicacion IN ('unica_sucursal', 'multiple_sucursal')
WHERE
    (FAV.Cantidad_Facturas_Aseguradas IS NOT NULL OR FAS.Cantidad_Facturas_Aseguradas IS NOT NULL)
    AND CAL.Fecha_Id >= '{{ last_date }}'
    AND CAL.Fecha_Id <= CAST(GETDATE() AS DATE)
