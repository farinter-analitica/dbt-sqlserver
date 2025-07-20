{%- set unique_key_list = ["Fecha_Id","Usuario_Id","Vendedor_Id","Articulo_Id","Suc_Id","Emp_Id"] -%}

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

WITH ComisionAgrupadaVenArt AS (
    SELECT * FROM {{ ref('dlv_kielsa_stg_comision_emp_suc_art_ven_fch') }}
    WHERE Fecha_Id >= '{{ last_date }}'
),

ComisionAgrupadaArt AS (
    SELECT * FROM {{ ref('dlv_kielsa_stg_comision_emp_suc_art_fch') }}
    WHERE Fecha_Id >= '{{ last_date }}'
)

SELECT
    ISNULL(CAL.Fecha_Calendario, '19000101') AS [Fecha_Id],
    ISNULL(BI.emp_id, 0) AS [Emp_Id],
    ISNULL(BI.suc_id, 0) AS [Suc_Id],
    ISNULL(ART.Articulo_Id, 'X') AS [Articulo_Id],
    ISNULL(ART.Casa_Id, 0) AS [Casa_Id],
    ISNULL(BI.vendedor_id, 0) AS [Vendedor_Id],
    ISNULL(BI.usuario_id, 0) AS [Usuario_Id],
    BI.regla_id AS [Regla_Id],
    BI.rol_id AS [Rol_Id],
    BI.rol_nombre AS [Rol_Nombre],
    BI.codigo_tipo AS [Codigo_Tipo],
    BI.tipo_aplicacion AS [Tipo_Aplicacion],
    BI.part_comision AS [Part_Comision],
    BI.part_regalia AS [Part_Regalia],
    BI.valor_por_receta_seguro AS [Valor_Por_Receta_Seguro],
    COALESCE(CAV.Comision_Total, CAA.Comision_Total, 0) AS Comision_Base_Total,
    COALESCE(CAV.Comision_CantArticulo, CAA.Comision_CantArticulo, 0) AS Comision_Cantidad_Articulo,
    COALESCE(CAV.Cantidad_Padre, CAA.Cantidad_Padre, 0) AS Comision_Cantidad_Padre,
    CAST(CASE
        WHEN BI.part_comision IS NOT NULL
            THEN COALESCE(CAV.Comision_Total, CAA.Comision_Total, 0) * BI.part_comision
        ELSE 0.0
    END AS DECIMAL(18, 6)) AS Comision_Ajustada,
    BI.EmpSuc_Id,
    {{ dwh_farinter_concat_key_columns(columns=['emp_id', 'articulo_id'], input_length=49, table_alias='ART') }} AS EmpArt_Id,
    BI.EmpVen_Id,
    BI.EmpUsu_Id,
    BI.EmpRol_Id,
    {% if is_incremental() -%} 
        GETDATE()
    {% else -%} 
        CAST(CAL.Fecha_Calendario AS DATETIME)
    {%- endif %} AS Fecha_Actualizado
FROM {{ ref('dlv_kielsa_incentivo_base_aplicacion') }} AS BI
INNER JOIN {{ ref('BI_Dim_Calendario_Dinamico') }} AS CAL
    ON
        BI.fecha_desde <= CAL.Fecha_Calendario
        AND (BI.fecha_hasta IS NULL OR BI.fecha_hasta >= CAL.Fecha_Calendario)

INNER JOIN {{ ref("BI_Kielsa_Dim_Articulo") }} AS ART
    ON BI.Emp_Id = ART.Emp_Id
LEFT JOIN ComisionAgrupadaVenArt AS CAV
    ON
        BI.emp_id = CAV.Emp_Id
        AND BI.suc_id = CAV.Suc_Id
        AND ART.Articulo_Id = CAV.Articulo_Id
        AND CAL.Fecha_Calendario = CAV.Fecha_Id
        AND BI.vendedor_id = CAV.Vendedor_Id
        AND BI.tipo_aplicacion IN ('individual_por_codigo')
LEFT JOIN ComisionAgrupadaArt AS CAA
    ON
        BI.emp_id = CAA.Emp_Id
        AND BI.suc_id = CAA.Suc_Id
        AND ART.Articulo_Id = CAA.Articulo_Id
        AND CAL.Fecha_Calendario = CAA.Fecha_Id
        AND BI.tipo_aplicacion IN ('unica_sucursal', 'multiple_sucursal')
WHERE
    (CAV.Comision_Total IS NOT NULL OR CAA.Comision_Total IS NOT NULL)
    AND CAL.Fecha_Calendario >= '{{ last_date }}'
    AND CAL.Fecha_Calendario <= CAST(GETDATE() AS DATE)
