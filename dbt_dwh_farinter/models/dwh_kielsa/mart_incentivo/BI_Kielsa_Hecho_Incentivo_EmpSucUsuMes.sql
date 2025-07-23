{%- set unique_key_list = ["Fecha_Id","Usuario_Id","Vendedor_Id","Suc_Id","Emp_Id"] -%}

{{ 
    config(
        as_columnstore=true,
        tags=["periodo/diario", "automation/periodo_por_hora", "detener_carga/si"],
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

WITH Vertebra AS (
    --Incluye todas las claves necesarias reales (con ventas)
    --individual_por_codigo
    SELECT
        Emp_Id,
        Suc_Id,
        Vendedor_Id,
        Fecha_Id
    FROM {{ ref('dlv_kielsa_incentivo_base_emp_suc_ven_fch') }}
    WHERE Fecha_Id >= CAST('{{ last_date }}' AS DATE)
    UNION ALL
    --unica_sucursal y multiple_sucursal
    SELECT
        Emp_Id,
        Suc_Id,
        NULL AS Vendedor_Id,
        Fecha_Id
    FROM {{ ref('dlv_kielsa_incentivo_base_emp_suc_fch') }}
    WHERE Fecha_Id >= CAST('{{ last_date }}' AS DATE)
),

FacturasAgrupadaVen AS (
    SELECT * FROM {{ ref('dlv_kielsa_stg_factura_encabezado_emp_suc_ven_fch') }}
    WHERE Fecha_Id >= CAST('{{ last_date }}' AS DATE)
),

FacturasAgrupadaSuc AS (
    SELECT * FROM {{ ref('dlv_kielsa_stg_factura_encabezado_emp_suc_fch') }}
    WHERE Fecha_Id >= CAST('{{ last_date }}' AS DATE)
),

BaseIncentivos AS (
    SELECT BI.*
    FROM {{ ref('dlv_kielsa_incentivo_base_aplicacion') }} AS BI
    WHERE
        BI.fecha_desde <= CAST('{{ last_date }}' AS DATE)
        AND (BI.fecha_hasta IS NULL OR BI.fecha_hasta >= CAST('{{ last_date }}' AS DATE))
),

Calendario AS (
    SELECT Fecha_Calendario
    FROM {{ ref('BI_Dim_Calendario_Dinamico') }}
    WHERE Fecha_Calendario BETWEEN CAST('{{ last_date }}' AS DATE) AND CAST(GETDATE() AS DATE)
),

Calculos AS (
    SELECT
        ISNULL(CAL.Fecha_Calendario, '19000101') AS [Fecha_Id],
        ISNULL(VERT.Emp_Id, 0) AS [Emp_Id],
        ISNULL(VERT.Suc_Id, 0) AS [Suc_Id],
        COALESCE(VERT.Vendedor_Id, BI.vendedor_id, 0) AS [Vendedor_Id],
        COALESCE(BI.usuario_id, VEN.Usuario_Id, 0) AS [Usuario_Id],
        BI.regla_id AS [Regla_Id],
        BI.rol_id AS [Rol_Id],
        BI.rol_nombre AS [Rol_Nombre],
        BI.codigo_tipo AS [Codigo_Tipo],
        BI.tipo_aplicacion AS [Tipo_Aplicacion],
        BI.part_comision AS [Part_Comision],
        BI.part_regalia AS [Part_Regalia],
        BI.valor_por_receta_seguro AS [Valor_Por_Receta_Seguro],
        -- Indicador de validez rezagada
        CASE
            WHEN BI.Tipo_Aplicacion IN ('individual_por_codigo') THEN 1
            WHEN CAL.Fecha_Calendario < BI.Fecha_Validado THEN 1
            ELSE 0
        END AS Es_Valido,

        -- Incentivos por aseguradoras
        COALESCE(FAV.Cantidad_Facturas_Aseguradas, FAS.Cantidad_Facturas_Aseguradas, 0) AS Cantidad_Facturas_Aseguradas,
        COALESCE(FAV.Cantidad_Clientes_Asegurados, FAS.Cantidad_Clientes_Asegurados, 0) AS Cantidad_Clientes_Asegurados,
        CAST(CASE
            WHEN BI.valor_por_receta_seguro IS NOT NULL AND BI.valor_por_receta_seguro > 0
                THEN COALESCE(FAV.Cantidad_Facturas_Aseguradas, FAS.Cantidad_Facturas_Aseguradas, 0) * BI.valor_por_receta_seguro
            ELSE 0.0
        END AS DECIMAL(18, 6)) AS Incentivo_Recetas_Seguro,
        BI.EmpSuc_Id,
        BI.EmpVen_Id,
        BI.EmpUsu_Id,
        BI.EmpRol_Id,
        {% if is_incremental() -%}
            GETDATE()
        {% else -%}
            CAST(CAL.Fecha_Calendario AS DATETIME)
        {%- endif %} AS Fecha_Actualizado
    FROM Vertebra AS VERT
    INNER JOIN Calendario AS CAL
        ON VERT.Fecha_Id = CAL.Fecha_Calendario
    INNER JOIN BaseIncentivos AS BI
        ON
            VERT.Emp_Id = BI.Emp_Id
            AND CAL.Fecha_Calendario >= BI.fecha_desde
            AND (BI.fecha_hasta IS NULL OR CAL.Fecha_Calendario <= BI.fecha_hasta)
            AND (
                (
                    BI.tipo_aplicacion = 'individual_por_codigo'
                    AND BI.Fecha_Validado = CAST(GETDATE() AS DATE)
                    AND VERT.Vendedor_Id = BI.Vendedor_Id
                )
                OR
                (
                    BI.tipo_aplicacion IN ('unica_sucursal', 'multiple_sucursal')
                    AND VERT.Vendedor_Id IS NULL
                    AND VERT.Suc_Id = BI.Suc_Id
                )
            )
    LEFT JOIN {{ ref('BI_Kielsa_Dim_Vendedor') }} AS VEN
        ON
            VERT.Emp_Id = VEN.Emp_Id
            AND COALESCE(VERT.Vendedor_Id, BI.vendedor_id, 0) = VEN.Vendedor_Id
    LEFT JOIN FacturasAgrupadaVen AS FAV
        ON
            VERT.Emp_Id = FAV.Emp_Id
            AND VERT.Suc_Id = FAV.Suc_Id
            AND VERT.Fecha_Id = FAV.Fecha_Id
            AND VERT.Vendedor_Id = FAV.Vendedor_Id
    LEFT JOIN FacturasAgrupadaSuc AS FAS
        ON
            VERT.Emp_Id = FAS.Emp_Id
            AND VERT.Suc_Id = FAS.Suc_Id
            AND VERT.Fecha_Id = FAS.Fecha_Id
            AND VERT.Vendedor_Id IS NULL
    WHERE
        (FAV.Cantidad_Facturas_Aseguradas IS NOT NULL OR FAS.Cantidad_Facturas_Aseguradas IS NOT NULL)
)

SELECT --noqa: ST06
    ISNULL(Fecha_Id, '19000101') AS [Fecha_Id],
    ISNULL(Emp_Id, 0) AS [Emp_Id],
    ISNULL(Suc_Id, 0) AS [Suc_Id],
    ISNULL(Vendedor_Id, 0) AS [Vendedor_Id],
    ISNULL(Usuario_Id, 0) AS [Usuario_Id],
    Regla_Id,
    Rol_Id,
    Rol_Nombre,
    Codigo_Tipo,
    Tipo_Aplicacion,
    Part_Comision,
    Part_Regalia,
    Valor_Por_Receta_Seguro,
    Es_Valido,
    EmpSuc_Id,
    EmpVen_Id,
    EmpUsu_Id,
    EmpRol_Id,
    Fecha_Actualizado,
    -- Volvemos incentivo cero para sucursales ya no asignadas en el periodo incremental
    Cantidad_Facturas_Aseguradas,
    Cantidad_Clientes_Asegurados,
    Incentivo_Recetas_Seguro * Es_Valido AS Incentivo_Recetas_Seguro
FROM Calculos
