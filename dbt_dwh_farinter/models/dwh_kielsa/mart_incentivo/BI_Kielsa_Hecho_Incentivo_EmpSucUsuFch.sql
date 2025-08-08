{%- set unique_key_list = ["Fecha_Id", "Usuario_Id", "Suc_Id", "Emp_Id"] -%}

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
        ],
        pre_hook=[
            "
            IF NOT EXISTS (
                SELECT 1
                FROM {{ ref('dlv_kielsa_incentivo_base_aplicacion') }}
                WHERE Fecha_Validado = CAST(GETDATE() AS DATE)
            )
            BEGIN
                THROW 50000, 'No hay incentivos válidos para el día de hoy en dlv_kielsa_incentivo_base_aplicacion', 1;
            END
            "
        ]
    ) 
}}

{%- if is_incremental() %}
    {# Actualizamos el mes completo una vez al día para corregir re-asignaciones de sucursal mensuales #}
    {%- set fecha_actualizado = (run_single_value_query_on_relation_and_return(
        query="""select ISNULL(CONVERT(VARCHAR, max(Fecha_Actualizado), 112), NULL) from """ ~ this,
        relation_not_found_value=None)|string)[:8] %}
    {%- set hoy = modules.datetime.datetime.now().date() %}
    {%- if fecha_actualizado is not none and fecha_actualizado == hoy.strftime('%Y%m%d') %}
        {%- set last_date = hoy.strftime('%Y%m%d') %}
    {%- else %}
        {# Si no es hoy, obtener el mes de la fecha_actualizado #}
        {%- set fecha_base = modules.datetime.datetime.strptime(fecha_actualizado, '%Y%m%d') if fecha_actualizado is not none else hoy %}
        {%- set primer_dia_mes = fecha_base.replace(day=1) %}
        {%- set last_date = primer_dia_mes.strftime('%Y%m%d') %}
    {%- endif %}
{%- else %}
    {%- set last_date = '20250601' %}
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
        (
            BI.fecha_hasta >= CAST('{{ last_date }}' AS DATE)
            OR BI.fecha_hasta IS NULL
        )
        AND BI.fecha_desde <= CAST(GETDATE() AS DATE)
),

Calendario AS (
    SELECT Fecha_Calendario
    FROM {{ ref('BI_Dim_Calendario_Dinamico') }}
    WHERE Fecha_Calendario BETWEEN CAST('{{ last_date }}' AS DATE) AND CAST(GETDATE() AS DATE)
),

Calculos AS (
    SELECT --noqa: ST06
        ISNULL(VERT.Emp_Id, 0) AS [Emp_Id],
        ISNULL(VERT.Suc_Id, 0) AS [Suc_Id],
        COALESCE(VERT.Vendedor_Id, BI.vendedor_id, 0) AS [Vendedor_Id],
        COALESCE(BI.usuario_id, 0) AS [Usuario_Id],
        BI.regla_id AS [Regla_Id],
        BI.rol_id AS [Rol_Id],
        BI.rol_nombre AS [Rol_Nombre],
        BI.codigo_tipo AS [Codigo_Tipo],
        ISNULL(BI.tipo_aplicacion, 'individual_por_codigo') AS [Tipo_Aplicacion],
        BI.part_comision AS [Part_Comision],
        BI.part_regalia AS [Part_Regalia],
        BI.valor_por_receta_seguro AS [Valor_Por_Receta_Seguro],
        ISNULL(CAL.Fecha_Calendario, '19000101') AS [Fecha_Id],
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
                    AND BI.codigo_tipo = 'vendedor_id'
                )
                OR
                (
                    BI.tipo_aplicacion IN ('unica_sucursal', 'multiple_sucursal')
                    AND VERT.Vendedor_Id IS NULL
                    AND VERT.Suc_Id = BI.Suc_Id
                    AND BI.codigo_tipo = 'usuario_id'
                )
            )
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
),

CalculosFinales AS (
    SELECT
        C.*,
        ISNULL(CASE WHEN C.Vendedor_Id = 0 THEN U.Vendedor_Id ELSE C.Vendedor_Id END, 0) AS [Vendedor_Id_Final],
        ISNULL(CASE WHEN C.Usuario_Id = 0 THEN V.Usuario_Id ELSE C.Usuario_Id END, 0) AS [Usuario_Id_Final],
        ISNULL(CASE WHEN C.Rol_Id = 0 THEN V.Rol_Id_Mapeado ELSE C.Rol_Id END, 0) AS [Rol_Id_Final]
    FROM Calculos AS C
    LEFT JOIN {{ ref('BI_Kielsa_Dim_Usuario') }} AS U
        ON
            C.Emp_Id = U.Emp_Id
            AND C.Usuario_Id = U.Usuario_Id
    LEFT JOIN {{ ref('BI_Kielsa_Dim_Vendedor') }} AS V
        ON C.Emp_Id = V.Emp_Id AND C.Vendedor_Id = V.Vendedor_Id
),

UnicosPorClave AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY Fecha_Id, Usuario_Id_Final, Suc_Id, Emp_Id, Vendedor_Id_Final
                ORDER BY Es_Valido DESC, Fecha_Actualizado DESC
            ) AS rn
        FROM CalculosFinales
    ) AS t
    WHERE rn = 1
)

SELECT --noqa: ST06
    ISNULL(C.Fecha_Id, '19000101') AS [Fecha_Id],
    ISNULL(C.Emp_Id, 0) AS [Emp_Id],
    ISNULL(C.Suc_Id, 0) AS [Suc_Id],
    ISNULL(C.Vendedor_Id_Final, 0) AS [Vendedor_Id],
    ISNULL(C.Usuario_Id_Final, 0) AS [Usuario_Id],
    C.Regla_Id,
    C.Rol_Id_Final AS Rol_Id,
    C.Codigo_Tipo,
    C.Tipo_Aplicacion,
    C.Part_Comision,
    C.Part_Regalia,
    C.Valor_Por_Receta_Seguro,
    C.Es_Valido,
    {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Suc_Id'], input_length=29, table_alias='C') }} AS EmpSuc_Id,
    {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Vendedor_Id_Final'], input_length=29, table_alias='C') }} AS EmpVen_Id,
    {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Usuario_Id_Final'], input_length=29, table_alias='C') }} AS EmpUsu_Id,
    {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Rol_Id_Final'], input_length=29, table_alias='C') }} AS EmpRol_Id,
    C.Fecha_Actualizado,
    -- Volvemos incentivo cero para sucursales ya no asignadas en el periodo incremental
    C.Cantidad_Facturas_Aseguradas,
    C.Cantidad_Clientes_Asegurados,
    C.Incentivo_Recetas_Seguro * C.Es_Valido AS Incentivo_Recetas_Seguro
FROM UnicosPorClave AS C
--WHERE (ISNULL(C.Usuario_Id_Final, 0) > 0 OR C.Regla_Id IS NOT NULL)
--WHERE C.Fecha_Id = '20250601' and ISNULL(C.Usuario_Id_Final, 0)=0 --and c.suc_id=1 and c.emp_id=2
