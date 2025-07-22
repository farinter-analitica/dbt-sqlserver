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

WITH Vertebra AS (
    --Incluye todas las claves necesarias reales (con ventas)
    --individual_por_codigo
    SELECT
        Emp_Id,
        Articulo_Id,
        Suc_Id,
        Vendedor_Id,
        Fecha_Id
    FROM {{ ref('dlv_kielsa_incentivo_base_emp_suc_art_ven_fch') }}
    WHERE Fecha_Id >= CAST('{{ last_date }}' AS DATE)
    UNION ALL
    --unica_sucursal y multiple_sucursal
    SELECT
        Emp_Id,
        Articulo_Id,
        Suc_Id,
        NULL AS Vendedor_Id,
        Fecha_Id
    FROM {{ ref('dlv_kielsa_incentivo_base_emp_suc_art_fch') }}
    WHERE Fecha_Id >= CAST('{{ last_date }}' AS DATE)
),

ComisionAgrupadaVenArt AS (
    SELECT * FROM {{ ref('dlv_kielsa_stg_comision_emp_suc_art_ven_fch') }}
    WHERE Fecha_Id >= CAST('{{ last_date }}' AS DATE)
),

ComisionAgrupadaArt AS (
    SELECT * FROM {{ ref('dlv_kielsa_stg_comision_emp_suc_art_fch') }}
    WHERE Fecha_Id >= CAST('{{ last_date }}' AS DATE)
),

RegaliaAgrupadaVenArt AS (
    SELECT * FROM {{ ref('dlv_kielsa_stg_regalia_emp_suc_art_ven_fch') }}
    WHERE Fecha_Id >= CAST('{{ last_date }}' AS DATE)
),

RegaliaAgrupadaArt AS (
    SELECT * FROM {{ ref('dlv_kielsa_stg_regalia_emp_suc_art_fch') }}
    WHERE Fecha_Id >= CAST('{{ last_date }}' AS DATE)
),

FacturasAgrupadaVenArt AS (
    SELECT * FROM {{ ref('dlv_kielsa_stg_factura_articulo_emp_suc_ven_fch') }}
    WHERE Fecha_Id >= CAST('{{ last_date }}' AS DATE)
),

FacturasAgrupadaArt AS (
    SELECT * FROM {{ ref('dlv_kielsa_stg_factura_articulo_emp_suc_fch') }}
    WHERE Fecha_Id >= CAST('{{ last_date }}' AS DATE)
),

ReglaRegalia AS (
    SELECT * FROM {{ ref('dlv_kielsa_incentivo_regla_regalia') }}
),

ReglaCasa AS (
    SELECT * FROM {{ ref('dlv_kielsa_incentivo_regla_casa') }}
),

BaseIncentivos AS (
    SELECT BI.*
    FROM {{ ref('dlv_kielsa_incentivo_base_aplicacion') }} AS BI
    WHERE
        BI.fecha_desde <= CAST(GETDATE() AS DATE)
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
        ISNULL(ART.Articulo_Id, 'X') AS [Articulo_Id],
        ISNULL(ART.Casa_Id, 0) AS [Casa_Id],
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
        -- Posiblemente no funcione bien para más que el periodo incremental de 7 dias
        -- Esto controla cuando hay cambios de asignacion de sucursal
        CASE
            WHEN BI.Tipo_Aplicacion IN ('individual_por_codigo') THEN 1
            WHEN CAL.Fecha_Calendario < BI.Fecha_Validado THEN 1
            ELSE 0
        END AS Es_Valido,

        -- Incentivos por comisiones directas
        COALESCE(CAV.Comision_Total, CAA.Comision_Total, 0) AS Comision_Base_Total,
        COALESCE(CAV.Comision_CantArticulo, CAA.Comision_CantArticulo, 0) AS Comision_Cantidad_Articulo,
        COALESCE(CAV.Cantidad_Padre, CAA.Cantidad_Padre, 0) AS Comision_Cantidad_Padre,
        CAST(CASE
            WHEN BI.part_comision IS NOT NULL
                THEN COALESCE(CAV.Comision_Total, CAA.Comision_Total, 0) * BI.part_comision
            ELSE 0.0
        END AS DECIMAL(18, 6)) AS Comision_Ajustada,
        COALESCE(RAV.Cantidad_Padre, RAA.Cantidad_Padre, 0) AS Regalia_Cantidad_Padre,
        -- Lógica de incentivo de regalia
        CAST(CASE
            -- Si hay exclusión de marca propia y es marca propia, no aplica incentivo
            WHEN RR.excluir_marca_propia = 1 AND COALESCE(ART.Bit_Marca_Propia, 0) = 1 THEN 0.0
            -- Si aplica por part_regalia, multiplica por part_regalia
            WHEN RR.aplica_por_part = 1 THEN COALESCE(RC.valor_regalia, RR.valor_predeterminado, 0) * COALESCE(BI.part_regalia, 0.0)
            -- Si no aplica por part, solo el incentivo por casa o default
            ELSE COALESCE(RC.valor_regalia, RR.valor_predeterminado, 0)
        END AS DECIMAL(18, 6)) AS Regalia_Valor_Incentivo_Unitario,
        CAST(CASE
            WHEN RR.excluir_marca_propia = 1 AND COALESCE(ART.Bit_Marca_Propia, 0) = 1 THEN 0.0
            WHEN RR.aplica_por_part = 1 THEN COALESCE(RAV.Cantidad_Padre, RAA.Cantidad_Padre, 0) * (COALESCE(RC.valor_regalia, RR.valor_predeterminado, 0) * COALESCE(BI.part_regalia, 0.0))
            ELSE COALESCE(RAV.Cantidad_Padre, RAA.Cantidad_Padre, 0) * COALESCE(RC.valor_regalia, RR.valor_predeterminado, 0)
        END AS DECIMAL(18, 6)) AS Regalia_Valor_Incentivo_Total,
        -- Lógica de incentivo por facturas
        COALESCE(FAV.Cantidad_Padre, FAA.Cantidad_Padre, 0.0) AS Factura_Cantidad_Padre,
        COALESCE(FAV.Valor_Venta_Neta, FAA.Valor_Venta_Neta, 0.0) AS Factura_Valor_Venta_Neta,

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
    FROM Vertebra AS VERT
    INNER JOIN Calendario AS CAL
        ON VERT.Fecha_Id = CAL.Fecha_Calendario
    INNER JOIN BaseIncentivos AS BI
        ON
            VERT.Emp_Id = BI.Emp_Id
            AND CAL.Fecha_Calendario >= BI.fecha_desde
            AND (BI.fecha_hasta IS NULL OR CAL.Fecha_Calendario <= BI.fecha_hasta)
            AND (
                -- Para individual_por_codigo: debe coincidir vendedor exactamente en validez actual.
                -- A este nivel sera unico, siempre valido, si vendio con su codigo.
                -- No nos interesa si antes estaba asignado en otra sucursal.
                (
                    BI.tipo_aplicacion = 'individual_por_codigo'
                    AND BI.Fecha_Validado = CAST(GETDATE() AS DATE)
                    AND VERT.Vendedor_Id = BI.Vendedor_Id
                )
                OR
                -- Para unica_sucursal y multiple sucursal: debe coincidir empresa y sucursal, vendedor null.
                -- La validez hará que se vuelvan cero las sucursales que ya no estan activas.
                (
                    BI.tipo_aplicacion IN ('unica_sucursal', 'multiple_sucursal')
                    AND VERT.Vendedor_Id IS NULL
                    AND VERT.Suc_Id = BI.Suc_Id
                )
            )
    INNER JOIN {{ ref("BI_Kielsa_Dim_Articulo") }} AS ART
        ON
            VERT.Emp_Id = ART.Emp_Id
            AND VERT.Articulo_Id = ART.Articulo_Id
    LEFT JOIN {{ ref('BI_Kielsa_Dim_Vendedor') }} AS VEN
        ON
            VERT.Emp_Id = VEN.Emp_Id
            AND COALESCE(VERT.Vendedor_Id, BI.vendedor_id, 0) = VEN.Vendedor_Id
    LEFT JOIN ComisionAgrupadaVenArt AS CAV
        ON
            VERT.Emp_Id = CAV.Emp_Id
            AND VERT.Suc_Id = CAV.Suc_Id
            AND VERT.Articulo_Id = CAV.Articulo_Id
            AND VERT.Fecha_Id = CAV.Fecha_Id
            AND VERT.Vendedor_Id = CAV.Vendedor_Id
    LEFT JOIN ComisionAgrupadaArt AS CAA
        ON
            VERT.Emp_Id = CAA.Emp_Id
            AND VERT.Suc_Id = CAA.Suc_Id
            AND VERT.Articulo_Id = CAA.Articulo_Id
            AND VERT.Fecha_Id = CAA.Fecha_Id
            AND VERT.Vendedor_Id IS NULL
    LEFT JOIN RegaliaAgrupadaVenArt AS RAV
        ON
            VERT.Emp_Id = RAV.Emp_Id
            AND VERT.Suc_Id = RAV.Suc_Id
            AND VERT.Articulo_Id = RAV.Articulo_Id
            AND VERT.Fecha_Id = RAV.Fecha_Id
            AND VERT.Vendedor_Id = RAV.Vendedor_Id
    LEFT JOIN RegaliaAgrupadaArt AS RAA
        ON
            VERT.Emp_Id = RAA.Emp_Id
            AND VERT.Suc_Id = RAA.Suc_Id
            AND VERT.Articulo_Id = RAA.Articulo_Id
            AND VERT.Fecha_Id = RAA.Fecha_Id
            AND VERT.Vendedor_Id IS NULL
    LEFT JOIN FacturasAgrupadaVenArt AS FAV
        ON
            VERT.Emp_Id = FAV.Emp_Id
            AND VERT.Suc_Id = FAV.Suc_Id
            AND VERT.Articulo_Id = FAV.Articulo_Id
            AND VERT.Fecha_Id = FAV.Fecha_Id
            AND VERT.Vendedor_Id = FAV.Vendedor_Id
    LEFT JOIN FacturasAgrupadaArt AS FAA
        ON
            VERT.Emp_Id = FAA.Emp_Id
            AND VERT.Suc_Id = FAA.Suc_Id
            AND VERT.Articulo_Id = FAA.Articulo_Id
            AND VERT.Fecha_Id = FAA.Fecha_Id
            AND VERT.Vendedor_Id IS NULL
    LEFT JOIN ReglaRegalia AS RR
        ON BI.regla_id = RR.regla_id
    LEFT JOIN ReglaCasa AS RC
        ON
            BI.regla_id = RC.regla_id
            AND ART.Casa_Id = RC.casa_id_ld
    WHERE
        (
            CAV.Comision_Total IS NOT NULL OR CAA.Comision_Total IS NOT NULL
            OR RAV.Cantidad_Padre IS NOT NULL OR RAA.Cantidad_Padre IS NOT NULL
            OR FAV.Cantidad_Padre IS NOT NULL OR FAA.Cantidad_Padre IS NOT NULL
        )
)

SELECT --noqa: ST06
    ISNULL(Fecha_Id, '19000101') AS [Fecha_Id],
    ISNULL(Emp_Id, 0) AS [Emp_Id],
    ISNULL(Suc_Id, 0) AS [Suc_Id],
    ISNULL(Vendedor_Id, 0) AS [Vendedor_Id],
    ISNULL(Usuario_Id, 0) AS [Usuario_Id],
    ISNULL(Articulo_Id, 'X') AS [Articulo_Id],
    Casa_Id,
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
    EmpArt_Id,
    EmpVen_Id,
    EmpUsu_Id,
    EmpRol_Id,
    Fecha_Actualizado,
    -- Volvemos incentivo cero para sucursales ya no asignadas en el periodo incremental
    Comision_Base_Total,
    Comision_Cantidad_Articulo,
    Comision_Cantidad_Padre,
    Comision_Ajustada * Es_Valido AS Comision_Ajustada,
    Regalia_Cantidad_Padre,
    Regalia_Valor_Incentivo_Unitario * Es_Valido AS Regalia_Valor_Incentivo_Unitario,
    Regalia_Valor_Incentivo_Total * Es_Valido AS Regalia_Valor_Incentivo_Total,
    Factura_Cantidad_Padre,
    Factura_Valor_Venta_Neta
FROM Calculos
