{{ 
    config(
        tags=["automation/periodo_mensual_inicio", "automation/periodo_por_hora"],
        materialized="view",
        post_hook=[
            "{{ dwh_farinter_remove_incremental_temp_table() }}",
        ]
    ) 
}}

-- Este modelo genera el stock por vencer a nivel de lote, material y almacén, filtrando por el mes en curso y excluyendo almacenes y casas según parámetros de planificación.
-- La lógica replica la transformación Power Query proporcionada, adaptada a SQL/dbt.

WITH CasasExcluir AS (
    -- Casas excluidas de planificación (Planificado = 'E')
    SELECT DISTINCT Gpo_Art_Id
    FROM {{ ref('DL_Planning_SAP_ParamSocGpo') }}
    WHERE Planificado = 'E'
),
AlmacenesExcluir AS (
    -- Almacenes excluidos de planificación (Planificado = 'N')
    SELECT DISTINCT Almacen_Id
    FROM {{ ref('DL_Edit_AlmacenFP_SAP') }}
    WHERE Planificado = 'N'
),
InventarioLotes AS (
    SELECT
        E.Sociedad_Id AS sociedad_id,
        E.Centro_Id AS centro_id,
        COALESCE(ALM.Almacen_Id_Mapeado, E.Almacen_Id) AS almacen_id,
        A.Material_Id AS material,
        EL.Lote_Id AS lote_id,
        L.Fecha_Caducidad AS fecha_cad,
        -- Suma de cantidades: Tránsito + Libre + Calidad
        ISNULL(EL.TransitoAlm_Cantidad,0) + ISNULL(EL.Libre_Cantidad,0) + ISNULL(EL.Calidad_Cantidad,0) AS ctd_stock
    FROM {{ source('DL_FARINTER', 'DL_SAP_Hecho_ExistenciasHist_Actual') }} E
    INNER JOIN {{ source('DL_FARINTER', 'DL_SAP_Hecho_ExistenciasLoteHist_Actual') }} EL
        ON E.Sociedad_Id = EL.Sociedad_Id
        AND E.Centro_Id = EL.Centro_Id
        AND E.Almacen_Id = EL.Almacen_Id
        AND E.Articulo_Id = EL.Articulo_Id
        AND E.AnioMes_Id = EL.AnioMes_Id
    INNER JOIN {{ source('BI_FARINTER', 'BI_Dim_Articulo_SAP') }} A
        ON E.Articulo_Id = A.Articulo_Id 
    INNER JOIN {{ source('BI_FARINTER', 'BI_SAP_Dim_Lote') }} L
        ON EL.Articulo_Id = L.Articulo_Id
        AND EL.Lote_Id = L.Lote_Id
    LEFT JOIN {{ ref('DL_Edit_AlmacenFP_SAP') }} ALM
        ON E.Almacen_Id = ALM.Almacen_Id --Cen-Alm
    -- Filtros principales
    WHERE
        -- Mes en curso
        E.AnioMes_Id = YEAR(GETDATE())*100 + MONTH(GETDATE())   
        -- Stock tipo 1 (Libre)
        AND E.Stock_Id = 1
        -- Excluir almacenes y casas según parámetros
        AND E.Almacen_Id NOT IN (SELECT Almacen_Id FROM AlmacenesExcluir)
        AND EL.Articulo_Id NOT IN (SELECT Gpo_Art_Id FROM CasasExcluir)
        -- Excluir lotes fuera de rango de vencimiento
        AND L.Fecha_Caducidad IS NOT NULL
        AND L.Fecha_Caducidad >= DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1)
        AND L.Fecha_Caducidad < DATEADD(month, 13, DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1))
        -- Excluir "DENTRO DE MAS DE UN AÑO" y "MESES ANTERIORES"
        -- (ya cubierto por el filtro anterior)
        -- Solo stock positivo
        AND (ISNULL(EL.TransitoAlm_Cantidad,0) + ISNULL(EL.Libre_Cantidad,0) + ISNULL(EL.Calidad_Cantidad,0)) > 0
)

SELECT
    sociedad_id,
    material,
    almacen_id,
    lote_id,
    CAST(MAX(fecha_cad) AS date) AS fecha_cad,
    SUM(ctd_stock) AS ctd_stock,
    CAST('{{ modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") }}' AS datetime) AS fecha_actualizado
FROM InventarioLotes
GROUP BY sociedad_id,
    material,
    almacen_id,
    lote_id