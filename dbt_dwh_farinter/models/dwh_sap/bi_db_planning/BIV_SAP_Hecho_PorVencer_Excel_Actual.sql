{% set unique_key_list = ["material", "centro", "almacen", "fecha_cad"] %}
{{
    config(
        tags=["automation/periodo_mensual_inicio","automation/periodo_por_hora"],
        materialized="view",
        post_hook=[
            "{{ dwh_farinter_remove_incremental_temp_table() }}",
        ]
    )
}}

-- Solo editable en dbt
-- Reemplaza la lógica de Power Query para obtener existencias por vencer (mes actual y próximos 12 meses).
-- Fuentes: DL_SAP_Hecho_ExistenciasHist_Actual, DL_SAP_Hecho_ExistenciasLoteHist_Actual, BI_Dim_Articulo_SAP, BI_SAP_Dim_Lote, BI_SAP_Dim_Almacen

WITH AlmacenesExcluir AS (
    -- Almacenes marcados como 'N' (No planificado) en parámetros.
    SELECT DISTINCT Almacen_Id
    FROM {{ ref('DL_Edit_AlmacenFP_SAP') }}
    WHERE Planificado = 'N'
),

InventarioPorVencer AS (
    SELECT
        E.Articulo_Id,
        EL.Lote_Id,
        E.Almacen_Id AS Almacen_Id_Original,
        COALESCE(E1.Almacen_Id_Mapeado, E.Almacen_Id) AS CenAlm_Id_Mapeado,
        (ISNULL(EL.Libre_Cantidad, 0) + ISNULL(EL.Calidad_Cantidad, 0) 
            + ISNULL(EL.TransitoAlm_Cantidad, 0) 
            + ISNULL(EL.TransitoCentro_Cantidad, 0)) AS stock_calculado
    FROM {{ source('DL_FARINTER', 'DL_SAP_Hecho_ExistenciasHist_Actual') }} E
    INNER JOIN {{ source('DL_FARINTER', 'DL_SAP_Hecho_ExistenciasLoteHist_Actual') }} EL
        ON E.Sociedad_Id = EL.Sociedad_Id
        AND E.Centro_Id = EL.Centro_Id
        AND E.Almacen_Id = EL.Almacen_Id
        AND E.Articulo_Id = EL.Articulo_Id
        AND E.AnioMes_Id = EL.AnioMes_Id
    LEFT JOIN {{ source('BI_FARINTER', 'BI_SAP_Dim_Lote') }} L
        ON EL.Articulo_Id = L.Articulo_Id
        AND EL.Lote_Id = L.Lote_Id
    LEFT JOIN {{ ref('DL_Edit_AlmacenFP_SAP') }} E1
        ON E.Almacen_Id = E1.Almacen_Id --Cen-Alm
    WHERE
        -- Mes actual
        E.AnioMes_Id = YEAR(GETDATE())*100 + MONTH(GETDATE())
        -- Solo stock libre (Stock_Id = 1)
        AND E.Stock_Id = 1
        -- Fecha de caducidad entre inicio de mes actual y fin de mes + 12 meses
        AND COALESCE(L.Fecha_Caducidad, '9999-12-31') >= DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1)
        AND COALESCE(L.Fecha_Caducidad, '9999-12-31') <= EOMONTH(DATEADD(MONTH, 12, GETDATE()))
        -- Excluir almacenes parametrizados
        AND E.Almacen_Id NOT IN (SELECT Almacen_Id FROM AlmacenesExcluir)
        -- Solo si hay stock
        AND (ISNULL(EL.Libre_Cantidad, 0) + ISNULL(EL.Calidad_Cantidad, 0) + ISNULL(EL.TransitoAlm_Cantidad, 0) + ISNULL(EL.TransitoCentro_Cantidad, 0)) > 0
)

SELECT
    A.Material_Id AS material,
    MAX(A.Articulo_Nombre) AS descripcion,
    CAST(COALESCE(L.Fecha_Caducidad, '9999-12-31') AS DATE) AS fecha_cad,
    ALM.Centro_Id AS centro,
    ALM.Almacen_Id AS almacen,
    SUM(I.stock_calculado) AS stock,
    CAST('{{ modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") }}' AS datetime) AS fecha_actualizado
FROM InventarioPorVencer I
INNER JOIN {{ source('BI_FARINTER', 'BI_Dim_Articulo_SAP') }} A
    ON I.Articulo_Id = A.Articulo_Id
INNER JOIN {{ ref('BI_SAP_Dim_Almacen') }} ALM
    ON I.CenAlm_Id_Mapeado = ALM.CenAlm_Id
LEFT JOIN {{ source('BI_FARINTER', 'BI_SAP_Dim_Lote') }} L
    ON I.Articulo_Id = L.Articulo_Id
    AND I.Lote_Id = L.Lote_Id
GROUP BY  A.Material_Id,
    CAST(COALESCE(L.Fecha_Caducidad, '9999-12-31') AS DATE),
    ALM.Centro_Id,
    ALM.Almacen_Id
--ORDER BY material ASC
