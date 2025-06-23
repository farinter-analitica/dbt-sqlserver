{{ 
    config(
        tags=["automation/periodo_mensual_inicio", "automation/periodo_por_hora"],
        materialized="view",
        post_hook=[
            "{{ dwh_farinter_remove_incremental_temp_table() }}",
        ]
    ) 
}}

-- Este modelo genera información de existencias de materiales para la herramienta de nivelación, 
-- filtrando por el mes en curso y meses anteriores, excluyendo almacenes y casas según parámetros.
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
ExistenciasMateriales AS (
    SELECT
        E.Sociedad_Id,
        MAX(A.Casa_Id) AS Casa_Id,
        MAX(A.Casa_Nombre) AS Casa_Nombre,
        MAX(A.Articulo_Nombre) AS descripcion,
        MAX(A.Codigo_Barra) AS EANUPC,
        A.Material_Id AS material,
        MAX(A.Marca_Id) AS Marca_Id,
        MAX(A.Marca_Nombre) AS Marca_Nombre,
        MAX(A.Grupo_Externo_Id) as Principio_Id,
        MAX(A.Grupo_Externo_Nombre) AS Principio_Nombre,
        MAX(A.Sector_Id) AS Sector_Id,
        MAX(A.Sector_Nombre) AS Sector_Nombre,
        MAX(A.Temperatura_Tipo) AS temp_id,
        SUM(E.Libre_Cantidad) AS libre_cantidad,
        SUM(E.TransitoAlm_Cantidad + E.TransitoCentro_Cantidad) AS transitos_cantidad,
        SUM(E.StockTotal_Cantidad) AS stock_cantidad_total,
        SUM(E.StockTotal_Valor) AS stock_valor_total
    FROM {{ source('DL_FARINTER', 'DL_SAP_Hecho_ExistenciasHist_Actual') }} E
    INNER JOIN {{ source('BI_FARINTER', 'BI_Dim_Articulo_SAP') }} A
        ON E.Articulo_Id = A.Articulo_Id
    -- Filtros principales
    WHERE
        -- Mes en curso o 1-6 meses antes
        (E.AnioMes_Id = FORMAT(GETDATE(), 'yyyyMM') OR 
         E.AnioMes_Id BETWEEN FORMAT(DATEADD(month, -6, GETDATE()), 'yyyyMM') AND FORMAT(DATEADD(month, -1, GETDATE()), 'yyyyMM'))
        -- Stock tipo 1 (Libre)
        AND E.Stock_Id = 1
        -- Excluir almacenes y casas según parámetros
        AND E.Almacen_Id NOT IN (SELECT Almacen_Id FROM AlmacenesExcluir)
        AND A.Casa_Id NOT IN (SELECT Gpo_Art_Id FROM CasasExcluir)
    GROUP BY
        E.Sociedad_Id,
        A.Material_Id
)

SELECT
    Sociedad_Id AS sociedad_id,
    Casa_Id AS casa_id,
    Casa_Nombre AS casa_nombre,
    descripcion,
    EANUPC,
    material,
    Marca_Id AS marca_id,
    Marca_Nombre AS marca_nombre,
    Principio_Id AS principio_id,
    Principio_Nombre AS principio_nombre,
    Sector_Id AS sector_id,
    Sector_Nombre AS sector_nombre,
    temp_id,
    -- Cálculo del precio unitario (similar a la columna personalizada en Power Query)
    CASE 
        WHEN stock_cantidad_total > 0 THEN stock_valor_total / stock_cantidad_total 
        ELSE NULL 
    END AS precio_uni,
    CAST('{{ modules.datetime.datetime.now().strftime("%Y%m%d %H:%M:%S") }}' AS datetime) AS fecha_actualizado
FROM ExistenciasMateriales
