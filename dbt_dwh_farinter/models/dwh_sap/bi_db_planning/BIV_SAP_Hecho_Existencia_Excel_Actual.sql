{% set unique_key_list = ["material","centro_id","almacen_id","sociedad_id"] %}
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
-- Reemplaza la lógica de Power Query para obtener existencias del mes actual.
-- Fuente principal: DL_SAP_Hecho_ExistenciasHist_Actual y DL_SAP_Hecho_ExistenciasLoteHist_Actual
-- Filtra por AnioMes_Id = mes actual (YYYYMM)

WITH CasasExcluir AS (
    -- Selecciona los Gpo_Art_Id (asumimos que equivalen a Casa_Id en este contexto) 
    -- marcados como 'E' (Excluir de planificación/reporte) en la tabla de parámetros de grupos.
    SELECT DISTINCT Gpo_Art_Id
    FROM {{ ref('DL_Planning_SAP_ParamSocGpo') }}
    WHERE Planificado = 'E'
),
AlmacenesExcluir AS (
    -- Selecciona los Almacen_Id marcados como 'N' (No planificado/Excluir de este reporte) 
    -- en la tabla de parámetros de Almacenes.
    SELECT DISTINCT Almacen_Id
    FROM {{ ref('DL_Edit_AlmacenFP_SAP') }} -- Referencia al modelo dbt que carga la tabla Almacenes
    WHERE Planificado = 'N'
),
CurrentMonthData AS (
    SELECT
        E.Sociedad_Id AS sociedad_id,
        MAX(A.Casa_Nombre) AS casa_nombre,
        MAX(A.Codigo_Barra) AS EANUPC,
        MAX(A.Casa_Id) AS casa_id,
        A.Material_Id AS material,
        MAX(A.Articulo_Nombre) AS descripcion,
        ALM.Centro_Id AS centro_id,
        ALM.Almacen_Id AS almacen_id, -- Usar el ID de almacén (sin el prefijo del centro)
        MAX(ALM.CenAlm_Id) as cen_alm_id,
        -- E.Almacen_Id AS almacen_id_original, -- ID Original mantenido por si se necesita (usado para exclusión)
        SUM(E.Libre_Cantidad) AS ctd_libre,
        SUM(E.Calidad_Cantidad) AS ctd_calidad,
        -- Suma de cantidades en tránsito (Almacén y Centro)
        SUM(ISNULL(E.TransitoAlm_Cantidad, 0) + ISNULL(E.TransitoCentro_Cantidad, 0)) AS ctd_transito,
        AVG(E.Precio_Costo) AS precio,
        -- Suma de cantidades de entrega (Cliente + Traslado)
        SUM(ISNULL(E.EntregCliente_Cantidad, 0) + ISNULL(E.EntregTraslado_Cantidad, 0)) AS ent_cliente,
        SUM(E.EntregCliente_Cantidad) AS ent_ventas, 
        SUM(E.EntregTraslado_Cantidad) AS ent_traslados, 
        SUM(E.Bloqueado_Cantidad) AS ctd_bloqueo
        -- Valor por defecto para gpo_cliente, requerido para consistencia aguas abajo
    FROM {{ source('DL_FARINTER', 'DL_SAP_Hecho_ExistenciasHist_Actual') }} E
    INNER JOIN {{ source('BI_FARINTER', 'BI_Dim_Articulo_SAP') }} A
        ON E.Articulo_Id = A.Articulo_Id 
    LEFT JOIN {{ ref('DL_Edit_AlmacenFP_SAP') }} E1
        ON E.Almacen_Id = E1.Almacen_Id --Cen-Alm
    INNER JOIN {{ ref('BI_SAP_Dim_Almacen') }} ALM
        ON COALESCE(E1.Almacen_Id_Mapeado, E.Almacen_Id) = ALM.CenAlm_Id

   -- Filtro para obtener solo datos del mes calendario actual
    WHERE E.AnioMes_Id = YEAR(GETDATE())*100 + MONTH(GETDATE())
      -- Aplicar exclusiones
      AND A.Casa_Id NOT IN (SELECT Gpo_Art_Id FROM CasasExcluir)
      AND E.Almacen_Id NOT IN (SELECT Almacen_Id FROM AlmacenesExcluir)
    GROUP BY 
    A.Material_Id,
    ALM.Centro_Id,
    ALM.Almacen_Id,
    E.Sociedad_Id
)

-- Selección final de columnas con los nombres y orden deseados para el Excel
SELECT
    sociedad_id,
    casa_nombre,
    EANUPC,
    casa_id,
    material,
    descripcion,
    centro_id,
    almacen_id, 
    cen_alm_id,
    ctd_libre,
    ctd_calidad,
    ctd_transito,
    precio,
    ent_cliente,
    ent_ventas,
    ent_traslados,
    ctd_bloqueo,
    CAST('{{ modules.datetime.datetime.now().strftime("%Y%m%d %H:%M:%S") }}' AS datetime) AS fecha_actualizado
FROM CurrentMonthData
