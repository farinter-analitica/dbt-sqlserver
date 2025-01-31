{% set unique_key_list = ["Emp_Id","Receta_Id","Linea_Id"] %}

{{ 
    config(
		tags=["periodo/diario"],
        materialized="view",
    )
}}


SELECT
    RC.Emp_Id,
    RC.Receta_Id,
    RC.EmpRec_Id,
    RC.Linea_Id,
    -- Claves concatenadas según estándar
    RC.EmpSuc_Id,
    RC.EmpMon_Id,
    RC.EmpArt_Id,
    -- Métricas
    RC.Cantidad_Recetada,
    RC.Dosis_Cantidad,
    RC.Consumo_Diario,
    RC.Comprado_Presentacion,
    RC.Necesidad_Vida_Tratamiento,
    RC.Faltante_Vida_Tratamiento,
    RC.Indicador_Tratamiento_Completo,
    RC.Dias_Stock_Comprados,
    RC.Dias_Stock_Actual,
    RC.Indicador_A_Tiempo,
    -- Fechas
    RC.Fecha_Receta  AS Momento_Receta,
    CAST(RC.Fecha_Receta AS DATE) AS Fecha_Receta,
    DATEPART(HOUR, RC.Fecha_Receta) AS Hora_Receta,
    RC.Fecha_Compra,
    RC.Contactar_El,
    RC.AnioMes_Id,
    RC.Fecha_Actualizado AS Fecha_Actualizado
FROM {{ ref('DL_Kielsa_RecetasCalculos') }} RC
