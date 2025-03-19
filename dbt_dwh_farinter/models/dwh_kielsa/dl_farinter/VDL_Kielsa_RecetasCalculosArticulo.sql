{{ 
    config(
		tags=["periodo/diario", "periodo_unico/si"],
		materialized="view",
	) 
}}
--Vista 
--AXELL PADILLA -- 20230727 
SELECT
	RC.Pais_Id,
	RC.Emp_Id,
	RC.Articulo_Id
	-- forma receta
	, MAX(RC.Articulo_Nombre) Articulo_Nombre
	, MAX(RC.Forma_Medicamento) Forma_Medicamento
	-- tipo presentacion
	, MAX(RC.Presentacion_Tipo) Presentacion_Tipo
	-- presentacion
	, MAX(RC.Presentacion) Presentacion
	, COUNT(1) AS Cantidad_Recetas
	, AVG(RC.Periodo_Tratamiento_Dias) Periodo_Tratamiento_DiasPromedio
	, AVG(RC.Duracion_Tratamiento_Dias) Duracion_Tratamiento_DiasPromedio
	, AVG(RC.Cantidad_Recetada) Cantidad_Recetada_Promedio
	, AVG(RC.Dosis_Cantidad) Dosis_Cantidad_Promedio
	, AVG(RC.Consumo_Diario) Consumo_Diario_Promedio
	, STDEV(RC.Consumo_Diario) Consumo_Diario_DesvEst
FROM	{{ ref("DL_Kielsa_RecetasCalculos")}} RC
WHERE Fecha_Compra >= DATEADD(MONTH,-12,GETDATE())
GROUP BY RC.Pais_Id,RC.Emp_Id,RC.Articulo_Id
