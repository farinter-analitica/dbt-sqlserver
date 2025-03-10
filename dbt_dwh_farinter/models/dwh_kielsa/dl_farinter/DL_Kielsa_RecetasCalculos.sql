{% set unique_key_list = ["Emp_Id","Receta_Id","Linea_Id"] %}
{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario", "periodo_unico/si"],
		materialized="table",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
        "{{ dwh_farinter_remove_incremental_temp_table() }}",
		"{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
        ]
	) 
}}

--AXELL PADILLA -- 20230726
WITH pre_calculos as 
(
	SELECT --top 100 
		RC.idpais
		, RC.suc_ingreso
		, RC.fecha_Receta
		, RC.identidad
		, RD.articulo_id
		, RD.articulo_nombre
		, RC.autoid AS receta_id
		, RD.numero_linea as linea_id
		, RD.cantidad_recetada
		, RD.dosis_cantidad
		, RD.periodo_tratamiento
		, RD.unidad_periodo
		, RD.duracion_tratamiento
		, RD.unidad_duracion
		, CAST( (CASE
										WHEN RD.unidad_duracion = 'Dias'
											THEN RD.duracion_tratamiento
										WHEN RD.unidad_duracion = 'Años'
											THEN ROUND(365.0 * RD.duracion_tratamiento, 2)
										WHEN RD.unidad_duracion = 'Horas'
											THEN ROUND(RD.duracion_tratamiento / 24.0, 2)
										WHEN RD.unidad_duracion = 'Meses'
											THEN ROUND(30.0 * RD.duracion_tratamiento, 2)
										WHEN RD.unidad_duracion = 'Minutos'
											THEN ROUND(RD.duracion_tratamiento / (24.0 * 60.0), 2)
										WHEN RD.unidad_duracion = 'Semanas'
											THEN ROUND(7 * RD.duracion_tratamiento, 2)
										WHEN RD.unidad_duracion IN ('Uso_Continuo','Uso_Permanente')
											THEN ROUND(365*99, 2) --99 años de esperanza de vida
										ELSE RD.periodo_tratamiento
									END) AS DECIMAL(16, 4)) duracion_tratamiento_dias
		, CASE								WHEN RD.unidad_duracion IN ('Uso_Continuo','Uso_Permanente')
											THEN 'Permanente' 
											ELSE 'Temporal' END AS [tipo_duracion]
		, CAST(CASE
										WHEN RD.unidad_periodo = 'Dias'
											THEN RD.periodo_tratamiento
										WHEN RD.unidad_periodo = 'Años'
											THEN ROUND(365.0 * RD.periodo_tratamiento, 5)
										WHEN RD.unidad_periodo = 'Horas'
											THEN ROUND(RD.periodo_tratamiento / 24.0, 5)
										WHEN RD.unidad_periodo = 'Meses'
											THEN ROUND(30.0 * RD.periodo_tratamiento , 5)
										WHEN RD.unidad_periodo = 'Minutos'
											THEN ROUND(RD.periodo_tratamiento / (24.0 * 60.0), 5)
										WHEN RD.unidad_periodo = 'Semanas'
											THEN ROUND(7.0 * RD.periodo_tratamiento, 5)
										ELSE RD.periodo_tratamiento
									END  AS DECIMAL(16, 4)) as periodo_tratamiento_dias
		, RD.forma_medicamento_id
		, RC.AnioMes_Id
		, RC.cod_medico
		, RC.factura
		, CAST(RD.dosis_cantidad / ISNULL(NULLIF(CASE
										WHEN RD.unidad_periodo = 'Dias'
											THEN RD.periodo_tratamiento
										WHEN RD.unidad_periodo = 'Años'
											THEN ROUND(365.0 * RD.periodo_tratamiento, 5)
										WHEN RD.unidad_periodo = 'Horas'
											THEN ROUND(RD.periodo_tratamiento / 24.0, 5)
										WHEN RD.unidad_periodo = 'Meses'
											THEN ROUND(30.0 * RD.periodo_tratamiento , 5)
										WHEN RD.unidad_periodo = 'Minutos'
											THEN ROUND(RD.periodo_tratamiento / (24.0 * 60.0), 5)
										WHEN RD.unidad_periodo = 'Semanas'
											THEN ROUND(7.0 * RD.periodo_tratamiento, 5)
										ELSE RD.periodo_tratamiento
									END,0),1) AS DECIMAL(16, 4)) consumo_diario
		,ISNULL(FACTP.Cantidad_Padre*AP.Presentación,AP.Presentación) as Comprado_Presentacion
		,FACTP.Factura_Fecha
		,AP.Presentación
		,AP.Uso
		,AP.Tipo
		,U.Usuario_Login as Usuario_Login
    	,ISNULL(U.Ultimo_Vendedor_Id_Asignado,0) AS Vendedor_Id

	FROM	DL_FARINTER.dbo.DL_Kielsa_RecetasDetalle RD --{{ source('DL_FARINTER', 'DL_Kielsa_RecetasDetalle') }}
	INNER JOIN DL_FARINTER.dbo.DL_Kielsa_RecetasCabecera RC --{{ source('DL_FARINTER', 'DL_Kielsa_RecetasCabecera') }}
		ON RD.autoid = RC.autoid 
		AND RD.AnioMes_Id = RC.AnioMes_Id
	LEFT JOIN BI_FARINTER.dbo.BI_Kielsa_Hecho_FacturaPosicion FACTP --{{ ref("BI_Kielsa_Hecho_FacturaPosicion") }}
		ON FACTP.Factura_Id = RC.factura
		AND FACTP.Articulo_Id = RD.articulo_id
		AND FACTP.Suc_Id = RC.suc_ingreso
		AND FACTP.Caja_Id = RC.caja_id
		AND FACTP.TipoDoc_Id = 1
		AND FACTP.Emp_Id = RC.idpais --OJOOOOO Pais no es lo mismo que empresa pero coincide mientras no se creen más empresas en el mismo pais.
		-- AND FACTP.AnioMes_Id >= YEAR(DATEADD(DAY, -365, GETDATE())) * 100 + MONTH(DATEADD(DAY, -365, GETDATE()))
		AND FACTP.Factura_Fecha <= DATEADD(DAY,+3,RC.fecha_receta) 
		AND FACTP.Factura_Fecha >= DATEADD(DAY,-3,RC.fecha_receta)
		AND FACTP.AnioMes_Id <=RC.AnioMes_Id
		AND FACTP.AnioMes_Id = YEAR(FACTP.Factura_Fecha) * 100 + MONTH(FACTP.Factura_Fecha)
        --AND RC.AnioMes_Id >= YEAR(DATEADD(MONTH,+3,FACTP.Factura_Fecha))*100 + MONTH(DATEADD(MONTH,+3,FACTP.Factura_Fecha))
        -- AND FACTP.AnioMes_Id =RC.AnioMes_Id
	LEFT JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_ArticuloPresentacion AP --{{ source('BI_FARINTER',"BI_Kielsa_Dim_ArticuloPresentacion") }}
		ON AP.Articulo_Id = RD.Articulo_Id
    LEFT JOIN {{ ref("BI_Kielsa_Dim_Usuario")}} as U
    ON RC.usuario_ingreso = U.Usuario_Nombre COLLATE DATABASE_DEFAULT
    AND U.Emp_Id = 1 AND U.Numero_Por_Nombre = 1
	WHERE RC.AnioMes_Id = YEAR(RC.fecha_receta) * 100 + MONTH(RC.fecha_receta) 
		AND RC.fecha_Receta >= DATEADD(YEAR, -3, GETDATE())
		)
,Calculos as
(
SELECT
	RD.idpais as Pais_Id
	, ISNULL(RD.idpais,0) AS Emp_Id
	, SUC.Sucursal_Id
	, SUC.Sucursal_Nombre
	, MED.nombre AS Nombre_Medico 
	, RD.Usuario_Login as Usuario_Login
	, RD.Vendedor_Id as Vendedor_Id
	, RD.identidad as Identidad
	, COALESCE(MOK.Monedero_Nombre,'N.E. (Revisar Id o Monedero)') AS Cliente_Nombre
	, RD.articulo_id as Articulo_Id
	, RD.articulo_nombre as Articulo_Nombre
	, RD.Presentación as Presentacion
	, RD.Tipo AS Presentacion_Tipo
    , CONCAT(RD.idpais,'-',SUC.Sucursal_Id) AS EmpSuc_Id
	, CONCAT(RD.idpais,'-',RD.Vendedor_Id) AS EmpVen_Id
    , CONCAT(RD.idpais,'-',RD.Identidad) AS EmpMon_Id
    , CONCAT(RD.idpais,'-',RD.Articulo_Id) AS EmpArt_Id
	, ISNULL(RD.receta_id,0) AS Receta_Id
    , CONCAT(RD.idpais,'-',RD.Receta_Id) AS EmpRec_Id
	, ISNULL(RD.linea_id,0) AS Linea_Id
	, CONCAT(RD.idpais,'-',RD.Receta_Id,'-',RD.Linea_Id) AS EmpRecLin_Id
	, RD.fecha_Receta AS Fecha_Receta
	, RD.Factura_Fecha as Fecha_Compra
	, RD.cantidad_recetada as Cantidad_Recetada
	, RD.dosis_cantidad as Dosis_Cantidad
	, RD.periodo_tratamiento as Periodo_Tratamiento
    , RD.periodo_tratamiento_dias as Periodo_Tratamiento_Dias
	, RD.unidad_periodo as Unidad_Periodo
	, RD.duracion_tratamiento as Duracion_Tratamiento
	, RD.duracion_tratamiento_dias as Duracion_Tratamiento_Dias
	, RD.unidad_duracion AS Unidad_Duracion
	, RD.tipo_duracion AS Tipo_Duracion
	, APF.Forma AS Forma_Medicamento
	--, RD.AnioMes_Id
	, RD.factura as Factura_Receta
	, RD.consumo_diario AS Consumo_Diario
	, RD.Comprado_Presentacion
	, (RD.consumo_diario*duracion_tratamiento_dias) AS Necesidad_Vida_Tratamiento
	, (RD.consumo_diario*duracion_tratamiento_dias) - RD.Comprado_Presentacion AS Faltante_Vida_Tratamiento
	, CASE WHEN ((RD.consumo_diario*duracion_tratamiento_dias) - RD.Comprado_Presentacion) > 0 THEN 'NO' ELSE 'SI' END AS Tratamiento_Completo
	, CASE WHEN ((RD.consumo_diario*duracion_tratamiento_dias) - RD.Comprado_Presentacion) > 0 THEN 0 ELSE 1 END AS Indicador_Tratamiento_Completo
	, RD.Uso as Uso_Medicamento	
	, CAST(ROUND((1.0*RD.Comprado_Presentacion) / NULLIF(consumo_diario, 0), 2) AS DECIMAL(16, 4)) AS Dias_Stock_Comprados
	, CAST(ROUND((1.0*RD.Comprado_Presentacion) / NULLIF(consumo_diario, 0), 2) AS DECIMAL(16, 4)) - DATEDIFF(DAY,Factura_Fecha,GETDATE()) AS Dias_Stock_Actual
	, CONCAT(
		[cantidad_recetada]
		, ' ('
		, APF.[Forma]
		, '), '
		, [dosis_cantidad]
		, ' dosis cada '
		, [periodo_tratamiento]
		, ' ('
		, [unidad_periodo]
		, '), durante '
		, [duracion_tratamiento]
		, ' ('
		, [unidad_duracion]
		, ')') AS Indicacion_Receta
	, DATEADD(DAY, 
		CASE 
			WHEN ((1.0*RD.Comprado_Presentacion) / NULLIF(consumo_diario, 0)) > 3650 THEN 3650 -- Cap at 10 years
			ELSE ((1.0*RD.Comprado_Presentacion) / NULLIF(consumo_diario, 0))
		END, 
		ISNULL(Factura_Fecha, fecha_Receta)) AS Contactar_El
	, ATR.Dia_Semana DiaSemana_Preferido
	, ATR.Hora Hora_Preferida
	, RD.AnioMes_Id
FROM  pre_calculos RD
LEFT JOIN DL_FARINTER.dbo.DL_ArticulosPresentacionesFormas_ExcelTemp APF  --{{ source('DL_FARINTER', 'DL_ArticulosPresentacionesFormas_ExcelTemp') }}
	ON APF.Id = RD.forma_medicamento_id
LEFT JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Sucursal SUC --{{ ref('BI_Kielsa_Dim_Sucursal') }}
	ON SUC.Sucursal_Id = RD.suc_ingreso AND SUC.Emp_Id = RD.idpais
LEFT JOIN DL_FARINTER.dbo.DL_Kielsa_RecetasMedicos MED --{{ source('DL_FARINTER', 'DL_Kielsa_RecetasMedicos') }}
	ON MED.autoid = RD.cod_medico
LEFT JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Monedero MOK --{{ ref('BI_Kielsa_Dim_Monedero') }}
	ON MOK.Monedero_Id = RD.identidad AND MOK.Emp_Id = RD.idpais
LEFT JOIN AN_FARINTER.dbo.AN_Cal_AtributosCliente_Kielsa ATR --{{ source('AN_FARINTER', 'AN_Cal_AtributosCliente_Kielsa') }}
	ON ATR.Cliente_Id = RD.identidad
	and ATR.Pais_Id = RD.idpais
-- WHERE DATEADD(DAY, (1.0*RD.Comprado_Presentacion) / NULLIF(consumo_diario, 0), Factura_Fecha) > CAST(DATEADD(DAY,-3,GETDATE()) AS DATE)
-- 	AND DATEADD(DAY, (1.0*RD.Comprado_Presentacion) / NULLIF(consumo_diario, 0), Factura_Fecha) < CAST(DATEADD(DAY,1,GETDATE()) AS DATE)


	-- SELECT TOP 100 * FROM BI_FARINTER.dbo.BI_Dim_Monedero_Kielsa MOK
	--SELECT  * FROM VDL_Kielsa_RecetasCalculos 
--
/*
SELECT Articulo_Id, Count(1) as Cantidad_Recetas, AVG(RC.Duracion_Tratamiento_Dias) DuracionTratamiento_DiasPromedio, AVG(RC.Periodo_Tratamiento_Dias) PeriodoTratamiento_DiasPromedio FROM VDL_Kielsa_RecetasCalculos RC 
GROUP BY Articulo_Id

SELECT  top 100 * FROM VDL_Kielsa_RecetasCalculos  WHERE Tipo_Duracion = 'Permanente' and Fecha_Receta >= '20240101'
--; Articulo_Id = '1110009008'

*/
)
SELECT * 
	, CASE
		WHEN CAST(GETDATE() AS DATE) <= Contactar_El
			THEN 'Si'
		ELSE 'No'
	END AS A_Tiempo
	, CASE
		WHEN CAST(GETDATE() AS DATE) <= Contactar_El
			THEN 1
		ELSE 0
	END AS Indicador_A_Tiempo
	, GETDATE() AS Fecha_Actualizado
FROM Calculos

