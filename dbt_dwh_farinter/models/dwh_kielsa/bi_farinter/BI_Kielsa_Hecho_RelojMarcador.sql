{% set unique_key_list = ["Ciclo_Id"] -%}

{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario"],
		materialized="incremental",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="append_new_columns",
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
        "{{ dwh_farinter_remove_incremental_temp_table() }}",
        "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
        "{{ dwh_farinter_create_dummy_data(unique_key=" ~ unique_key_list | tojson ~ ", is_incremental=0) }}"
        ]
	) 
}}

{%- if is_incremental() %}
	{%- set last_date = run_single_value_query_on_relation_and_return(query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -30*6, max(Fecha_Actualizado)), 112), '19000101')  from  """ ~ this, relation_not_found_value='19000101'|string)|string %}
{%- else %}
	{%- set last_date = '19000101' %}
{%- endif %}

-- Tabla de equivalencias de sospechosos:
-- -2: Hora de entrada posterior a hora de salida
--  0: Registro válido sin sospecha
--  1: Diferencia de horas mayor a 18 o sin hora de salida registrada
--  2: Diferencia de horas menor o igual a 1 hora
--  3: Marcación fuera del rango permitido (±2 horas) respecto al horario de la sucursal

-- Reglas de detección de sospechosos:
-- 1: Registros con más de 18 horas entre entrada y salida o sin hora de salida
-- 2: Registros con 1 hora o menos entre entrada y salida
-- 3: Registros con marcación fuera del rango permitido de la sucursal (±2 horas)
-- 4: TODO -> Validar contra existencia de facturas del empleado en el día
-- 5: TODO -> Validar contra cierres de caja del empleado
-- 6: TODO -> Validar contra registros de inicio de sesión del empleado

-- Reglas de corrección aplicadas:
-- 1: Para sospechosos tipo 1: Se mantiene entrada y se calcula salida usando promedio de horas
-- 2: Para sospechosos tipo 2/-2: Se corrige según proximidad a horario apertura/cierre
-- 3: Para sospechosos tipo 3: Se ajusta al límite permitido (±2 horas del horario)
-- 4: TODO -> Corrección basada en evidencia de facturas 
-- 5: TODO -> Corrección basada en cierres de caja
-- 6: TODO -> Corrección basada en evidencias para rangos vacios sobrantes de registros invalidos

WITH Marcador_Base AS (
    SELECT --TOP 2
        Ciclo, 
        M.Pais as Pais_Nombre,
        isnull(cast(P.Pais_ISO2 as nvarchar(2)),'ND')  AS Pais_ISO2,
        isnull(cast(P.Pais_Id as int),0) AS Pais_Id,
        --Empresa_Id no existe
        Id_Empleado, 
        HEntrada as fh_entrada, 
        HSalida as fh_salida,
        TRY_CAST(SUBSTRING(Computadora, 2, 3) AS INT)  -- Cambio: LTRIM es para texto no numeros
			AS sucursal_numero, 
		CAST(HEntrada AS DATE) AS fecha_entrada,
		CAST(HSalida AS DATE) AS fecha_salida,
		DATEPART(HOUR, HEntrada) AS hora_entrada,
		DATEPART(HOUR, HSalida) AS hora_salida,
		CASE 
		WHEN HEntrada>HSalida 
			THEN -2 
		WHEN DATEDIFF(HOUR, HEntrada, HSalida) <=1
			THEN 2  
		WHEN HSalida IS NULL OR DATEDIFF(HOUR, HEntrada, HSalida) > 18  
			THEN 1 ELSE NULL END AS sospecha_id,
			--La tabla tiene fecha-hora, no textos
        -- CAST(SUBSTRING(CAST(HEntrada AS NVARCHAR), 1, 11) AS DATE) AS fecha_entrada,
        -- CAST(SUBSTRING(CAST(HSalida AS NVARCHAR), 1, 11) AS DATE) AS fecha_salida
			-- Esto solo se puede hacer con la fecha real, no con los rangos
        ((DATEPART(WEEKDAY, HEntrada) + @@DATEFIRST - 2) % 7) + 1 AS dia_semana_iso_id
    --SELECT top 100 * --distinct Computadora
    FROM [DL_FARINTER].[dbo].[DL_Kielsa_Reloj_Marcador] M  -- {{ ref('DL_Kielsa_Reloj_Marcador') }}
    LEFT JOIN [BI_FARINTER].[dbo].BI_Dim_Pais P
        ON P.Pais_Nombre = M.Pais COLLATE DATABASE_DEFAULT
    WHERE Pais = 'Honduras'  --OJO: Agregar Pais_ISO2 para cada pais
        AND HEntrada IS NOT NULL
    {% if is_incremental() %}
        AND HEntrada > '{{ last_date }}'
    {% endif %}
        --AND HSalida IS NOT NULL : en estos casos se debe de agregar HSalida con la fecha de HEntrada y con la hora de h_cierre
        --AND DATEDIFF(DAY, CAST(HEntrada AS DATE), CAST(HSalida AS DATE)) <=2 -- 
        --AND DATEDIFF(HOUR, HEntrada, HSalida) >= 1
		--No nos interesan las computadoras con caracteres no numéricos
		--Es mala practica convertir tipos de datos en el WHERE, pero estamos construyendo
		--AND SUBSTRING(Computadora, 2, 3) NOT LIKE '%[^0-9]%' 
        --AND Id_Empleado = 'KKARGUETA' AND HEntrada >= '20250211'
        --AND DATEDIFF(DAY, HEntrada, HSalida) <= 3
)
-- SELECT top 100 * 
-- FROM Marcador_Base
-- where sospecha_id=2 
-- ORDER BY HEntrada DESC
,
--WITH
Horarios AS 
(
    --TODO: Deberian ser horarios planeados historicos
    SELECT *
		--select top 100 *
    FROM BI_FARINTER.dbo.BI_Kielsa_Dim_Sucursal_Horario_DiaSemana -- {{ ref('BI_Kielsa_Dim_Sucursal_Horario_DiaSemana') }}
	--where h_apertura > h_cierre
    WHERE h_apertura  IS NOT NULL
),
--SELECT * FROM Horarios
Marcador_Horario AS
(
	SELECT 
		Marcador_Base.*, 
		--Horarios.fh_apertura,
		--Horarios.fh_cierre,
		--Horarios.horas_cero_hasta_cierre,
        S.Sucursal_Id,
		Horarios.horas_abierto,
		DATEADD(HOUR, datepart(hour,Horarios.h_apertura), CAST(Marcador_Base.fecha_entrada AS DATETIME))  AS fh_apertura,
		DATEADD(HOUR, Horarios.horas_abierto, DATEADD(HOUR, datepart(hour,Horarios.h_apertura), CAST(Marcador_Base.fecha_entrada AS DATETIME)))  AS fh_cierre

	FROM Marcador_Base
	INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Sucursal S -- {{ ref('BI_Kielsa_Dim_Sucursal') }}
		ON Marcador_Base.sucursal_numero = S.Sucursal_Numero 
		AND S.Emp_Id = 1
	LEFT JOIN Horarios
        --TODO: Falta el pais en la tabla de horarios
		ON S.Sucursal_Id = Horarios.suc_id
		AND Marcador_Base.dia_semana_iso_id = Horarios.dia_semana_iso_id
),
Marcador_Horario_Sospechoso AS
(
	SELECT
        MH.Ciclo,
        MH.Pais_Nombre,
        MH.Pais_ISO2,
        MH.Pais_Id,
        MH.Sucursal_Id,
		MH.Id_Empleado,
		MH.sucursal_numero,
		MH.fecha_entrada,
		MH.fecha_salida,
		MH.dia_semana_iso_id,
		MH.fh_apertura,
		MH.fh_cierre,
		MH.fh_entrada,
		MH.fh_salida,
		MH.horas_abierto,
		ISNULL(MH.sospecha_id, 
		--marca entrada o salida mas menos 2 horas antes o despues de apertura o cierre
		CASE WHEN MH.fh_entrada < DATEADD(HOUR, -2, MH.fh_apertura) 
				OR MH.fh_salida > DATEADD(HOUR, 2, MH.fh_salida)
			THEN 3 ELSE 0 END
		) AS sospecha_id,
		--diferencia en horas contra horarios de cierre y apertura
		DATEDIFF(SECOND, MH.fh_apertura, MH.fh_entrada) / 3600.0 AS diferencia_horas_apertura,
		DATEDIFF(SECOND, MH.fh_salida, MH.fh_cierre) / 3600.0 AS diferencia_horas_cierre
	FROM Marcador_Horario MH
),
Horas_Promedio AS
(
    --TODO: Promedio movil
	SELECT 
		Id_Empleado,
		AVG(DATEDIFF(SECOND, fh_entrada, fh_salida) / 3600.0) AS horas_promedio
	FROM Marcador_Horario_Sospechoso MHS
	WHERE sospecha_id = 0
	GROUP BY Id_Empleado
),
--SELECT TOP 100 * FROM Horas_Promedio
Marcador_Corregido AS
(
	SELECT MH.*,
		CASE WHEN MH.sospecha_id IN (1) 
			--Agregar las horas para el cierre desde el dia del horario
			THEN MH.fh_entrada
			WHEN MH.sospecha_id IN (2,-2) 
				THEN 
					CASE 
					WHEN ABS(MH.diferencia_horas_cierre)<ABS(MH.diferencia_horas_apertura)
						THEN DATEADD(MINUTE, -ISNULL(HP.horas_promedio*60,8*60), MH.fh_salida)
					WHEN ABS(MH.diferencia_horas_cierre)>ABS(MH.diferencia_horas_apertura)
						THEN MH.fh_entrada
					ELSE NULL END
			WHEN MH.sospecha_id in (3)
				THEN CASE WHEN MH.fh_salida < DATEADD(HOUR,1,MH.fh_apertura) 
						THEN NULL
					WHEN MH.fh_entrada < DATEADD(HOUR,2,MH.fh_apertura)
						THEN DATEADD(HOUR,2,MH.fh_apertura)
					ELSE MH.fh_entrada END
			WHEN MH.sospecha_id in (0)
				THEN MH.fh_entrada
			ELSE NULL END AS fh_entrada_corregida,
		CASE 
		WHEN MH.sospecha_id IN (1)
			--Agregar las horas para el cierre desde el dia del horario
			THEN DATEADD(MINUTE, ISNULL(HP.horas_promedio*60,8*60), MH.fh_entrada)
		WHEN MH.sospecha_id IN (2,-2) 
			THEN 
				CASE 
				WHEN ABS(MH.diferencia_horas_cierre)<ABS(MH.diferencia_horas_apertura)
					THEN MH.fh_salida
				WHEN ABS(MH.diferencia_horas_cierre)>ABS(MH.diferencia_horas_apertura)
					THEN DATEADD(MINUTE, ISNULL(HP.horas_promedio*60,8*60), MH.fh_entrada)
				ELSE NULL END
		WHEN MH.sospecha_id in (3)
			THEN CASE WHEN MH.fh_salida < DATEADD(HOUR,1,MH.fh_apertura) 
					THEN NULL ELSE MH.fh_salida END
		WHEN MH.sospecha_id in (0)
			THEN MH.fh_salida
		ELSE NULL END AS fh_salida_corregida
	FROM Marcador_Horario_Sospechoso MH
	LEFT JOIN Horas_Promedio HP
		ON MH.Id_Empleado = HP.Id_Empleado
)
/*
SELECT TOP 1000 * 
FROM Marcador_Corregido
WHERE sospecha_id IN (3)
AND Id_Empleado='CKPUERTO' AND fecha_entrada >= '20230701' AND fecha_entrada < '20230710'*/
,Marcador_Final AS
(
	SELECT 
        isnull(MC.Ciclo,0) as Ciclo_Id,
        MC.Pais_Nombre,
        MC.Pais_ISO2,
        MC.Pais_Id,
		MC.Id_Empleado as Empleado_Marcador_Id,
        --TODO: Codigo de empleado_id/vendedor_id/sap
        NULL AS Empleado_Id,
		MC.sucursal_numero as Sucursal_Numero,
        MC.Sucursal_Id,
		MC.fecha_entrada as Fecha_Marcador_Entrada,
		MC.fecha_salida as Fecha_Marcador_Salida,
		MC.dia_semana_iso_id as Dia_Semana_Iso_Id,
		MC.fh_apertura as FH_Apertura_Sucursal ,
		MC.fh_cierre as FH_Cierre_Sucursal , 
		MC.fh_entrada as FH_Entrada_Marcador,
		MC.fh_salida as FH_Salida_Marcador,
		MC.horas_abierto as Horas_Abierto_Sucursal,
		MC.sospecha_id as Sospecha_Id,
        --TODO: Limitar a la hora real de operaciones, no de horarios planeados.
		CASE WHEN MC.fh_entrada_corregida < DATEADD(HOUR, -2, MC.fh_apertura) 
			THEN DATEADD(HOUR, -2, MC.fh_apertura)  ELSE MC.fh_entrada_corregida END AS FH_Entrada_Corregida,
		CASE WHEN MC.fh_salida_corregida > DATEADD(HOUR, 2, MC.fh_cierre)
			THEN DATEADD(HOUR, 2, MC.fh_cierre) ELSE MC.fh_salida_corregida END AS FH_Salida_Corregida
	FROM Marcador_Corregido MC
)
SELECT --TOP 100 
    *,
		--horas trabajadas
		DATEDIFF(SECOND, MF.fh_entrada_corregida, MF.fh_salida_corregida) / 3600.0 AS Horas_Trabajadas,
		CASE WHEN MF.fh_entrada_corregida IS NULL 
				OR DATEDIFF(SECOND, MF.fh_entrada_corregida, MF.fh_salida_corregida) / 3600.0 <2
			THEN 0 ELSE 1 END AS Es_Valido,
    GETDATE() AS Fecha_Actualizado
FROM Marcador_Final MF
