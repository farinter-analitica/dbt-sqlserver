
{%- set unique_key_list = ["id"] -%}
{{ 
    config(
		tags=["periodo/diario"],
		materialized="view",
) }}

WITH
Datos1 AS
	(SELECT
		*, MIN(A.Fecha) OVER (PARTITION BY A.TarjetaKC_Id) AS Inicial
	FROM
		(SELECT
			1 AS Transaccion_Id
			, A.TarjetaKC_Id COLLATE DATABASE_DEFAULT AS TarjetaKC_Id
			, A.FRegistro AS Fecha
			, A.CodPlanKielsaClinica COLLATE DATABASE_DEFAULT AS CodPlanKielsaClinica
			, CASE
				WHEN C.Ingreso
					BETWEEN DATEADD(minute, -3, A.FRegistro) AND DATEADD(minute, 3, A.FRegistro)
					THEN 'automatico'
				ELSE 'mecanico'
			END AS Tipo_Ingreso
			, A.Origen
			, 'suscripcion' AS Tipo_Documento
			, A.Sucursal_Registro
			, A.Usuario_Registro COLLATE DATABASE_DEFAULT AS Usuario_Registro
			, A.TipoPlan
			, 'Nuevo' AS Tipo_Registro
			, 1 AS Transac_No
		FROM	[DL_FARINTER].[dbo].[DL_Kielsa_KPP_Suscripcion] AS A -- {{ source ("DL_FARINTER","DL_Kielsa_KPP_Suscripcion") }}
		INNER JOIN [DL_FARINTER].[dbo].[DL_Kielsa_Monedero] AS C
			ON A.TarjetaKC_Id = C.MonederoTarj_Id_Original COLLATE DATABASE_DEFAULT
		WHERE C.Emp_Id = 1 and A.Suscripcion_Id < 26173	--- '26173' es el id de suscripci�n del primer monedero registrado en la tbl LogMovimientoSuscripcion
											--ORDER BY A.FRegistro ASC
		UNION ALL
		SELECT	*
		FROM
			(SELECT
				A.Transaccion_Id
				, A.TarjetaKC_Id
				, A.Fecha
				, ISNULL(B.CodPlanKielsaClinica, 'No definido') AS CodPlanKielsaClinica
				, A.Tipo_Ingreso
				, 0 AS Origen
				, 'No definido' AS Tipo_Documento
				, ISNULL(B.Sucursal_Registro, 0) AS Sucursal_Registro
				, ISNULL(B.Usuario_Registro, 'No definido') AS Usuario_Registro
				, ISNULL(B.TipoPlan, 0) AS TipoPlan
				, 'Existente' AS Tipo_Registro
				, ROW_NUMBER() OVER (PARTITION BY A.TarjetaKC_Id ORDER BY A.Fecha) AS Transac_No
			FROM
				(SELECT
					A.Transaccion_Id
					, A.TarjetaKC_Id
					, CASE
						WHEN B.Ingreso
							BETWEEN DATEADD(minute, -3, A.Fecha) AND DATEADD(minute, 3, A.Fecha)
							THEN 'automatico'
						ELSE 'mecanico'
					END AS Tipo_Ingreso
					, CONVERT(DATE, A.Fecha) AS Fecha
					, ROW_NUMBER() OVER (PARTITION BY A.TarjetaKC_Id, CONVERT(DATE, A.Fecha)ORDER BY A.Fecha) AS Veces
				FROM	[REPLICASLD].[KPP_DB].[dbo].[Transacciones] A
				INNER JOIN [DL_FARINTER].[dbo].[DL_Kielsa_Monedero] AS B
					ON A.TarjetaKC_Id = B.MonederoTarj_Id_Original COLLATE DATABASE_DEFAULT
				WHERE B.Emp_Id = 1 and A.Transaccion_Id < 30350	--- '30350' es el id de transacci�n del primer monedero registrado en la tbl LogMovimientoSuscripcion
													--ORDER BY Fecha ASC
			) AS A
			LEFT JOIN
				(SELECT
					TarjetaKC_Id
					, Usuario_Registro
					, Sucursal_Registro
					, TipoPlan
					, CONVERT(DATE, FRegistro) AS Fecha
					, CodPlanKielsaClinica
					, ROW_NUMBER() OVER (PARTITION BY TarjetaKC_Id, CONVERT(DATE, FRegistro)ORDER BY FRegistro) AS Veces
				FROM	[DL_FARINTER].[dbo].[DL_Kielsa_KPP_LogSuscripcion] -- {{ ref ("DL_Kielsa_KPP_LogSuscripcion") }}
				WHERE (ErrorMessage NOT LIKE '%Error%' AND ErrorMessage NOT LIKE '%time%') AND id < 24905 --- '24905' es el id de LogSuscripcion del primer monedero registrado en la tbl LogMovimientoSuscripcion
																											--ORDER BY id ASC
			) AS B
				ON A.TarjetaKC_Id = B.TarjetaKC_Id COLLATE DATABASE_DEFAULT
				 AND A.Fecha = B.Fecha
			WHERE A.Veces = 1 OR B.Veces = 1) AS A
		WHERE A.Transac_No <> 1) AS A )
,
Datos AS
	(SELECT
		Transaccion_Id
		, TarjetaKC_Id
		, Fecha
		, CodPlanKielsaClinica
		, CASE
			WHEN Tipo_Ingreso = 'automatico'
				THEN 'mecanico'
			WHEN Tipo_Ingreso = 'mecanico'
				THEN 'automatico'
		END AS Tipo_Ingreso
		, Origen
		, Tipo_Documento
		, Sucursal_Registro
		, Usuario_Registro
		, TipoPlan
		, Tipo_Registro
		, ROW_NUMBER() OVER (PARTITION BY A.TarjetaKC_Id ORDER BY A.Fecha) AS Transac_No
	FROM
		(SELECT
			Transaccion_Id
			, TarjetaKC_Id
			, Fecha
			, CodPlanKielsaClinica
			, Tipo_Ingreso
			, Origen
			, Tipo_Documento
			, Sucursal_Registro
			, Usuario_Registro
			, TipoPlan
			, Tipo_Registro
			, Transac_No
		FROM	Datos1
		WHERE Transac_No > 1 AND Fecha <> Inicial
		UNION ALL
		SELECT
			Transaccion_Id
			, TarjetaKC_Id
			, Fecha
			, CodPlanKielsaClinica
			, Tipo_Ingreso
			, Origen
			, Tipo_Documento
			, Sucursal_Registro
			, Usuario_Registro
			, TipoPlan
			, Tipo_Registro
			, Transac_No
		FROM	Datos1
		WHERE Transac_No = 1) AS A )
,
FINAL AS
	(SELECT DISTINCT
			A.TarjetaKC_Id COLLATE DATABASE_DEFAULT AS TarjetaKC_Id
			, A.Fecha
			, CASE
				WHEN A.CodPlanKielsaClinica = 'No definido'
					THEN B.CodPlanKielsaClinica
				ELSE A.CodPlanKielsaClinica
			END AS CodPlanKielsaClinica
			, A.Tipo_Ingreso
			, CASE
				WHEN A.Origen = 0
					THEN B.Origen
				ELSE A.Origen
			END AS Origen
			, A.Tipo_Documento
			, CASE
				WHEN A.Sucursal_Registro = 0
					THEN B.Sucursal_Registro
				ELSE A.Sucursal_Registro
			END AS Sucursal_Registro
			, CASE
				WHEN A.Usuario_Registro = 'No definido'
					THEN B.Usuario_Registro
				ELSE A.Usuario_Registro
			END AS Usuario_Registro
			, CASE
				WHEN A.TipoPlan = 0
					THEN B.TipoPlan
				ELSE A.TipoPlan
			END AS TipoPlan
			, A.Tipo_Registro
			, GETDATE() AS Fecha_Actualizado
	FROM	Datos AS A
	INNER JOIN [DL_FARINTER].[dbo].[DL_Kielsa_KPP_Suscripcion] AS B -- {{ source ("DL_FARINTER","DL_Kielsa_KPP_Suscripcion") }}
		ON A.TarjetaKC_Id = B.TarjetaKC_Id COLLATE DATABASE_DEFAULT)
SELECT
	-ROW_NUMBER() OVER (ORDER BY Fecha) AS id
	, TarjetaKC_Id
	, Fecha
	, CodPlanKielsaClinica
	, Tipo_Ingreso
	, Origen
	, Tipo_Documento
	, Sucursal_Registro
	, Usuario_Registro
	, TipoPlan
	, Tipo_Registro
	, Fecha_Actualizado
FROM	FINAL