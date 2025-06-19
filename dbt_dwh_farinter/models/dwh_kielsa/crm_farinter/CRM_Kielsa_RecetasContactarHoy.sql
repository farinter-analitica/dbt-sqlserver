{%- set unique_key_list = ["Emp_Id","Sucursal_Id","Contactar_El","Ciclo","Receta_Id","Linea_Id"] -%}

{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario","periodo_unico/si"],
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

-- Solo editable en DBT DAGSTER
WITH Calendario_Filtrado AS (
    SELECT Fecha_Calendario,
        CONVERT(INT, CONVERT(VARCHAR(8), Fecha_Calendario, 112)) AS Semilla
    FROM BI_FARINTER.dbo.BI_Dim_Calendario -- {{ source('BI_FARINTER', 'BI_Dim_Calendario') }}
    WHERE Fecha_Calendario BETWEEN DATEADD(MONTH, -18, GETDATE())
        AND DATEADD(MONTH, 6, GETDATE())
),
VendedorSucursal_Filtrado AS (
    SELECT Emp_Id,
        Suc_Id,
        Vendedor_Id
    FROM DL_fARINTER.dbo.DL_Kielsa_VendedorSucursal -- {{ source('DL_FARINTER', 'DL_Kielsa_VendedorSucursal') }}
    WHERE Vendedor_Id <> '10000001'
        AND Bit_Activo = 1
),
VSBase AS (
    SELECT VS.Emp_Id,
        VS.Suc_Id,
        VS.Vendedor_Id,
        CAL.Fecha_Calendario,
        COUNT(*) OVER (
            PARTITION BY VS.Emp_Id,
            VS.Suc_Id,
            CAL.Fecha_Calendario
        ) AS Vendedores_Sucursal,
        ROW_NUMBER() OVER (
            PARTITION BY VS.Emp_Id,
            VS.Suc_Id,
            CAL.Fecha_Calendario
            ORDER BY CHECKSUM(VS.Vendedor_Id, CAL.Fecha_Calendario)
        ) AS Vendedor_Sucursal_Orden
    FROM VendedorSucursal_Filtrado VS
        CROSS JOIN Calendario_Filtrado CAL --WHERE VS.Suc_Id=82
),
VSAgrupado AS (
    SELECT Emp_Id,
        Suc_Id,
        Fecha_Calendario,
        Vendedores_Sucursal
    FROM VSBase
    GROUP BY Emp_Id,
        Suc_Id,
        Fecha_Calendario,
        Vendedores_Sucursal
),
RecetasCalculos_Filtrado AS (
    SELECT *,
        CAST(RC.Contactar_El AS DATE) AS Contactar_El_Date
    FROM DL_FARINTER.[dbo].[DL_Kielsa_RecetasCalculos] RC
    WHERE RC.Fecha_Receta >= DATEADD(MONTH, -18, GETDATE())
        AND RC.Indicador_Tratamiento_Completo = 0
),
RCCiclos AS (
    SELECT --TOP 100
        1 AS Ciclo,
        RC.Pais_Id,
        RC.Emp_Id AS Emp_Id,
        RC.Sucursal_Id,
        RC.Sucursal_Nombre,
        RC.Fecha_Compra,
        RC.Identidad,
        RC.Cliente_Nombre,
        RC.Articulo_Id,
        RC.Articulo_Nombre,
        RC.Cantidad_Recetada,
        RC.Comprado_Presentacion,
        RC.Indicacion_Receta,
        RC.Receta_Id,
        RC.Linea_Id,
        Fecha_Receta,
        RC.Contactar_El_Date AS Contactar_El,
        Tipo_Duracion,
        Presentacion,
        Consumo_Diario,
        Indicador_Tratamiento_Completo,
        RC.Indicador_A_Tiempo,
        DATEPART(ISO_WEEK, Contactar_El) Contactar_El_Semana,
        DATEPART(YEAR, Contactar_El) Contactar_El_Anio
    FROM RecetasCalculos_Filtrado RC
    UNION ALL
    --Recursivo de Ciclos hasta 3
    SELECT Ciclo + 1 AS Ciclo,
        RC.Pais_Id,
        RC.Emp_Id AS Emp_Id,
        RC.Sucursal_Id,
        RC.Sucursal_Nombre,
        RC.Fecha_Compra,
        RC.Identidad,
        RC.Cliente_Nombre,
        RC.Articulo_Id,
        RC.Articulo_Nombre,
        RC.Cantidad_Recetada,
        RC.Comprado_Presentacion,
        RC.Indicacion_Receta,
        RC.Receta_Id,
        RC.Linea_Id,
        RC.Fecha_Receta,
        DATEADD(
            DAY,
            ROUND(Presentacion * 1.0 / Consumo_Diario, 0),
            Contactar_El
        ) AS Contactar_El,
        Tipo_Duracion,
        Presentacion,
        Consumo_Diario,
        Indicador_Tratamiento_Completo,
        CASE
            WHEN DATEADD(
                DAY,
                ROUND(Presentacion * 1.0 / Consumo_Diario, 0),
                Contactar_El
            ) >= CONVERT(DATE, GETDATE()) THEN 1
            ELSE 0
        END Indicador_A_Tiempo,
        DATEPART(
            ISO_WEEK,
            DATEADD(
                DAY,
                ROUND(Presentacion * 1.0 / Consumo_Diario, 0),
                Contactar_El
            )
        ) Contactar_El_Semana,
        DATEPART(
            YEAR,
            DATEADD(
                DAY,
                ROUND(Presentacion * 1.0 / Consumo_Diario, 0),
                Contactar_El
            )
        ) Contactar_El_Anio
    FROM RCCiclos RC
    WHERE Ciclo < 3
        AND Tipo_Duracion = 'Permanente'
        AND ROUND(Presentacion * 1.0 / Consumo_Diario, 0) > 0
),
RCAgruparArticulosSemana AS (
    SELECT Emp_Id,
        Identidad,
        Contactar_El_Semana,
        Contactar_El_Anio,
        MIN(Contactar_El) Contactar_El_MinSemana
    FROM RCCiclos
    GROUP BY Emp_Id,
        Identidad,
        Contactar_El_Semana,
        Contactar_El_Anio
),
RCBase AS (
    SELECT RC.*,
        RCA.Contactar_El_MinSemana
    FROM RCCiclos RC
        INNER JOIN RCAgruparArticulosSemana RCA ON RC.Emp_Id = RCA.Emp_Id
        AND RC.Identidad = RCA.Identidad
        AND RC.Contactar_El_Semana = RCA.Contactar_El_Semana
        AND RC.Contactar_El_Anio = RCA.Contactar_El_Anio
    WHERE RC.Indicador_A_Tiempo = 1 -- AND RCA.Contactar_El_MinSemana >= '20241105' --CONVERT(DATE, GETDATE())
        -- AND RCA.Contactar_El_MinSemana < '20241106'--CONVERT(DATE, GETDATE() + 1)
        -- AND RC.Contactar_El >= CONVERT(DATE, GETDATE())
        -- AND RC.Contactar_El < CONVERT(DATE, DATEADD(DAY,1,GETDATE()))
        AND RCA.Contactar_El_MinSemana >= CONVERT(DATE, GETDATE())
        AND RCA.Contactar_El_MinSemana < CONVERT(DATE, DATEADD(DAY, 1, GETDATE()))
) --SELECT TOP 100 * FROM RCBase RCA
/*SELECT TOP 100 * FROM RCCiclos RCA
 WHERE 		RCA.Contactar_El >= CONVERT(DATE, GETDATE())
 AND RCA.Contactar_El < CONVERT(DATE, DATEADD(DAY,5,GETDATE()))
 and rca.Ciclo=1*/
/*SELECT TOP 100 * FROM RCBaseFiltroInicial  RCA
 WHERE RCA.Contactar_El >= CONVERT(DATE, GETDATE())
 AND RCA.Contactar_El < CONVERT(DATE, DATEADD(DAY,5,GETDATE()))
 */
,
RCClientes AS (
    SELECT Pais_Id,
        Emp_Id,
        Sucursal_Id,
        Identidad,
        Contactar_El_MinSemana
    FROM RCBase
    GROUP BY Pais_Id,
        Emp_Id,
        Sucursal_Id,
        Identidad,
        Contactar_El_MinSemana
),
RCClientesBase AS (
    SELECT RCClientes.*,
        COUNT(*) OVER (
            PARTITION BY RCClientes.Pais_Id,
            RCClientes.Sucursal_Id,
            RCClientes.Contactar_El_MinSemana
        ) AS Clientes_Sucursal,
        ROW_NUMBER() OVER (
            PARTITION BY RCClientes.Pais_Id,
            RCClientes.Sucursal_Id,
            RCClientes.Contactar_El_MinSemana
            ORDER BY CHECKSUM(
                    RCClientes.Identidad,
                    RCClientes.Contactar_El_MinSemana
                )
        ) AS Identidad_Sucursal_Orden
    FROM RCClientes
),
RCClientesOrden AS (
    SELECT RC.*,
        FLOOR(
            (RC.Identidad_Sucursal_Orden - 1) * VSA.Vendedores_Sucursal / RC.Clientes_Sucursal
        ) + 1 AS Vendedor_Sucursal_Orden,
        VSA.Vendedores_Sucursal
    FROM RCClientesBase RC
        INNER JOIN VSAgrupado VSA ON RC.Sucursal_Id = VSA.Suc_Id
        AND RC.Emp_Id = VSA.Emp_Id
        AND RC.Contactar_El_MinSemana = VSA.Fecha_Calendario --WHERE RCClientesBase.Sucursal_Id=82
) --SELECT TOP 100 * FROM RCClientesOrden
,
RCOrden AS (
    SELECT RCBase.*,
        RCClientesOrden.Vendedor_Sucursal_Orden,
        RCClientesOrden.Vendedores_Sucursal,
        RCClientesOrden.Identidad_Sucursal_Orden,
        RCClientesOrden.Clientes_Sucursal
    FROM RCBase
        LEFT JOIN RCClientesOrden ON RCBase.Sucursal_Id = RCClientesOrden.Sucursal_Id
        AND RCBase.Emp_Id = RCClientesOrden.Emp_Id
        AND RCBase.Identidad = RCClientesOrden.Identidad
        AND RCBase.Contactar_El_MinSemana = RCClientesOrden.Contactar_El_MinSemana
),
VSOrden AS (
    SELECT VSBase.*,
CASE
            WHEN VSBase.Vendedor_Sucursal_Orden >= VSBase.Vendedores_Sucursal THEN 1
            ELSE 0
        END AS Ultimo_Vendedor_Sucursal
    FROM VSBase
),
RCReparticion AS (
    SELECT RCO.*,
        VSO.Vendedor_Id,
        VSO.Ultimo_Vendedor_Sucursal
    FROM RCOrden RCO
        LEFT JOIN VSOrden VSO ON RCO.Sucursal_Id = VSO.Suc_Id
        AND RCO.Emp_Id = VSO.Emp_Id
        AND RCO.Contactar_El_MinSemana = VSO.Fecha_Calendario
        AND RCO.Vendedor_Sucursal_Orden = VSO.Vendedor_Sucursal_Orden
) --SELECT * FROM RCReparticion ORDER BY Pais_Id, Sucursal_Id, Identidad_Sucursal_Orden
SELECT ISNULL(RCReparticion.Pais_Id,0) AS Pais_Id,
    ISNULL(RCReparticion.Emp_Id,0) AS Emp_Id,
    ISNULL(RCR2.Sucursal_Id,0) AS Sucursal_Id,
    ISNULL(SUC.Sucursal_Nombre,'') AS Sucursal_Nombre,
    ISNULL(RCR2.Sucursal_Id,0) AS Sucursal_Id_Original,
    ISNULL(RCReparticion.Sucursal_Nombre,'') AS Sucursal_Nombre_Original,
    RCReparticion.Fecha_Compra,
    RCReparticion.Identidad,
    RCReparticion.Cliente_Nombre,
    RCReparticion.Articulo_Id,
    RCReparticion.Articulo_Nombre,
    RCReparticion.Cantidad_Recetada,
    cast(
        RCReparticion.Comprado_Presentacion AS decimal(18, 4)
    ) Comprado_Presentacion,
    RCReparticion.Indicacion_Receta,
    RCR2.Vendedor_Id,
    ISNULL(RCReparticion.Receta_Id,0) AS Receta_Id,
    ISNULL(RCReparticion.Linea_Id,0) AS Linea_Id,
    V.Empleado_Nombre,
    ISNULL(RCReparticion.Contactar_El_MinSemana,'19000101') AS Contactar_El,
    ISNULL(RCReparticion.Ciclo,0) AS Ciclo,
    RCReparticion.Indicador_A_Tiempo,
    RCReparticion.Fecha_Receta,
    RCReparticion.Consumo_Diario,
    RCReparticion.Presentacion,
    RCReparticion.Vendedores_Sucursal,
    RCReparticion.Ultimo_Vendedor_Sucursal,
    RCReparticion.Clientes_Sucursal,
    RCReparticion.Identidad_Sucursal_Orden,
    RCReparticion.Vendedor_Sucursal_Orden
FROM RCReparticion
    LEFT JOIN (
        SELECT Pais_Id,
            Identidad,
            Sucursal_Id,
            Emp_Id,
            Contactar_El_MinSemana,
            MAX(Vendedor_Id) AS Vendedor_Id,
            DENSE_RANK() OVER (
                PARTITION BY RCR2.Pais_Id,
                RCR2.Identidad,
                RCR2.Contactar_El_MinSemana
                ORDER BY RCR2.Sucursal_Id
            ) AS Orden_Vendedor_Identidad
        FROM RCReparticion RCR2
        GROUP BY Pais_Id,
            Identidad,
            Sucursal_Id,
            Emp_Id,
            Contactar_El_MinSemana
    ) RCR2 ON RCReparticion.Pais_Id = RCR2.Pais_Id
    AND RCReparticion.Emp_Id = RCR2.Emp_Id
    AND RCReparticion.Identidad = RCR2.Identidad
    AND RCReparticion.Contactar_El_MinSemana = RCR2.Contactar_El_MinSemana
    AND RCR2.Orden_Vendedor_Identidad = 1
    LEFT JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Empleado V -- {{ ref('BI_Kielsa_Dim_Empleado') }}
    ON RCR2.Vendedor_Id = V.Empleado_Id
    AND RCR2.Emp_Id = V.Emp_Id
    LEFT JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Sucursal SUC -- {{ ref('BI_Kielsa_Dim_Sucursal') }}
    ON RCR2.Sucursal_Id = SUC.Sucursal_Id
    AND RCR2.Emp_Id = SUC.Emp_Id
    LEFT JOIN (
        SELECT DISTINCT Emp_Id,
            Identidad
        FROM CRM_FARINTER.dbo.CRM_Kielsa_RecetasContactarHist -- {{ ref('CRM_Kielsa_RecetasContactarHist') }}
        WHERE Contactar_El >= CAST(DATEADD(DAY, -7, GETDATE()) AS DATE)
            AND Contactar_El < CAST(GETDATE() AS DATE)
    ) CEH ON RCReparticion.Emp_Id = CEH.Emp_Id
    AND RCReparticion.Identidad = CEH.Identidad
    LEFT JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_ClienteGeneral CLI -- {{ source('BI_FARINTER', 'BI_Kielsa_Dim_ClienteGeneral') }}
    ON RCReparticion.Identidad = CLI.Identidad_Limpia
    AND RCReparticion.Emp_Id = CLI.Emp_Id
WHERE CEH.Emp_Id IS NULL
    AND (
        CLI.Celular_Valido = 1
        OR CLI.Telefono_Valido = 1
    ) --WHERE RCReparticion.Orden_Vendedor_Identidad =1 --Eliminar identidades en varias sucursales
    /*###Pruebas
     
     DROP TABLE IF EXISTS #CRM_Kielsa_RecetasContactarHoy_Temp2
     select *
     INTO #CRM_Kielsa_RecetasContactarHoy_Temp2
     from [dbo].[CRM_Kielsa_RecetasContactarHoy]
     
     DROP TABLE IF EXISTS #CRM_Kielsa_RecetasContactarHoy_Temp
     select *
     INTO #CRM_Kielsa_RecetasContactarHoy_Temp
     from [dbo].[CRM_Kielsa_RecetasContactarHoy]
     --WHERE Clientes_Sucursal>=Vendedores_Sucursal
     
     print 'Prueba 1: Identidades con mas de un vendedor'
     IF EXISTS (
     select * from(
     select  Identidad, Pais_Id , COUNT(DISTINCT Vendedor_Id) as total
     from CRM_FARINTER.[dbo].[CRM_Kielsa_RecetasContactarHoy_Temp]
     group by  Identidad, Pais_Id ) as A where total > 1
     ) BEGIN;THROW 50000, 'Identidades con mas de un vendedor', 1 ; END
     ELSE BEGIN;PRINT 'Prueba pasada: No hay identidades con mas de un vendedor'; END
     
     print 'Prueba 2: Claves duplicadas'
     IF EXISTS (
     select * from(
     select  Ciclo,Identidad,Pais_Id,Sucursal_Id,Contactar_El,Receta_Id,Linea_Id, COUNT(*) as total
     from CRM_FARINTER.[dbo].[CRM_Kielsa_RecetasContactarHoy_Temp]
     group by  Ciclo,Identidad,Pais_Id,Sucursal_Id,Contactar_El,Receta_Id,Linea_Id) as A where total > 1
     ) BEGIN;THROW 50000, 'Claves duplicadas', 1 ; END
     ELSE BEGIN;PRINT 'Prueba pasada: No hay claves duplicadas'; END
     
     print 'Prueba 3: Vendedores Correctamente Distribuidos'
     IF EXISTS (
     select *, CEILING(Clientes_Sucursal*1.0/ Vendedores_Sucursal ) AS Proporcion from(
     select  Pais_Id,Sucursal_Id,Vendedor_Id,Contactar_El,COUNT(DISTINCT Identidad) as total, MAX(Vendedores_Sucursal) as Vendedores_Sucursal, MAX(Clientes_Sucursal) as Clientes_Sucursal
     from CRM_FARINTER.[dbo].[CRM_Kielsa_RecetasContactarHoy_Temp]
     group by  Pais_Id,Sucursal_Id,Vendedor_Id,Contactar_El) as A 
     where total > CEILING(Clientes_Sucursal*1.0/ Vendedores_Sucursal ) OR total < FLOOR(Clientes_Sucursal*1.0/ Vendedores_Sucursal )
     ) BEGIN;THROW 50000, 'Vendedores Correctamente Distribuidos', 1 ; END
     ELSE BEGIN;PRINT 'Prueba pasada: Vendedores Correctamente Distribuidos'; END
     
     
     
     SELECT TOP 100 * FROM CRM_Kielsa_RecetasContactarHoy_Temp --WHERE Sucursal_Id=45
     WHERE Identidad = '0101198905859' AND Linea_Id = 3
     
     select * from(
     select  Identidad, COUNT(DISTINCT Sucursal_Id) as total
     from CRM_FARINTER.[dbo].[CRM_Kielsa_RecetasContactarHoy]
     group by  Identidad ) as A where total > 1
     
     SELECT COUNT(*) 	FROM DL_FARINTER.[dbo].[DL_Kielsa_RecetasCalculos] RC
     WHERE RC.Fecha_Receta >= DATEADD(MONTH, -18, GETDATE())
     AND RC.Indicador_Tratamiento_Completo = 0 
     
     
     
     */