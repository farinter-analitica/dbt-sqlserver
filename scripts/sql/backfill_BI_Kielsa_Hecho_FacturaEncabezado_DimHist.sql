-- Backfill skeleton for BI_Kielsa_Hecho_FacturaEncabezado_DimHist
-- Use this script as the body of a SQL Agent Job step.
-- Itera por año-mes desde @start_ym hasta @end_ym y ejecuta una query directa
-- (NO usa SQL dinámico). Registra inicio/fin/error en dbo.BI_CnfBitacora.
-- -------------------------------------------------------------------------
SET NOCOUNT ON;
USE BI_FARINTER;
-- Configurable start / end (cambiar si es necesario)
DECLARE @start_ym INT = 201801;   -- 2020-01
DECLARE @end_ym   INT = 202509;   -- 2025-08 (según requerimiento: hasta 202508)
DECLARE @current_ym INT = @start_ym;

DECLARE @anio INT;
DECLARE @mes INT;
DECLARE @start_dt DATETIME;
DECLARE @end_dt DATETIME;
DECLARE @duration_minutes NUMERIC(16,4);

WHILE @current_ym <= @end_ym
BEGIN
    SET @anio = @current_ym / 100;
    SET @mes  = @current_ym % 100;

    -- Saltar meses inválidos (por ejemplo 202013, 202100, etc.)
    IF @mes BETWEEN 1 AND 12
    BEGIN
        PRINT CONCAT('Backfill: procesando ', @current_ym, ' (Anio=', @anio, ', Mes=', @mes, ')');

        SET @start_dt = GETDATE();

        -- Registrar inicio en la bitácora
        INSERT INTO dbo.BI_CnfBitacora (Bitacora_Fecha, Bitacora_Mensaje, Bitacora_Tipo, Bitacora_Tabla, Servidor, Duracion_Minutos)
        VALUES (
            @start_dt,
            CONCAT('Iniciando backfill ', @current_ym, ' (Anio=', @anio, ', Mes=', @mes, ')'),
            'INFO',
            'BI_Kielsa_Hecho_FacturaEncabezado_DimHist',
            NULL,
            NULL
        );

        BEGIN TRY
            -- =========================
            -- Pegar aquí su query por mes (DIRECTA, sin SQL dinámico)
            -- Debe filtrar por estas variables en el WHERE:
            --    WHERE Anio_Id = @anio AND Mes_Id = @mes
            -- Ejemplo de plantilla (ADAPTAR según su query real):
            -- =========================
            -- BEGIN TRAN;
            --     -- Ejemplo: insertar/merge/actualizar datos del mes
            --     INSERT INTO dbo.SuTablaTarget (col1, col2, Anio_Id, Mes_Id)
            --     SELECT col1, col2, Anio_Id, Mes_Id
            --     FROM dbo.SuFuente
            --     WHERE Anio_Id = @anio AND Mes_Id = @mes;
            -- COMMIT;
            -- =========================

            -- >>> PASTE YOUR MONTHLY QUERY HERE (use @anio and @mes) <<<

            WITH agregado_factura AS (
                SELECT -- noqa: ST06 
                    CAST(Fecha_Id AS DATE) AS Factura_Fecha,
                    CAST(Pais_Id AS INT) AS Emp_Id,
                    CASE
                        WHEN Sucursal_Id LIKE '%-%' THEN TRY_CAST(SUBSTRING(Sucursal_Id, CHARINDEX('-', Sucursal_Id) + 1, 50) AS INT)
                        ELSE TRY_CAST(Sucursal_Id AS INT)
                    END AS Suc_Id,
                    Sucursal_Id AS EmpSuc_Id,
                    CASE
                        WHEN Documento_Id LIKE '%-%' THEN TRY_CAST(SUBSTRING(Documento_Id, CHARINDEX('-', Documento_Id) + 1, 50) AS INT)
                        ELSE TRY_CAST(Documento_Id AS INT)
                    END AS TipoDoc_Id,
                    Documento_Id AS EmpTipoDoc_Id,
                    --TRY_CAST(SubDocumento_Id AS INT) AS SubDoc_Id,
                    SubDocumento_Id AS SubDoc_Id,
                    CASE
                        WHEN Factura_Id LIKE '%-%' THEN TRY_CAST(
                            SUBSTRING(
                                Factura_Id,
                                CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id) + 1) + 1) + 1,
                                CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id) + 1) + 1) + 1)
                                - CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id) + 1) + 1)
                                - 1
                            ) AS INT
                        ) ELSE TRY_CAST(Factura_Id AS INT)
                    END AS Caja_Id,
                    CASE
                        WHEN Factura_Id LIKE '%-%' THEN TRY_CAST(
                            SUBSTRING(
                                Factura_Id,
                                CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id) + 1) + 1) + 1) + 1,
                                LEN(Factura_Id) - CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id) + 1) + 1) + 1)
                            ) AS VARCHAR(50)
                        ) ELSE TRY_CAST(Factura_Id AS INT)
                    END AS Factura_Id,
                    Factura_Id AS EmpSucDocCajFac_Id,
                    CAST(Fecha_Id AS DATE) AS Fecha_Id,
                    -- Requisito 2: Sucursal histórica
                    CASE
                        WHEN Zona_Id LIKE '%-%' THEN TRY_CAST(SUBSTRING(Zona_Id, CHARINDEX('-', Zona_Id) + 1, 50) AS INT)
                        ELSE TRY_CAST(Zona_Id AS INT)
                    END AS Zona_Id,
                    Zona_Id AS EmpZona_Id,
                    CASE
                        WHEN Departamento_Id LIKE '%-%' THEN TRY_CAST(SUBSTRING(Departamento_Id, CHARINDEX('-', Departamento_Id) + 1, 50) AS INT)
                        ELSE TRY_CAST(Departamento_Id AS INT)
                    END AS Departamento_Id,
                    Departamento_Id AS EmpDep_Id,
                    CASE
                        WHEN Municipio_Id LIKE '%-%' THEN TRY_CAST(REVERSE(SUBSTRING(REVERSE(Municipio_Id), 1, CHARINDEX('-', REVERSE(Municipio_Id)) - 1)) AS INT)
                        ELSE TRY_CAST(Municipio_Id AS INT)
                    END AS Municipio_Id,
                    Municipio_Id AS EmpDepMun_Id,
                    CASE
                        WHEN Ciudad_Id LIKE '%-%' THEN TRY_CAST(REVERSE(SUBSTRING(REVERSE(Ciudad_Id), 1, CHARINDEX('-', REVERSE(Ciudad_Id)) - 1)) AS INT)
                        ELSE TRY_CAST(Ciudad_Id AS INT)
                    END AS Ciudad_Id,
                    Ciudad_Id AS EmpDepMunCiu_Id,
                    CASE
                        WHEN TipoSucursal_Id LIKE '%-%' THEN TRY_CAST(SUBSTRING(TipoSucursal_Id, CHARINDEX('-', TipoSucursal_Id) + 1, 50) AS INT)
                        ELSE TRY_CAST(TipoSucursal_Id AS INT)
                    END AS TipoSucursal_Id,
                    TipoSucursal_Id AS EmpTipoSucursal_Id,
                    CASE
                        WHEN Plan_Id LIKE '%-%' THEN TRY_CAST(SUBSTRING(Plan_Id, CHARINDEX('-', Plan_Id) + 1, 50) AS INT)
                        ELSE TRY_CAST(Plan_Id AS INT)
                    END AS Plan_Id,
                    Plan_Id AS EmpPlan_Id,
                    -- Requisito 4: Tipo cliente histórico
                    CASE
                        WHEN TipoCliente_Id LIKE '%-%' THEN TRY_CAST(SUBSTRING(TipoCliente_Id, CHARINDEX('-', TipoCliente_Id) + 1, 50) AS INT)
                        ELSE TRY_CAST(TipoCliente_Id AS INT)
                    END AS TipoCliente_Id,
                    TipoCliente_Id AS EmpTipoCliente_Id,
                    -- Campos adicionales de control
                    Cliente_Id,
                    Monedero_Id,
                    CanalVenta_Id,
                    Anio_Id,
                    Mes_Id
                    -- Agregaciones de métricas a nivel factura
                FROM BI_FARINTER.[dbo].[BI_Hecho_VentasHist_Kielsa] WITH (NOLOCK)
                WHERE
                    Anio_Id = @Anio
                    AND Mes_Id = @Mes            )
            INSERT INTO BI_Kielsa_Hecho_FacturaEncabezado_DimHist (
                Factura_Fecha,
                Emp_Id,
                Suc_Id,
                EmpSuc_Id,
                TipoDoc_Id,
                EmpTipoDoc_Id,
                SubDoc_Id,
                Caja_Id,
                Factura_Id,
                EmpSucDocCajFac_Id,
                Fecha_Id,
                Zona_Id,
                EmpZona_Id,
                Departamento_Id,
                EmpDep_Id,
                Municipio_Id,
                EmpDepMun_Id,
                Ciudad_Id,
                EmpDepMunCiu_Id,
                TipoSucursal_Id,
                EmpTipoSucursal_Id,
                Plan_Id,
                EmpPlan_Id,
                TipoCliente_Id,
                EmpTipoCliente_Id,
                Cliente_Id,
                Monedero_Id,
                CanalVenta_Id,
                Fecha_Actualizado
            )
            SELECT --noqa: ST06
                ISNULL(MAX(Factura_Fecha), '19000101') AS Factura_Fecha,
                ISNULL(Emp_Id, 0) AS Emp_Id,
                ISNULL(MAX(Suc_Id), 0) AS Suc_Id,
                ISNULL(EmpSuc_Id, 'X') AS EmpSuc_Id,
                ISNULL(MAX(TipoDoc_Id), 0) AS TipoDoc_Id,
                ISNULL(MAX(EmpTipoDoc_Id), 'X') AS EmpTipoDoc_Id,
                ISNULL(MAX(SubDoc_Id), 0) AS SubDoc_Id,
                ISNULL(MAX(Caja_Id), 0) AS Caja_Id,
                ISNULL(MAX(Factura_Id), 0) AS Factura_Id,
                ISNULL(MAX(EmpSucDocCajFac_Id), 'X') AS EmpSucDocCajFac_Id,
                MAX(Fecha_Id) AS Fecha_Id,
                ISNULL(MAX(Zona_Id), 0) AS Zona_Id,
                MAX(EmpZona_Id) AS EmpZona_Id,
                ISNULL(MAX(Departamento_Id), 0) AS Departamento_Id,
                MAX(EmpDep_Id) AS EmpDep_Id,
                ISNULL(MAX(Municipio_Id), 0) AS Municipio_Id,
                MAX(EmpDepMun_Id) AS EmpDepMun_Id,
                ISNULL(MAX(Ciudad_Id), 0) AS Ciudad_Id,
                MAX(EmpDepMunCiu_Id) AS EmpDepMunCiu_Id,
                ISNULL(MAX(TipoSucursal_Id), 0) AS TipoSucursal_Id,
                MAX(EmpTipoSucursal_Id) AS EmpTipoSucursal_Id,
                ISNULL(MAX(Plan_Id), 0) AS Plan_Id,
                MAX(EmpPlan_Id) AS EmpPlan_Id,
                ISNULL(MAX(TipoCliente_Id), 0) AS TipoCliente_Id,
                MAX(EmpTipoCliente_Id) AS EmpTipoCliente_Id,
                MAX(Cliente_Id) AS Cliente_Id,
                MAX(Monedero_Id) AS Monedero_Id,
                MAX(CanalVenta_Id) AS CanalVenta_Id,
                CASE WHEN CAST(MAX(Fecha_Id) AS DATETIME) > GETDATE() - 7 THEN GETDATE() ELSE CAST(ISNULL(MAX(Fecha_Id), '19000101') AS DATETIME) END AS Fecha_Actualizado
            FROM agregado_factura
            GROUP BY
                Anio_Id,
                Mes_Id,
                Fecha_Id,
                Emp_Id,
                EmpSuc_Id,
                EmpSucDocCajFac_Id

            SET @end_dt = GETDATE();
            SET @duration_minutes = CAST(DATEDIFF(SECOND, @start_dt, @end_dt) AS NUMERIC(16,4)) / 60.0;

            INSERT INTO dbo.BI_CnfBitacora (Bitacora_Fecha, Bitacora_Mensaje, Bitacora_Tipo, Bitacora_Tabla, Servidor, Duracion_Minutos)
            VALUES (
                @end_dt,
                CONCAT('Completado backfill ', @current_ym, ' (Anio=', @anio, ', Mes=', @mes, '). Duración_min=', CONVERT(VARCHAR(30), @duration_minutes)),
                'INFO',
                'BI_Kielsa_Hecho_FacturaEncabezado_DimHist',
                NULL,
                @duration_minutes
            );

        END TRY
        BEGIN CATCH
            SET @end_dt = GETDATE();
            SET @duration_minutes = CAST(DATEDIFF(SECOND, @start_dt, @end_dt) AS NUMERIC(16,4)) / 60.0;

            INSERT INTO dbo.BI_CnfBitacora (Bitacora_Fecha, Bitacora_Mensaje, Bitacora_Tipo, Bitacora_Tabla, Servidor, Duracion_Minutos)
            VALUES (
                @end_dt,
                CONCAT('Error backfill ', @current_ym, ' (Anio=', @anio, ', Mes=', @mes, '): ', ERROR_MESSAGE()),
                'ERROR',
                'BI_Kielsa_Hecho_FacturaEncabezado_DimHist',
                NULL,
                @duration_minutes
            );

            -- Opcional: continuar con siguiente mes o re-raisear el error
            -- RAISERROR(ERROR_MESSAGE(), 16, 1);
        END CATCH

        -- Opcional: pequeña pausa para reducir presión en el servidor
        WAITFOR DELAY '00:00:02';
    END

    -- Avanzar al siguiente mes
    IF @mes = 12
        SET @current_ym = (@anio + 1) * 100 + 1; -- año siguiente, mes 01
    ELSE
        SET @current_ym = @current_ym + 1; -- siguiente mes
END

PRINT 'Backfill finalizado.';

-- Instrucciones para crear un SQL Agent Job:
-- 1) Crear un nuevo Job en SQL Server Agent.
-- 2) Añadir un Step de tipo 'Transact-SQL script (T-SQL)' apuntando a la base de datos objetivo.
-- 3) Pegar el contenido de este archivo en el Step.
-- 4) Ejecutar o programar según se requiera.
