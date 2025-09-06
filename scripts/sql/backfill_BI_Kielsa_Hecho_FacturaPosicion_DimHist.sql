-- Backfill skeleton for BI_Kielsa_Hecho_FacturaEncabezado_DimHist
-- Use this script as the body of a SQL Agent Job step.
-- Itera por año-mes desde @start_ym hasta @end_ym y ejecuta una query directa
-- (NO usa SQL dinámico). Registra inicio/fin/error en dbo.BI_CnfBitacora.
-- -------------------------------------------------------------------------
SET NOCOUNT ON;
USE BI_FARINTER;

-- Configurable start / end (cambiar si es necesario)
DECLARE @start_ym INT = 201803;
DECLARE @end_ym   INT = 202412;
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

            INSERT INTO dbo.BI_Kielsa_Hecho_FacturaPosicion_DimHist (
                Factura_Fecha,
                Emp_Id,
                Suc_Id,
                TipoDoc_Id,
                EmpTipoDoc_Id,
                Caja_Id,
                Factura_Id,
                EmpSucDocCajFac_Id,
                Fecha_Id,
                Articulo_Id,
                EmpArt_Id,
                Casa_Id,
                EmpCasa_Id,
                Marca1_Id,
                EmpMarca1_Id,
                CategoriaArt_Id,
                EmpCategoriaArt_Id,
                DeptoArt_Id,
                EmpDeptoArt_Id,
                SubCategoria1Art_Id,
                EmpCatSubCategoria1Art_Id,
                SubCategoria2Art_Id,
                EmpCatSubCategoria1_2Art_Id,
                SubCategoria3Art_Id,
                EmpCatSubCategoria1_2_3Art_Id,
                SubCategoria4Art_Id,
                EmpCatSubCategoria1_2_3_4Art_Id,
                Proveedor_Id,
                EmpProv_Id,
                Cuadro_Id,
                Mecanica_Id,
                Fecha_Actualizado
            )
            SELECT -- noqa: ST06 
                ISNULL(CAST(Fecha_Id AS DATE), '19000101') AS Factura_Fecha,
                ISNULL(CAST(Pais_Id AS INT), 0) AS Emp_Id,
                ISNULL(CASE
                    WHEN Sucursal_Id LIKE '%-%' THEN TRY_CAST(SUBSTRING(Sucursal_Id, CHARINDEX('-', Sucursal_Id) + 1, 50) AS INT)
                    ELSE TRY_CAST(Sucursal_Id AS INT)
                END, 0) AS Suc_Id,
                ISNULL(CASE
                    WHEN Documento_Id LIKE '%-%' THEN TRY_CAST(SUBSTRING(Documento_Id, CHARINDEX('-', Documento_Id) + 1, 50) AS INT)
                    ELSE TRY_CAST(Documento_Id AS INT)
                END, 0) AS TipoDoc_Id,
                ISNULL(Documento_Id, 'X') AS EmpTipoDoc_Id,
                ISNULL(CASE
                    WHEN Factura_Id LIKE '%-%' THEN TRY_CAST(
                        SUBSTRING(
                            Factura_Id,
                            CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id) + 1) + 1) + 1,
                            CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id) + 1) + 1) + 1)
                            - CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id) + 1) + 1)
                            - 1
                        ) AS INT
                    ) ELSE TRY_CAST(Factura_Id AS INT)
                END, 0) AS Caja_Id,
                ISNULL(CASE
                    WHEN Factura_Id LIKE '%-%' THEN TRY_CAST(
                        SUBSTRING(
                            Factura_Id,
                            CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id) + 1) + 1) + 1) + 1,
                            LEN(Factura_Id) - CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id) + 1) + 1) + 1)
                        ) AS VARCHAR(50)
                    ) ELSE TRY_CAST(Factura_Id AS INT)
                END, 0) AS Factura_Id,
                Factura_Id AS EmpSucDocCajFac_Id,
                CAST(Fecha_Id AS DATE) AS Fecha_Id,
                ISNULL(CASE
                    WHEN Articulo_Id LIKE '%-%' THEN TRY_CAST(SUBSTRING(Articulo_Id, CHARINDEX('-', Articulo_Id) + 1, 50) AS NVARCHAR(50))
                    ELSE TRY_CAST(Articulo_Id AS NVARCHAR(50))
                END, 0) AS Articulo_Id,
                Articulo_Id AS EmpArt_Id,
                CASE
                    WHEN Casa_Id LIKE '%-%' THEN TRY_CAST(SUBSTRING(Casa_Id, CHARINDEX('-', Casa_Id) + 1, 50) AS INT)
                    ELSE TRY_CAST(Casa_Id AS INT)
                END AS Casa_Id,
                Casa_Id AS EmpCasa_Id,
                CASE
                    WHEN Marca1_Id LIKE '%-%' THEN TRY_CAST(SUBSTRING(Marca1_Id, CHARINDEX('-', Marca1_Id) + 1, 50) AS INT)
                    ELSE TRY_CAST(Marca1_Id AS INT)
                END AS Marca1_Id,
                Marca1_Id AS EmpMarca1_Id,
                CASE
                    WHEN CategoriaArt_Id LIKE '%-%' THEN TRY_CAST(SUBSTRING(CategoriaArt_Id, CHARINDEX('-', CategoriaArt_Id) + 1, 50) AS INT)
                    ELSE TRY_CAST(CategoriaArt_Id AS INT)
                END AS CategoriaArt_Id,
                CategoriaArt_Id AS EmpCategoriaArt_Id,
                CASE
                    WHEN DeptoArt_Id LIKE '%-%' THEN TRY_CAST(SUBSTRING(DeptoArt_Id, CHARINDEX('-', DeptoArt_Id) + 1, 50) AS INT)
                    ELSE TRY_CAST(DeptoArt_Id AS INT)
                END AS DeptoArt_Id,
                DeptoArt_Id AS EmpDeptoArt_Id,
                CASE
                    WHEN SubCategoria1Art_Id LIKE '%-%' THEN TRY_CAST(REVERSE(SUBSTRING(REVERSE(SubCategoria1Art_Id), 1, CHARINDEX('-', REVERSE(SubCategoria1Art_Id)) - 1)) AS INT)
                    ELSE TRY_CAST(SubCategoria1Art_Id AS INT)
                END AS SubCategoria1Art_Id,
                SubCategoria1Art_Id AS EmpCatSubCategoria1Art_Id,
                CASE
                    WHEN SubCategoria2Art_Id LIKE '%-%' THEN TRY_CAST(REVERSE(SUBSTRING(REVERSE(SubCategoria2Art_Id), 1, CHARINDEX('-', REVERSE(SubCategoria2Art_Id)) - 1)) AS INT)
                    ELSE TRY_CAST(SubCategoria2Art_Id AS INT)
                END AS SubCategoria2Art_Id,
                SubCategoria2Art_Id AS EmpCatSubCategoria1_2Art_Id,
                CASE
                    WHEN SubCategoria3Art_Id LIKE '%-%' THEN TRY_CAST(REVERSE(SUBSTRING(REVERSE(SubCategoria3Art_Id), 1, CHARINDEX('-', REVERSE(SubCategoria3Art_Id)) - 1)) AS INT)
                    ELSE TRY_CAST(SubCategoria3Art_Id AS INT)
                END AS SubCategoria3Art_Id,
                SubCategoria3Art_Id AS EmpCatSubCategoria1_2_3Art_Id,
                CASE
                    WHEN SubCategoria4Art_Id LIKE '%-%' THEN TRY_CAST(REVERSE(SUBSTRING(REVERSE(SubCategoria4Art_Id), 1, CHARINDEX('-', REVERSE(SubCategoria4Art_Id)) - 1)) AS INT)
                    ELSE TRY_CAST(SubCategoria4Art_Id AS INT)
                END AS SubCategoria4Art_Id,
                SubCategoria4Art_Id AS EmpCatSubCategoria1_2_3_4Art_Id,
                CAST(Proveedor_Id AS INT) AS Proveedor_Id,
                CONCAT(Pais_Id, '-', Proveedor_Id) AS EmpProv_Id,
                CAST(Cuadro_Id AS INT) AS Cuadro_Id,
                CAST(Mecanica_Id AS NVARCHAR(50)) AS Mecanica_Id,
                CASE WHEN CAST([Fecha_Id] AS DATETIME) > GETDATE() - 7 THEN GETDATE() ELSE CAST(ISNULL([Fecha_Id], '19000101') AS DATETIME) END AS Fecha_Actualizado
            FROM BI_FARINTER.[dbo].[BI_Hecho_VentasHist_Kielsa] WITH (NOLOCK)
            --order by Fecha_Id desc
            WHERE
                Anio_Id = @anio
                AND Mes_Id = @mes

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
        -- WAITFOR DELAY '00:00:02';
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
