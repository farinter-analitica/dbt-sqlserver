{% set unique_key_list = ['Pais_Id','Cliente_Id'] %}

{{ 
    config(
       group="kielsa_analitica_atributos",
       tags=["automation/periodo_diario","periodo_unico/si", "automation_only"],
       materialized="view",
       on_schema_change="append_new_columns",
       meta={"owners": ["edwin.martinez@farinter.com"]}
    )
}}

-- Solo editable en DBT DAGSTER
-- llaves: {{ unique_key_list }}

WITH CTE_Cronico AS (
    SELECT DISTINCT
        Patologia,
        Es_Cronico
    FROM {{ source('DL_FARINTER','DL_Kielsa_Patologia_Cronico_Excel') }}
    WHERE Es_Cronico = 'X'
),

CTE_Usuarios AS (
    SELECT
        dni,
        MAX(created_at_date) AS created_at_date
    FROM {{ source('DL_FARINTER','DL_MDBKTMPRO_Clinicas_Usuarios') }}
    GROUP BY dni
),

CTE_MAGR AS (
    SELECT
        *,
        DATEDIFF(DAY, Fecha_Ultima_Factura, GETDATE()) AS Recencia
    FROM {{ ref('BI_Kielsa_Agr_Monedero') }}
)

SELECT
    A.Pais_Id,
    A.Cliente_Id,
    A.Trx_Total,
    A.Dias_Total,
    A.Estado_Cliente,
    A.Ticket_Promedio,
    A.PromedioEntre_Trx,
    A.Actividad AS Estado,
    A.Grupo_Ticket AS [Distribución_Monto],
    A.Grupo_Frecuencia AS [Distribución_Frec],
    A.Grupo_Margen AS [Distribución_Margen],
    A.RangoTicketPromedio,
    A.RangoFrecuencia,
    A.RangoMargen,
    A.Lealtad,
    A.Lealtad_Estado,
    A.Tipo_TendFrecuencia AS Tendencia_Frecuencia,
    A.Tipo_TendTicketProm AS Tendencia_Monto,
    A.TipoSucursal_Nombre AS Segmento,
    A.Canal,
    A.Dia_Mes,
    A.Dia_Semana,
    A.Hora,
    A.Tipo_TendMargen AS Tendencia_Rent,
    'No definido' AS Vendedor_Dev,
    A.Sucursal_Nombre AS Sucursal,
    A.Marcas_Propias,
    A.Depto_Articulo AS DeptoArt,
    A.TipoCliente AS TipoCliente_Id,
    A.[Forma Pago] AS Forma_Pago,
    A.Edad,
    A.RangoEdad,
    A.Genero,
    A.Nombre,
    A.Celular,
    A.Telefono,
    A.Tipo_Plan,
    A.Saldo_Puntos,
    A.Enfermedades,
    A.Alerta_Lealtad AS Alerta_Actual,
    ISNULL(MAGR.Fecha_Ultima_Factura, '19000101') AS [Ultima_Compra],
    CASE
        WHEN C.Es_Cronico IS NOT NULL THEN 'Si'
        ELSE 'No'
    END AS Es_Cronico,
    CASE
        WHEN
            A.Alerta_Lealtad = 'Verde'
            AND A.Alerta_Estado_Ayer = 'Roja' THEN 'Verde'
        WHEN
            A.Alerta_Lealtad = 'Naranja'
            AND A.Alerta_Estado_Ayer = 'Verde' THEN 'Naranja'
        WHEN
            A.Alerta_Lealtad = 'Roja'
            AND A.Alerta_Estado_Ayer = 'Naranja' THEN 'Roja'
        ELSE 'Se mantiene'
    END AS Alerta_Transicion,
    CASE
        WHEN
            A.Telefono <> ''
            OR A.Celular <> '' THEN 'Si'
        ELSE 'No'
    END AS Contactable,
    CASE
        WHEN B.[Paciente_C&L] = 1 THEN 'Si'
        ELSE 'No'
    END AS [Paciente_C&L], -- noqa: RF05
    ISNULL(MAGR.Recencia, -1) AS Recencia,
    CASE
        WHEN A.Notificar_Carrito_Pendiente = 1 THEN 'SI'
        ELSE 'NO'
    END AS Notificar_Carrito_Pendiente,
    CASE
        WHEN
            A.Trx_Total = 1
            AND ISNULL(MAGR.Recencia, -1) = 30 THEN 1 --Nuevos sin compra primera vez 30 dias
        WHEN
            A.Trx_Total = 1
            AND ISNULL(MAGR.Recencia, -1) = 60 THEN 2 --Nuevos sin compra segunda vez 60 dias
        WHEN
            A.Trx_Total = 2
            AND ISNULL(MAGR.Recencia, -1) = 45
            /*and Condicion30ConEmail*/
            THEN 0 --Nuevos de 30 dias que compraron el dia 15 despues de cumplir 30 dias
        WHEN
            A.Trx_Total = 2
            AND ISNULL(MAGR.Recencia, -1) = 75
            /*and Condicion60ConEmail*/
            THEN 0 --Nuevos de 60 dias que compraron despues de email el dia 15 despes de cumplir 60 dias
        ELSE 0
    END AS Clientes_Nuevos,
    CASE
        WHEN CONVERT(DATE, D.created_at_date) = CONVERT(DATE, GETDATE()) THEN 1
        ELSE 0
    END AS [Nuevo_C&L], -- noqa: RF05
    CASE
        WHEN ISNULL(MAGR.Fecha_Primer_Factura, '19000101') = '19000101' THEN 1
        ELSE 0
    END AS Sin_Compra
FROM {{ ref('AN_Cal_AtributosCliente_Kielsa') }} AS A
LEFT JOIN {{ source('AN_FARINTER', 'AN_Cal_ClientesTemp_Kielsa') }} AS B
    ON
        A.Pais_Id = B.Pais_Id
        AND A.Cliente_Id = B.Cliente_Id
LEFT JOIN CTE_Cronico AS C
    ON A.Enfermedades = C.Patologia
LEFT JOIN CTE_Usuarios AS D
    ON A.Cliente_Id = D.dni
LEFT JOIN CTE_MAGR AS MAGR
    ON
        A.Pais_Id = MAGR.Emp_Id
        AND A.Cliente_Id = MAGR.Monedero_Id
