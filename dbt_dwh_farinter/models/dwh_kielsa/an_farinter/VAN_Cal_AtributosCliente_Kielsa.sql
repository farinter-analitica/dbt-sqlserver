{% set unique_key_list = ['Pais_Id','Cliente_Id'] %}

{{ 
    config(
       group="kielsa_analitica_atributos",
       tags=["automation/periodo_diario","periodo_unico/si", "automation_only"],
       materialized="view",
    )
}}

-- Solo editable en DBT DAGSTER
-- llaves: {{ unique_key_list }}
SELECT A.Pais_Id,
       A.Cliente_Id,
       ISNULL(MAGR.Fecha_Ultima_Factura, '19000101') Ultima_Compra,
       A.Trx_Total,
       A.Dias_Total,
       A.Ticket_Promedio,
       A.PromedioEntre_Trx,
       A.Actividad AS Estado,
       A.Grupo_Ticket AS Distribución_Monto,
       A.Grupo_Frecuencia AS Distribución_Frec,
       A.Lealtad,
       A.Lealtad_Estado,
       A.Tipo_TendFrecuencia AS Tendencia_Frecuencia,
       A.Tipo_TendTicketProm AS Tendencia_Monto,
       A.TipoSucursal_Nombre AS Segmento,
       A.Canal AS Canal,
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
       A.Genero,
       A.Nombre,
       A.Celular,
       A.Telefono,
       A.Tipo_Plan,
       A.Saldo_Puntos,
       A.Enfermedades,
       CASE
              WHEN C.Es_Cronico IS NOT NULL THEN 'Si'
              ELSE 'No'
       END AS Es_Cronico,
       CASE
              WHEN A.Alerta_Lealtad = 'Verde'
              AND A.Alerta_Estado_Ayer = 'Roja' THEN 'Verde'
              WHEN A.Alerta_Lealtad = 'Naranja'
              AND A.Alerta_Estado_Ayer = 'Verde' THEN 'Naranja'
              WHEN A.Alerta_Lealtad = 'Roja'
              AND A.Alerta_Estado_Ayer = 'Naranja' THEN 'Roja'
              ELSE 'Se mantiene'
       END AS Alerta_Transicion,
       A.Alerta_Lealtad AS Alerta_Actual,
       CASE
              WHEN A.Telefono <> ''
              OR A.Celular <> '' THEN 'Si'
              ELSE 'No'
       END AS Contactable,
       CASE
              WHEN B.[Paciente_C&L] = 1 THEN 'Si'
              ELSE 'No'
       END AS [Paciente_C&L],
       ISNULL(MAGR.Recencia, -1) AS Recencia,
       CASE
              WHEN A.Notificar_Carrito_Pendiente = 1 THEN 'SI'
              ELSE 'NO'
       END Notificar_Carrito_Pendiente,
       CASE
              WHEN A.Trx_Total = 1
              AND ISNULL(MAGR.Recencia, -1) = 30 THEN 1 --Nuevos sin compra primera vez 30 dias
              WHEN A.Trx_Total = 1
              AND ISNULL(MAGR.Recencia, -1) = 60 THEN 2 --Nuevos sin compra segunda vez 60 dias
              WHEN A.Trx_Total = 2
              AND ISNULL(MAGR.Recencia, -1) = 45
              /*and Condicion30ConEmail*/
              THEN 0 --Nuevos de 30 dias que compraron el dia 15 despues de cumplir 30 dias
              WHEN A.Trx_Total = 2
              AND ISNULL(MAGR.Recencia, -1) = 75
              /*and Condicion60ConEmail*/
              THEN 0 --Nuevos de 60 dias que compraron despues de email el dia 15 despes de cumplir 60 dias
              ELSE 0
       END AS Clientes_Nuevos,
       CASE
              WHEN CONVERT(DATE, D.created_at_date) = CONVERT(DATE, GETDATE()) THEN 1
              ELSE 0
       END AS [Nuevo_C&L],
       CASE
              WHEN ISNULL(MAGR.Fecha_Primer_Factura, '19000101') = '19000101' THEN 1
              ELSE 0
       END AS Sin_Compra
FROM AN_FARINTER.dbo.AN_Cal_AtributosCliente_Kielsa A -- {{ ref('AN_Cal_AtributosCliente_Kielsa') }}
       LEFT JOIN AN_FARINTER.dbo.AN_Cal_ClientesTemp_Kielsa B -- {{ source('AN_FARINTER', 'AN_Cal_ClientesTemp_Kielsa') }}
       ON A.Pais_Id = B.Pais_Id 
       AND A.Cliente_Id = B.Cliente_Id
       LEFT JOIN (
              SELECT DISTINCT Patologia,
                     Es_Cronico
              FROM [DL_FARINTER].[dbo].[DL_Kielsa_Patologia_Cronico_Excel] --{{source('DL_FARINTER','DL_Kielsa_Patologia_Cronico_Excel')}}
              WHERE Es_Cronico = 'X'
       ) C ON A.Enfermedades = C.Patologia
       LEFT JOIN (
              SELECT DISTINCT dni,
                     MAX(created_at_date) AS created_at_date
              FROM [DL_FARINTER].[dbo].[DL_MDBKTMPRO_Clinicas_Usuarios] --{{source('DL_FARINTER','DL_MDBKTMPRO_Clinicas_Usuarios')}}
              GROUP BY dni
       ) D ON A.Cliente_Id = D.dni
       LEFT JOIN (
              SELECT *,
                     DATEDIFF(DAY, Fecha_Ultima_Factura, GETDATE()) AS Recencia
              FROM BI_FARINTER.dbo.BI_Kielsa_Agr_Monedero MAGR -- {{ ref('BI_Kielsa_Agr_Monedero') }} MAGR
       ) MAGR ON MAGR.Emp_Id = A.Pais_Id
       AND A.Cliente_Id = MAGR.Monedero_Id