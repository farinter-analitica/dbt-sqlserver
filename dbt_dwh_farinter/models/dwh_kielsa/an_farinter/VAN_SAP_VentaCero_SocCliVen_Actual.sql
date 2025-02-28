{% set unique_key_list = ['Sociedad_Id','Cliente_Id','Vendedor_Id'] %}

{{ 
    config(
		tags=["periodo/diario"],
        materialized="view",
    )
}}

-- Solo editable en DBT DAGSTER

SELECT 
       MAX(LEFT(CAST(A.AnioMes_Id AS VARCHAR), 4)) AS Anio,
       MAX(RIGHT(CAST(A.AnioMes_Id AS VARCHAR), 2)) AS Mes,
       A.Sociedad_Id,
       A.Cliente_Id,
       A.Vendedor_Id,
       ISNULL(SUM(CASE WHEN A.Venta = 0 
                    THEN B.Venta 
                    ELSE 0 
             END),0) AS Venta_Efectiva,
       SUM(CASE WHEN A.Venta = 0 
                    THEN A.Ticket_Promedio 
                    ELSE 0 
             END) AS Venta_Potencial,
	  MAX(C.TipoCuenta_Nombre) AS Tipo_Cuenta,
	  MAX(A.Estado) AS Estado
FROM AN_FARINTER.dbo.AN_SAP_Cal_VentaCero A  -- {{ source('AN_FARINTER', 'AN_SAP_Cal_VentaCero') }} A
LEFT JOIN (SELECT 
                    Sociedad_Id,
                    Cliente_Id,
                    CanalDistribucion_Id,
                    AnioMes_Id,
                    Venta,
                    Ticket_Promedio
                FROM AN_FARINTER.dbo.AN_SAP_Cal_VentaCero  -- {{ source('AN_FARINTER', 'AN_SAP_Cal_VentaCero') }} B
                ) B 
       ON A.Sociedad_Id = B.Sociedad_Id
       AND A.Cliente_Id = B.Cliente_Id
       AND A.CanalDistribucion_Id = B.CanalDistribucion_Id
       AND B.AnioMes_Id = CASE
                                               WHEN A.AnioMes_Id % 100 = 12
                                                      THEN (A.AnioMes_Id / 100 + 1) * 100 + 1
                                               ELSE A.AnioMes_Id + 1
                                        END
LEFT JOIN  BI_FARINTER.dbo.BI_SAP_Dim_Cliente C  -- {{ source('BI_FARINTER', 'BI_SAP_Dim_Cliente') }} C
	ON C.Cliente_Id = A.Cliente_Id
WHERE    A.AnioMes_Id = YEAR(GETDATE()) * 100 + MONTH(GETDATE()) 
         --AND A.Sociedad_Id IN ('1200','1300','1301','2500')  
       AND A.GrupoCliente_Id NOT IN ('X','50','44','43','42','41','40','39','38','27','26','25','24','23',
                                                 '22','21','20','19','18','14','13','11')
       AND C.Bloqueo_Pedidos = '' 
       AND C.Bloqueo_Pedidos_Id = '' 
       AND C.Indicador_Grupo_Baja = '' 
       AND C.Indicador_Borrado = ''
       AND C.TipoCuenta_Nombre IN ('Aseguradoras','Clientes','Esporadicos','Farmacias','Financieros','Franquicias','Gobierno','Laboratorios','Otros Deudores')
       AND A.Ultima_Compra <> '1900-01-01'
GROUP BY 
             A.Sociedad_Id,
             A.Cliente_Id,
          A.Vendedor_Id