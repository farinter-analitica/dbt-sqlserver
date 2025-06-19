{% set unique_key_list = ["almacen_id","casa_id","material","periodo","gpo_cliente"] %}
{{ 
    config(
		tags=["periodo/diario"],
		materialized="view",
		post_hook=[
        "{{ dwh_farinter_remove_incremental_temp_table() }}",
        ]
	) 
}}

-- Solo editable en DBT
-- =============================================
-- Author:		<Axell Padilla>
-- Create date: <2022-08-26>
-- Description:	<Vista de tabla, no cambiar nombres de columnas debido a aplicaciones en Productivo>
-- =============================================
-- <2023-07-20><Axell Padilla> Cambios de datos de credito por nueva estructura con credito exacto y fecha de modificación de limite de credito
SELECT --TOP (1000) 
	   A.[Sociedad_Id] -- Clave
      ,A.[Cliente_Id] -- Clave
      ,A.[Ultima_Compra] --Fecha
	  ,A.Primera_Compra
      ,A.[Ticket_Promedio] -- 
      ,A.[PromedioEntre_Trx] -- -1 = no tiene promedio
      ,A.[Estado] 
      ,A.[Estado_Anterior] 
      ,A.[Distribucion_Monto] --A,B,C,D
      ,A.[Distribucion_Frec] --A,B,C,D
      ,A.[Lealtad] --Alta,Media,Baja, No Leal
      ,A.[Lealtad_Estado]
		,A.Lealtad_Estado_Anterior
		,A.Lealtad_Estado_UltimoCambio
	,A.[Tendencia_Frecuencia] -- Alza, Constante, Cambiante, Baja
      ,A.[Tendencia_Monto]  -- Alza, Constante, Cambiante, Baja
      ,A.[Horario] --Ultima Hora por la cual se le vende más
      ,A.[Tendencia_Rent]  -- Alza, Constante, Cambiante, Baja
      --,[Tendencia_Rep]
      --,[Vendedor_Devol]
      --,[Sucursal_Id]
      --,[Marcas_Propias]
      --,[Categoria_Articulo]  --Ultima categoria o grupo de materiales de venta que se le vende más / Ej. Exclusivo, Marcas Propias
      ,A.[Grupo_Nombre] as Grupo--nombre --Ultimo Grupo de clientes por el cual se le vende más
      ,A.[Canal_Nombre] AS Canal-- nombre --Ultimo Canal de distribución por el cual se le vende más
      ,A.[Categoria_Articulo_Nombre] as Categoria_Articulo --nombre
      ,A.[Telefono] 
	  ,E.Correo_e 
	  , E.Tipo_Cuenta
      ,A.[Cliente_Nombre] --nombre
      ,A.[Direccion]
      ,A.[Cliente_Tipo] --tipo de cliente ejemplo Empleados, Clientes, Empresas
	  ,A.[Trx_Total] --Suma de trx totales desde la primera compra
	  ,A.[Ciclo_Vida] --Mide la duración del utlimo ciclo de vida
	  --,[Zona_Id]
	  ,A.[Zona_Nombre]
	  ,A.[Dia_Mes] --Es el número  del día del mes en que mas compra el cliente
	  ,A.[Dia_Semana] --Es el día de la semana en que mas compra el cliente
	  ,A.[Dias_Muestra]--Es la cantidad de días para la muestra
	  ,A.[Dias_Total]--Es la cantidad de días en que ha realizado compras
	  ,A.[Recencia]-- Es el periodo entre la última compra y hoy
	  ,A.[Limite_Dias_Razonable_Recencia]-- Es el tiempo razonable esperado entre trx para que vuelva a comprar
	  ,A.[Estado_Cliente_Anterior] -- Es el estado del periodo anterior del cliente -- Puede entenderse el flujo de estados de esta manera: Nuevo->Activo->Por Perderse->Abandona->Perdido->Pronto Inactivo->Inactivo->Vuelve->
	  ,A.[Alerta_Estado] -- Es la agrupación del estado del cliente en un semáforo de verde, anaranjado y rojo
	  ,A.[Alerta_Estado_Anterior] -- Es la agrupación del estado del cliente en el mes anterior en un semáforo de verde, anaranjado y rojo
	  ,A.[Periodo_Compra] -- Es la cantidad de meses que ha comprado el cliente.

	  ,A.[Grupo_Utilidad] -- Es la agrupación de la Utilidad por medio de percentil.
	  ,A.[Grupo_Margen] -- Es la agrupación del Margen por medio de percentil.
	  ,A.Vendedor_Id
	  ,A.Vendedor as Vendedor
	  ,case when F.Cliente_Id is not null and F.CondicionPago_Id = 'Z000' then 'Contado' 
	        when CCALC.Valor_Limite_Credito < CCALC.Valor_Exposicion_Credito then 'Excedido' 
			when C.Indicador_Bloqueado = 'X' then 'Cerrado' 
			when C.Indicador_Bloqueado = '' then 'Abierto' 
			else 'No aplica' end as Credito_Estado
	  ,isnull(C.Fecha_Modificacion_Limite,'1900-01-01') as Fecha_Modificacion_Limite_Credito
	  ,isnull(CCALC.Valor_Limite_Credito,0) as Limite_Credito
	  ,ISNULL(CCALC.Valor_Exposicion_Credito,0) AS Credito_Consumido
	  ,ISNULL(CCALC.Valor_Limite_Credito - CCALC.Valor_Exposicion_Credito,0) AS Credito_Disponnible
	  ,isnull(B.Tipo_Pagador, 'No Aplica') as Tipo_Pagador
	  , case when D.Cliente_Id is null then 'Sin pagos pendientes'
	        when isnull(D.Deuda, 0) > 0 and isnull(D.Dias_Promedio, 0) 
			     between isnull(D.Promedio_Plazos,0)*0.8 and isnull(D.Promedio_Plazos,0) then 'Cerca de pagar'
	        when isnull(D.Deuda, 0) > 0 and isnull(D.Dias_Promedio, 0) <= isnull(D.Promedio_Plazos,0) then 'Pendiente de pago'
			when isnull(D.Deuda, 0) > 0 and isnull(D.Dias_Promedio, 0) > isnull(D.Promedio_Plazos,0) then 'Pago vencido'
			when isnull(D.Deuda, 0) < 0 then 'Devoluciones'
			when isnull(D.Deuda, 0) = 0 then 'Sin Pagos pendientes'
			else 'No aplica' end as Pagos_Estado
		, ISNULL(B.Promedio,-1) AS Promedio_Pagos_Dias
	  , ISNULL(B.Promedio_Plazos,-1) AS Promedio_PlazoCredito_Dias	
	  , case when E.Registro_Tributario in ('08019003000364', '080190003000364', '80119003000364', '0801900300364', '8019003000364',
	   'RE9HRI-T', '08019995347622', '08011900300036', '0801900300364', '05019999180979', '08019019139296', '08019009230199')
	   then 'Si' else 'No' end as Competencia
	   , E.Registro_Tributario
	   , A.Valor_Venta_Total_Mes --Venta en el mes
	   , A.Region_Id --Region Preferida
	   , A.Fecha_Actualizacion
		,A.Valor_Venta_Total_24Meses_Ultima_Trx
		,A.Valor_Venta_Promedio_24Meses_Ultima_Trx

  FROM [AN_FARINTER].[dbo].[AN_Cal_AtributosCliente_SAP] A --{{source('AN_FARINTER','AN_Cal_AtributosCliente_SAP')}}
	   INNER JOIN BI_FARINTER.[dbo].[BI_SAP_Dim_Sociedad] S --{{ref('BI_SAP_Dim_Sociedad')}}
	   on A.Sociedad_Id = S.Sociedad_Id 
	  INNER JOIN BI_FARINTER.dbo.BI_Dim_Cliente_SAP E --{{source('BI_FARINTER','BI_Dim_Cliente_SAP')}}
	  on A.Cliente_Id = E.Cliente_Id
	   LEFT JOIN BI_FARINTER.[dbo].[BI_SAP_Dim_ClienteAreaCredito] C --{{source('BI_FARINTER','BI_SAP_Dim_ClienteAreaCredito')}}
	   on S.AreaCredito_Id = C.AreaCredito_Id and A.Cliente_Id = C.Cliente_Id
	   LEFT JOIN BI_FARINTER.[dbo].[BI_SAP_Dim_ClienteAreaCredito_Calc] CCALC --{{source('BI_FARINTER','BI_SAP_Dim_ClienteAreaCredito_Calc')}}
	   on S.AreaCredito_Id = CCALC.AreaCredito_Id and A.Cliente_Id = CCALC.Cliente_Id
	   LEFT JOIN AN_FARINTER.dbo.AN_SAP_Cal_ClientesRecordCredito B --{{source('AN_FARINTER','AN_SAP_Cal_ClientesRecordCredito')}}
	   ON B.AnioMes_Id = year(getdate())*100+month(getdate()) and A.Sociedad_Id = B.Sociedad_Id and A.Cliente_Id = B.Cliente_Id AND A.Canal=B.CanalDistribucion_Id
	   LEFT JOIN (select 
	                     Sociedad_Id,
	                     Cliente_Id,
	                     sum(case when left(Factura_Id,2)<>'06' then 
                                       1 else 0 end) as Facturas,
	                     min(Factura_Fecha) as Primer_Factura,
	                     max(Factura_Fecha) as Ultima_Factura,
	                     avg(datediff(day, Factura_Fecha, getdate())) as Dias_Promedio,
	                     AVG(Plazo) as Promedio_Plazos,
	                     sum(case when left(Factura_Id,2)<>'06' then 
                                       Pago_Valor else (-1)*Pago_Valor end) as Deuda
                         from DL_FARINTER.dbo.DL_SAP_Acum_PagosCreditoVentasHist --{{source('DL_FARINTER','DL_SAP_Acum_PagosCreditoVentasHist')}}
                              where AnioMes_Id = 190001 and Sociedad_Id in ('1200', '1300', '1301', '1700')
						               --and Cliente_Id = '0000101396'
                         group by Sociedad_Id, Cliente_Id) D
	  on A.Sociedad_Id = D.Sociedad_Id and A.Cliente_Id = D.Cliente_Id
	  LEFT JOIN DL_FARINTER.[dbo].[DL_SAP_Acum_JerarquiaClientes] F --{{source('DL_FARINTER','DL_SAP_Acum_JerarquiaClientes')}}
	  on A.Sociedad_Id = F.Sociedad_Id and A.Cliente_Id = F.Cliente_Id and A.Grupo = F.GrupoCliente_Id
	  AND F.CanalDistribucion_Id=A.Canal 
	  
	--	SELECT TOP 1000 * FROM [dbo].[VAN_Cal_AtributosCliente_SAP]  WHERE Cliente_Id ='0000100297'
	-- SELECT TOP 1000 * FROM AN_FARINTER.dbo.AN_SAP_Cal_ClientesRecordCredito B  WHERE Cliente_Id ='0000100297' AND AnioMes_Id =202310