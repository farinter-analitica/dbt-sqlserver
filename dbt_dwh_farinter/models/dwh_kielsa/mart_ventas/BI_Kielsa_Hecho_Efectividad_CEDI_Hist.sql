{% set unique_key_list = ["Emp_Id","Sucursal_Id","Articulo_Id", "Efectividad_Id", "Fecha_Id",] %}

{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario"],
		materialized="incremental",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="ignore",
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
        "{{ dwh_farinter_remove_incremental_temp_table() }}",
        "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
        ]
	) 
}}

{%- if is_incremental() %}
	{% set last_date = run_single_value_query_on_relation_and_return(query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -120, max(Fecha_Actualizado)), 112), '19000101')  from  """ ~ this, relation_not_found_value='19000101'|string)|string %}
{%- else %}
	{% set last_date = '19000101' %}
{%- endif %}
--
WITH DespachoD AS (
	             SELECT
						DesDet.Emp_Id as Emp_Id,
						DesDet.Suc_Id as Sucursal_Id,
			            DesDet.Articulo_Id as Articulo_Id,
					    MAX(DEnc.Desp_Usuario_Aplica) as Usuario_Id,
					    MAX(DesDet.CEDI_Id) AS CEDI_Id,
					    DesDet.Pedido_Id as Pedido_Id,
					    MAX(DesDet.SubPedido_Id) as SubPedido_Id,
					    convert(date,DEnc.Desp_Fecha_Aplicado) as Fecha_Id,
		                sum(Pedido.Detalle_Sugerido) as Sugerido,
		                sum(DesDet.Detalle_Cantidad_Pedida) as Pedida,
		                sum(DesDet.Detalle_Cantidad_Auditada) as Despachada,
					    MAX(DesDet.Detalle_Costo_Bruto_Desp) as Costo_Bruto,
					    MAX(DesDet.Detalle_Descuento_Desp) as Descuento 
	               FROM DL_FARINTER.dbo.DL_Kielsa_Inv_Despacho_Detalle DesDet --{{ source('DL_FARINTER', 'DL_Kielsa_Inv_Despacho_Detalle')}}
		                INNER JOIN DL_FARINTER.dbo.DL_Kielsa_Detalle_Pedido Pedido --{{ source('DL_FARINTER', 'DL_Kielsa_Detalle_Pedido')}}
			            ON DesDet.Emp_Id = Pedido.Emp_Id and DesDet.Suc_Id = Pedido.Suc_Id and DesDet.Pedido_Id = Pedido.Pedido_Id 
			               and DesDet.Articulo_Id = Pedido.Articulo_Id
			            INNER JOIN DL_FARINTER.dbo.DL_Kielsa_Inv_Despacho_Encabezado DEnc  --{{ source('DL_FARINTER', 'DL_Kielsa_Inv_Despacho_Encabezado')}}
			            ON DesDet.Emp_Id = DEnc.Emp_Id and DesDet.CEDI_Id = DEnc.CEDI_Id and DesDet.Suc_Id = DEnc.Suc_Id and
			               DesDet.Pedido_Id = DEnc.Pedido_Id and DesDet.SubPedido_Id = DEnc.SubPedido_Id
		           WHERE DEnc.Desp_Estado in ('AP', 'RU', 'RE', 'EN') 
                   {% if is_incremental() -%} 
                   AND DEnc.Desp_Fecha_Aplicado > '{{ last_date }}' 
                   {% else -%}
                   AND DEnc.Desp_Fecha_Aplicado >= '{{ (modules.datetime.datetime.now() - modules.datetime.timedelta(days=360*3)).replace(day=1, month=1, hour=0, minute=0, second=0).strftime('%Y%m%d')  }}'
                   {% endif -%}
		           GROUP BY DesDet.Emp_Id,DesDet.Articulo_Id, DesDet.Suc_Id, DEnc.Desp_Fecha_Aplicado
			      , DesDet.Pedido_Id
),
DespachoE AS (
			SELECT  
						DesDet.Emp_Id as Emp_Id,
			             DesDet.Suc_Id as Sucursal_Id,
					     DesDet.Articulo_Id as Articulo_Id,
					     MAX(DEnc.Desp_Usuario_Anula) as Usuario_Id,
					     MAX(DesDet.CEDI_Id) AS CEDI_Id,
					     DesDet.Pedido_Id as Pedido_Id,
					     MAX(DesDet.SubPedido_Id) as SubPedido_Id,
					     convert(date,DEnc.Desp_Fecha_Anulado) as Fecha_Id,
		                 sum(Pedido.Detalle_Sugerido) as Sugerido,
		                 sum(DesDet.Detalle_Cantidad_Pedida) as Pedida,
		                 sum(DesDet.Detalle_Cantidad_Auditada) as Despachada,
					     MAX(DesDet.Detalle_Costo_Bruto_Desp) as Costo_Bruto,
					     MAX(DesDet.Detalle_Descuento_Desp) as Descuento 
	                FROM DL_FARINTER.dbo.DL_Kielsa_Inv_Despacho_Detalle DesDet --{{ source('DL_FARINTER', 'DL_Kielsa_Inv_Despacho_Detalle')}}
		                 INNER JOIN DL_FARINTER.dbo.DL_Kielsa_Detalle_Pedido Pedido  --{{ source('DL_FARINTER', 'DL_Kielsa_Detalle_Pedido')}}
			             ON DesDet.Emp_Id = Pedido.Emp_Id and DesDet.Suc_Id = Pedido.Suc_Id and DesDet.Pedido_Id = Pedido.Pedido_Id 
			                and DesDet.Articulo_Id = Pedido.Articulo_Id
			             INNER JOIN DL_FARINTER.dbo.DL_Kielsa_Inv_Despacho_Encabezado DEnc --{{ source('DL_FARINTER', 'DL_Kielsa_Inv_Despacho_Encabezado')}}
			             ON DesDet.Emp_Id = DEnc.Emp_Id and DesDet.CEDI_Id = DEnc.CEDI_Id and DesDet.Suc_Id = DEnc.Suc_Id and
			                DesDet.Pedido_Id = DEnc.Pedido_Id and DesDet.SubPedido_Id = DEnc.SubPedido_Id
		           WHERE DEnc.Desp_Estado = 'NO' 
                   {% if is_incremental() -%} 
                   AND DEnc.Desp_Fecha_Aplicado > '{{ last_date }}' 
                   {% else -%}
                   AND DEnc.Desp_Fecha_Aplicado >= '{{ (modules.datetime.datetime.now() - modules.datetime.timedelta(days=360*3)).replace(day=1, month=1, hour=0, minute=0, second=0).strftime('%Y%m%d')  }}'
                   {% endif -%}
				   GROUP BY DesDet.Emp_Id,DesDet.Suc_Id, DesDet.Articulo_Id, DEnc.Desp_Fecha_Anulado,
			        DesDet.Pedido_Id
			      
),
Despachos AS(
SELECT --top 10
						*
						FROM DespachoD DespachoD
						--GROUP BY DespachoD.Emp_Id,DespachoD.Articulo_Id, DespachoD.Sucursal_Id, DespachoD.Usuario_Id, DespachoD.Fecha_Id,DespachoD.Pedido_Id
						UNION ALL 
						SELECT --top 10
						*
						 FROM DespachoE DespachoE
						--GROUP BY DespachoE.Emp_Id,DespachoE.Sucursal_Id, DespachoE.Articulo_Id, DespachoE.Usuario_Id, DespachoE.Fecha_Id,DespachoE.Pedido_Id
						 ),
Despacho AS (
			SELECT Despachos.Emp_Id as Emp_Id,
						Despachos.Sucursal_Id as Sucursal_Id,
			            Despachos.Articulo_Id as Articulo_Id,
					    MAX(Despachos.Usuario_Id) as Usuario_Id,
					    MAX(Despachos.CEDI_Id) AS CEDI_Id,
					    Despachos.Pedido_Id as Pedido_Id,
					    MAX(Despachos.SubPedido_Id) as SubPedido_Id,
					    Despachos.Fecha_Id as Fecha_Id,
		                MAX(Despachos.Sugerido) as Sugerido,
		                MAX(Despachos.Pedida) as Pedida,
		                MAX(Despachos.Despachada) as Despachada,
					    MAX(Despachos.Costo_Bruto) as Costo_Bruto,
					    MAX(Despachos.Descuento) as Descuento
			FROM Despachos

						GROUP BY Despachos.Emp_Id, Despachos.Sucursal_Id , Despachos.Articulo_Id ,
								Despachos.Pedido_Id , Despachos.Fecha_Id
),
Alerta as (
		
SELECT Alerta.Emp_Id,Alerta.Articulo_Id, 
       CASE 
           WHEN Emp_Id = 3 AND MAX(Alerta.Alerta_Id) BETWEEN 3 AND 6 THEN max(Alerta.Alerta_Id)
           WHEN Emp_Id = 2 AND MAX(Alerta.Alerta_Id) BETWEEN 2 AND 4 THEN max(Alerta.Alerta_Id)
           WHEN Emp_Id = 1 AND MAX(Alerta.Alerta_Id) BETWEEN 1 AND 3 THEN max(Alerta.Alerta_Id)
           WHEN Emp_Id = 5 AND MAX(Alerta.Alerta_Id) BETWEEN 27 AND 35 THEN (35 - max(Alerta.Alerta_Id))
           ELSE 0
       END AS PV_Alerta_Id
FROM DL_FARINTER.dbo.DL_Kielsa_Articulo_Alerta Alerta --{{ source('DL_FARINTER', 'DL_Kielsa_Articulo_Alerta')}}
WHERE (Emp_Id = 3 AND Alerta.Alerta_Id BETWEEN 3 AND 6)
   OR (Emp_Id = 2 AND Alerta.Alerta_Id BETWEEN 2 AND 4)
   OR (Emp_Id = 1 AND Alerta.Alerta_Id BETWEEN 1 AND 3)
   OR (Emp_Id = 5 AND Alerta.Alerta_Id BETWEEN 27 AND 35)
GROUP BY Emp_Id,Alerta.Articulo_Id
),
Efectividad AS (
		SELECT  
	             Despacho.Emp_Id as Emp_Id,
		         MAX(Despacho.Sucursal_Id) as Sucursal_Id,
		         MAX(Suc.Zona_Id) as Zona_Id,
		         MAX(Suc.Departamento_Id) as Departamento_Id,
		         MAX(Suc.Municipio_Id) as Municipio_Id,
		         MAX(Suc.Ciudad_Id) as Ciudad_Id,
		         MAX(Suc.TipoSucursal_Id) as TipoSucursal_Id,
		         MAX(ISNULL(ArtInf.Articulo_Id,Art.Articulo_Id)) as Articulo_Id,
		         MAX(ISNULL(Art.Articulo_Codigo_Padre, Despacho.Articulo_Id)) as ArticuloPadre_Id,
		         MAX(ArtInf.Casa_Id) as Casa_Id,
		         MAX(ArtInf.Marca_Id) as Marca_Id,
		         MAX(ArtInf.Categoria_Id) as CategoriaArt_Id,
		         MAX(ArtInf.Depto_Id) as DeptoArt_Id,
		         MAX(ArtInf.SubCategoria_Id) as SubCategoria1Art_Id,
		         MAX(ArtInf.SubCategoria2_Id) as SubCategoria2Art_Id,
		         MAX(ArtInf.SubCategoria3_Id) as SubCategoria3Art_Id,
		         MAX(ArtInf.SubCategoria4_Id) as SubCategoria4Art_Id,
		         MAX(ISNULL(Prov.Proveedor_Id,0)) as Proveedor_Id,
		         MAX(ISNULL(ArtxMec.MecanicaCanje_Id,'x')) as Mecanica_Id,
		         MAX(ISNULL(Alerta.PV_Alerta_Id,0)) as Cuadro_Id,
		         MAX(CASE Art.ABC_Cadena
		              WHEN 'A' then 1 
			          WHEN 'B' then 2
			          WHEN 'C' then 3
			          WHEN 'D' then 4
			          WHEN 'E' then 5
			          else 0 end) as ABCCadena_Id,
		         0 as AlertaInv_Id,
		         0 as DiasInv_Id,
		         MAX(case when Despacho.Usuario_Id = 0 then 'x' else Despacho.usuario_Id end) as Usuario_Id,
				 MAX(convert(nvarchar,Despacho.CEDI_Id) + '-' + convert(nvarchar, Despacho.Pedido_Id)
		             + '-' + convert(nvarchar,Despacho.SubPedido_Id)) as Efectividad_Id,
				CASE 
					WHEN sum(Despacho.Despachada) > 0 
							 AND Despacho.Fecha_Id BETWEEN (GETDATE() - 1) AND GETDATE()  
					THEN 1  
					ELSE 0  
					END AS EstadoDespacho_Id,
		         Despacho.Fecha_Id as Fecha_Id,
		         year(Despacho.Fecha_Id) as Anio_Id,
		         datepart(quarter,Despacho.Fecha_Id) as Trimestre_Id,
		         month(Despacho.Fecha_Id) as Mes_Id,
		         DATEPART(WEEKDAY, DATEADD(DAY, 1, Despacho.Fecha_Id)) AS Dias_Id,
		         day(Despacho.Fecha_Id) as Dia_Id,--/isnull(D.Articulo_Factor_Unitario,1)
	             sum(case when Despacho.Sugerido>0 then Despacho.Sugerido else Despacho.Pedida end) as Efectividad_Sugerido,
		         sum(Despacho.Pedida) as Efectividad_Pedido,
		         sum(Despacho.Despachada) as Efectividad_Despacho,
		         MAX(CASE WHEN Art.Indicador_PadreHijo = 'P' THEN  ISNULL(Art.Factor_Denominador,1) else 1 END) as Efectividad_FactorUnitario,
		         MAX(Despacho.Costo_Bruto) as Efectividad_Costo_Bruto,
		         MAX(Despacho.Descuento) as Efectividad_Descuento,
				 MAX(Suc.EmpSuc_Id) as EmpSuc_Id,
				 MAX(Art.EmpArt_Id) as EmpArt_Id
				 FROM Despacho Despacho
				 INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Sucursal Suc -- {{ref ('BI_Kielsa_Dim_Sucursal')}}
				 ON Despacho.Emp_Id = Suc.Emp_Id AND Despacho.Sucursal_Id = Suc.Sucursal_Id
				 INNER JOIN DL_FARINTER.dbo.DL_Kielsa_Articulo_Info ArtInf --{{ source('DL_FARINTER', 'DL_Kielsa_Articulo_Info')}}
				 ON Despacho.Emp_Id = ArtInf.Emp_Id AND Despacho.Articulo_Id = ArtInf.Articulo_Id
				 LEFT JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Articulo Art -- {{ref ('BI_Kielsa_Dim_Articulo')}}
				 ON Despacho.Emp_Id= Art.Emp_Id AND Despacho.Articulo_Id = Art.Articulo_Id 
				 LEFT OUTER JOIN Alerta Alerta
				 ON Despacho.Emp_Id = Alerta.Emp_Id AND Despacho.Articulo_Id= Alerta.Articulo_Id
				 LEFT OUTER JOIN AN_FARINTER.[dbo].[AN_Cal_CasaXProveedor_Kielsa] Prov --{{ source('AN_FARINTER', 'AN_Cal_CasaXProveedor_Kielsa')}}
				 ON  Despacho.Emp_Id =  Prov.Pais_Id and Art.Casa_Id = Prov.Casa_Id and Despacho.Fecha_Id between Prov.Inicio and Prov.Final
				 LEFT OUTER JOIN DL_FARINTER.[dbo].[DL_TC_ArticuloXMecanica_Kielsa] ArtxMec --{{ ref( 'DL_TC_ArticuloXMecanica_Kielsa')}}
				 ON Despacho.Emp_Id =ArtxMec.Emp_Id and Despacho.Articulo_Id = ArtxMec.Articulo_Id
				      and Despacho.Fecha_Id between Prov.Inicio and Prov.Final

				GROUP BY
	               Despacho.Fecha_Id,
				   Despacho.Emp_Id,
	               Art.Articulo_Id,
	               Suc.Sucursal_Id
					
)	
Select
					Efectividad.Emp_Id,
	               Efectividad.Sucursal_Id, 
	               Efectividad.Zona_Id,
	               Efectividad.Departamento_Id,
	               Efectividad.Municipio_Id,
	               Efectividad.Ciudad_Id,
				   Efectividad.TipoSucursal_id,
	               Efectividad.Articulo_Id, 
	               Efectividad.ArticuloPadre_Id,
	               Efectividad.Casa_Id,
	               Efectividad.Marca_Id,
	               Efectividad.CategoriaArt_Id,
	               Efectividad.DeptoArt_Id,
	               Efectividad.SubCategoria1Art_Id,
	               Efectividad.SubCategoria2Art_Id,
	               Efectividad.SubCategoria3Art_Id,
	               Efectividad.SubCategoria4Art_Id,
				   Efectividad.Proveedor_Id,
				   Efectividad.Cuadro_Id,
				   Efectividad.Mecanica_Id,
				   Efectividad.ABCCadena_Id,
				   Efectividad.AlertaInv_Id,
				   Efectividad.DiasInv_Id,
				   Efectividad.Usuario_Id,
				   Efectividad.Efectividad_Id,
				   Efectividad.EstadoDespacho_Id,
	               Efectividad.Fecha_Id,
				   Efectividad.Anio_Id,
				   Efectividad.Trimestre_Id,
				   Efectividad.Mes_Id,
				   Efectividad.Dias_Id,
				   Efectividad.Dia_Id,
				   Efectividad.Efectividad_Sugerido,
				   CAST(Efectividad.Efectividad_Pedido as INT) as Efectividad_Pedido,
				   CAST(Efectividad.Efectividad_Despacho as INT) as Efectividad_Despacho,
				   Efectividad.Efectividad_FactorUnitario,
				   Efectividad.Efectividad_Costo_Bruto,
				   Efectividad.Efectividad_Descuento,
				   Efectividad.EmpSuc_Id,
				   Efectividad.EmpArt_Id,
				   GETDATE() as Fecha_Actualizado

--INTO #PruebaEfectividad
FROM Efectividad

--SELECT * from #PruebaEfectividad

