{%- set unique_key_list = ["Suc_Id","Caja_Id", "Emp_Id"] -%}
{{ 
	config(
		as_columnstore=true,
		tags=["periodo/diario", "detener_carga/si"],
		materialized="incremental",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
			"{{ dwh_farinter_remove_incremental_temp_table() }}",
			"{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
		]
) }}

{%- set query_empresas -%}
SELECT Empresa_Id, Empresa_Id_Original, Pais_Id
	,LS_LDCOM_Replica AS Servidor_Vinculado, D_LDCOM_Replica as Base_Datos
FROM BI_FARINTER.dbo.BI_Kielsa_Dim_Empresa WITH (NOLOCK)
WHERE LS_LDCOM_RepLocal IS NOT NULL and Es_Empresa_Principal = 1
{%- endset -%}
{%- set empresas = run_query_and_return(query_empresas) -%}

{%- set valid_empresas = [] -%}
{%- for item in empresas -%}
				{%- if check_linked_server(item['Servidor_Vinculado']) -%}
								{%- do valid_empresas.append(item) -%}
				{%- endif -%}
{%- endfor -%}

WITH DatosBase
AS
(
{%- for item in valid_empresas -%}
{%- if not loop.first %}
	UNION ALL{%- endif %}
	SELECT 
		ISNULL({{item['Empresa_Id']}},0) AS [Emp_Id]
		, ISNULL(CAST(Suc_Id AS INT),0) AS Suc_Id
		, ISNULL(CAST(Caja_Id AS INT),0) AS Caja_Id
		, ISNULL(CAST(Bodega_Apartado AS INT),0) AS Bodega_Apartado
		, ISNULL(CAST(Bodega_Factura AS INT),0) AS Bodega_Factura
		, ISNULL(CAST(Cliente_Id AS INT),0) AS Cliente_Id
		, ISNULL(CAST(Tipo_Id AS INT),0) AS Tipo_Id
		, ISNULL(CAST(SubDoc_Id AS INT),0) AS SubDoc_Id
		, ISNULL(CAST(Vendedor_Id AS INT),0) AS Vendedor_Id
		, Caja_Nombre COLLATE DATABASE_DEFAULT AS [Caja_Nombre]
		, ISNULL(CAST(Caja_Factura AS INT),0) AS Caja_Factura
		, ISNULL(CAST(Caja_Recibo AS INT),0) AS Caja_Recibo
		, ISNULL(CAST(Caja_Operacion AS INT),0) AS Caja_Operacion
		, ISNULL(CAST(Caja_Devolucion AS INT),0) AS Caja_Devolucion
		, ISNULL(CAST(Caja_Muestra_Error AS BIT),0) AS Caja_Muestra_Error
		, ISNULL(CAST(Caja_Utilizada AS BIT),0) AS Caja_Utilizada
		, Caja_Host COLLATE DATABASE_DEFAULT AS Caja_Host
		, Caja_Apartado_Pago COLLATE DATABASE_DEFAULT AS Caja_Apartado_Pago
		, ISNULL(CAST(Caja_Facturacion_Negativa AS BIT),0) AS Caja_Facturacion_Negativa
		, Caja_Facturacion_Pago COLLATE DATABASE_DEFAULT AS Caja_Facturacion_Pago
		, ISNULL(CAST(Caja_Cantidad_Maxima_Linea AS INT),0) AS Caja_Cantidad_Maxima_Linea
		, ISNULL(CAST(Caja_Monto_Maximo_Linea AS MONEY),0) AS Caja_Monto_Maximo_Linea
		, ISNULL(CAST(Caja_Monto_Maximo_Facturado AS MONEY),0) AS Caja_Monto_Maximo_Facturado
		, ISNULL(CAST(Caja_Monto_Maximo_Acumulado AS MONEY),0) AS Caja_Monto_Maximo_Acumulado
		, ISNULL(CAST(Caja_Monto_Maximo_Acumulado_Bloqueo AS MONEY),0) AS Caja_Monto_Maximo_Acumulado_Bloqueo
		, ISNULL(Caja_Fec_Actualizacion, '1900-01-01') AS Caja_Fec_Actualizacion
		, ISNULL(CAST(Caja_Express AS BIT),0) AS Caja_Express
		, ISNULL(CAST(Caja_Apartado AS INT),0) AS Caja_Apartado
		, ISNULL(CAST(Caja_Recibo_Servicio AS INT),0) AS Caja_Recibo_Servicio
		, ISNULL(Caja_Fecha_Sincronizacion, '1900-01-01') AS Caja_Fecha_Sincronizacion
		, ISNULL(CAST(Caja_Enviar_Facturas_MonitorDesp AS BIT),0) AS Caja_Enviar_Facturas_MonitorDesp
		, ISNULL(CAST(Caja_Ticket AS INT),0) AS Caja_Ticket
		, ISNULL(CAST(Caja_ConsumidorFinal AS INT),0) AS Caja_ConsumidorFinal
		, ISNULL(CAST(Caja_CreditoFiscal AS INT),0) AS Caja_CreditoFiscal
		, ISNULL(CAST(Caja_Nota AS INT),0) AS Caja_Nota
		, ISNULL(CAST(Caja_Anulacion AS INT),0) AS Caja_Anulacion
		, ISNULL(Caja_Fec_Ult_Cierre_Z, '1900-01-01') AS Caja_Fec_Ult_Cierre_Z
		, ISNULL(Caja_Fec_Ult_Cierre_Z_Mensual, '1900-01-01') AS Caja_Fec_Ult_Cierre_Z_Mensual
		, ISNULL(Caja_Fec_Primer_Tiquete, '1900-01-01') AS Caja_Fec_Primer_Tiquete
		, ISNULL(CAST(Caja_Journal AS BIT),0) AS Caja_Journal
		, ISNULL(CAST(Caja_Transaccion_Ficohsa AS INT),0) AS Caja_Transaccion_Ficohsa
	FROM {{item['Servidor_Vinculado']}}.{{item['Base_Datos']}}.dbo.Caja C
	WHERE Emp_Id = {{item['Empresa_Id_Original']}}
	{% if is_incremental() %}
	  and C.Caja_Fec_Actualizacion >= coalesce((select max(Fecha_Actualizado) from {{ this }}), '1900-01-01')
	{% endif %}
{% endfor -%}
)
SELECT *
	, ISNULL({{ dwh_farinter_concat_key_columns(unique_key_list, 50)  }}, '') AS SucCajEmp_Id
	, ISNULL({{ dwh_farinter_hash_column(unique_key_list) }},'')   AS Hash_SucCajEmp
	, GETDATE() AS [Fecha_Carga]
	, GETDATE() AS [Fecha_Actualizado]
FROM DatosBase