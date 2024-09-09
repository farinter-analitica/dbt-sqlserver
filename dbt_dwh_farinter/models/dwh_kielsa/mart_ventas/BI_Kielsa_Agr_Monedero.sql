
{% set unique_key_list = ["Monedero_Id","Emp_Id"] %}

{% if is_incremental() %}
	{% set v_last_date = run_single_value_query_on_relation_and_return(query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, 0, max(Fecha_Actualizado)), 112), '19000101')  from  """ ~ this, relation_not_found_value='19000101'|string)|string %}
	{% set v_merge_exclude_columns = unique_key_list + ["Fecha_Carga","Fecha_Primer_Factura"]  %}
	{% set v_merge_check_diff_exclude_columns = v_merge_exclude_columns + ["Fecha_Actualizado"]  %}
{% else %}
	{% set v_last_date = '19000101' %}
	{% set v_merge_exclude_columns = unique_key_list + ["Fecha_Carga"]  %}
	{% set v_merge_check_diff_exclude_columns = v_merge_exclude_columns + ["Fecha_Actualizado"]  %}
{% endif %}

{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario"],
		materialized="incremental",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="sync_all_columns",
		merge_exclude_columns= v_merge_exclude_columns,
		merge_check_diff_exclude_columns= v_merge_check_diff_exclude_columns,
		post_hook=[
      "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
		]
		
) }}

  -- depends_on: {{ this }}
{% if is_incremental() %}
WITH Fecha_Desde AS
(
	--Se hace asi ya que cada empresa puede estar actualizando por separado
	SELECT Emp_Id
		,max(Fecha_Actualizado) AS Fecha_Desde
		, YEAR(max(Fecha_Actualizado) ) * 100 + MONTH(max(Fecha_Actualizado) ) AS AnioMes_Id_Desde
	FROM {{ this }}
	GROUP BY Emp_Id
),
{% else %}
WITH
{% endif %}
Fecha_Max AS
(
	--Excluir las facturas del ultimo día (puede estar incompleto, ej. hoy)
	SELECT CAST(MAX(Factura_Fecha) AS DATE) AS Fecha_Max, Emp_Id, MAX(AnioMes_Id) AS AnioMes_Id_Max
	FROM {{source('DL_FARINTER', 'DL_Kielsa_FacturaEncabezado')}}
	GROUP BY Emp_ID
),
Resumen_FacturaEncabezado AS
(
    SELECT MonederoTarj_Id_Limpio AS Monedero_Id
        , FE.Emp_Id
        , MIN(FE.Factura_Fecha) AS Fecha_Primer_Factura
        , MAX(FE.Factura_Fecha) AS Fecha_Ultima_Factura
		, COUNT(FE.Factura_Id) AS Cantidad_Facturas

    FROM {{source('DL_FARINTER', 'DL_Kielsa_FacturaEncabezado')}}  FE
{% if is_incremental() %}
	INNER JOIN Fecha_Max FM
		ON FM.Emp_Id = FE.Emp_Id
		AND FE.Factura_Fecha < FM.Fecha_Max
		AND FE.AnioMes_Id <= FM.AnioMes_Id_Max
	LEFT JOIN Fecha_Desde FD
		ON FD.Emp_Id = FE.Emp_Id
		AND FE.Factura_Fecha > FD.Fecha_Desde
		AND FE.AnioMes_Id >= FD.AnioMes_Id_Desde
	WHERE FD.Emp_Id IS NOT NULL OR FE.Factura_Fecha > '{{ v_last_date }}'
{% endif %}
    GROUP BY FE.MonederoTarj_Id_Limpio, FE.Emp_Id
)   
	SELECT ISNULL(RFE.Monedero_Id,'') Monedero_Id     
		, ISNULL(RFE.Emp_Id,0) Emp_Id  
		, RFE.Fecha_Primer_Factura Fecha_Primer_Factura
		, RFE.Fecha_Ultima_Factura Fecha_Ultima_Factura
{% if is_incremental() %}
		, RFE.Cantidad_Facturas + ISNULL(MAGR.Cantidad_Facturas,0) Cantidad_Facturas
{% else %}
		, RFE.Cantidad_Facturas Cantidad_Facturas
{% endif %}
		, {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Monedero_Id'], input_length=49, table_alias='RFE')}} [EmpMon_Id]
		, GETDATE() AS Fecha_Actualizado
	FROM Resumen_FacturaEncabezado RFE
{% if is_incremental() %}
	LEFT JOIN {{ this }} MAGR
		ON MAGR.Monedero_Id = RFE.Monedero_Id
		AND MAGR.Emp_Id = RFE.Emp_Id
{% endif %}	