
{%- set unique_key_list = ["Monedero_Id","Emp_Id"] -%}
{{ 
    config(
		as_columnstore=false,
		tags=["periodo/diario"],
		materialized="incremental",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="fail",
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
		"{{ dwh_farinter_remove_incremental_temp_table() }}",
		"{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
        "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Fecha_Actualizado']) }}",
		]
		
) }}

{%- set query_empresas -%}
SELECT Empresa_Id, Empresa_Id_Original, Pais_Id
	, LS_LDCOM_RepLocal AS Servidor_Vinculado, D_LDCOM_RepLocal as Base_Datos
	--,LS_LDCOM, D_LDCOM
	--,LS_LDCOM_Replica AS Servidor_Vinculado, D_LDCOM_Replica as Base_Datos
FROM BI_FARINTER.dbo.BI_Kielsa_Dim_Empresa WITH (NOLOCK)
WHERE LS_LDCOM_RepLocal IS NOT NULL and Es_Empresa_Principal = 1
{%- endset -%}
{%- set empresas = run_query_and_return(query_empresas) -%} {# Returns: [{Empresa_Id,Emp_Id_Original,Pais_Id,LS_LDCOM_Replica,D_LDCOM_Replica}] #}

WITH DatosBase
AS
(
	{%- for item in empresas -%}
		{%- if not loop.first %}
		UNION ALL{%- endif %}
	SELECT ISNULL(CAST({{item['Empresa_Id']}} AS SMALLINT),0) AS [Emp_Id]
		,ISNULL([Monedero_Id],0) AS [Monedero_Id] 
		,[Monedero_Nombre] COLLATE DATABASE_DEFAULT AS [Monedero_Nombre]
		,[Monedero_Desde]
		,[Monedero_Hasta]
		,[Monedero_Limite_Uso]
		,[Monedero_Estado]
		,[Monedero_Acum_Sig_Compra]
		,[Monedero_Activacion_Site]
		,[Monedero_Asigna_Puntos_Site]
		,[Monedero_Monto_Minimo]
		,[Monedero_Acumula_Punto_Venta_Monedero]
		,[Monedero_Fec_Actualizacion]
		,[Monedero_Monto_Minimo_Canje]
		,[Aplica_Cliente]
		,[Aplica_Validacion_Edad]
		,[Edad_Minima_Permitida]
		,[Valida_Numero_Tarjeta]
		,[Monedero_Monto_Maximo_Acumulado]
		,[Dias_Inactividad_Limpieza_Saldo]
		,[Marca_Comercial_Id]
		,[Monedero_Acumula_FP_Monedero]
		,[Monedero_Formula_Acum]
		,[Monedero_Hora_Inicio]
		,[Monedero_Hora_Final]
		,[Monedero_Dia1]
		,[Monedero_Dia2]
		,[Monedero_Dia3]
		,[Monedero_Dia4]
		,[Monedero_Dia5]
		,[Monedero_Dia6]
		,[Monedero_Dia7]
		FROM {{item['Servidor_Vinculado']}}.{{item['Base_Datos']}}.dbo.Monedero_Plan
		WHERE Emp_Id = {{item['Empresa_Id_Original']}}
	{%- endfor -%}   
)
SELECT *
	, GETDATE() AS [Fecha_Carga]
	, GETDATE() AS [Fecha_Actualizado]
FROM datosBase