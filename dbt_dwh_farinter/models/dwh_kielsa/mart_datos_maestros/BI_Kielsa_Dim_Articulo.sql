
{# Add dwh_farinter_remove_incremental_temp_table to all incremental models #}
{# unique_key is accessible with config.get('unique_key') but it returns a string #}
{# remember that macro here executes before the model is created, so we can't use it here #}
{% set unique_key_list = ["Articulo_Id","Emp_Id"] %}
{# Post_hook can't access this context variables so we create the string here if needed only if the macros dont depende on query execution (just returns the query text) #}
{#{% set post_hook_dwh_farinter_create_primary_key =  dwh_farinter_create_primary_key(this,columns=unique_key_list, create_clustered=False, is_incremental=0, show_info=True, if_another_exists_drop_it=True)  %}#}
{{ 
    config(
		as_columnstore=false,
		tags=["periodo/diario","periodo/por_hora"],
		materialized="incremental",
		incremental_strategy="farinter_merge",
		on_schema_change="sync_all_columns",
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		unique_key=unique_key_list,
		post_hook=[
      "{{ dwh_farinter_remove_incremental_temp_table() }}",
      "{{ dwh_farinter_create_clustered_columnstore_index(is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
      "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
      "{{ dwh_farinter_create_dummy_data(unique_key=" ~ unique_key_list | tojson ~ ", is_incremental=0) }}"
		]
		
) }}

SELECT
	ISNULL(A.[Articulo_Id],0) AS [Articulo_Id]
	, ISNULL(A.[Emp_Id],0) AS [Emp_Id]
	, A.[Version_Id]
	, A.[Version_Fecha]
	, A.[Nauca_Id]
    , A.[Casa_Id]
    , Casa.Casa_Nombre
    , Dept.DeptoArt_Id
    , Dept.DeptoArt_Nombre
    , Cat.Categoria_Id
    , Cat.Categoria_Nombre
	, A.[Tipo_Articulo_id]
	, A.[Articulo_Nombre]
	, A.[Articulo_Nombre_Corto]
	, A.[Articulo_Codigo_Padre]
	, A.[Articulo_Custom1]
	, A.[Articulo_Puntaje]
	, A.[Articulo_Rotacion_Dias]
	, A.[Articulo_Rotacion_Maximo]
	, A.[Articulo_Rotacion_Minimo]
	, A.[Articulo_Costo_Actual]
	, A.[Articulo_Margen1]
	, A.[Articulo_Precio1]
	, A.[Articulo_Compuesto]
	, A.[Articulo_Activo]
	, A.[Articulo_Vigencia_Inicio]
	, A.[Articulo_Vigencia_Fin]
	, A.[Articulo_Ultimo_costo]
	, A.[Articulo_Sugerencia]
	, A.[Articulo_Cantidad_Minima]
	, A.[Articulo_Costo_Actual_Dolar]
	, A.[Articulo_Ultimo_Costo_Dolar]
	, A.[Articulo_Tipo_Cambio]
	, A.[Articulo_Modelo]
	, A.[Articulo_Costo_Neto]
	, A.[Articulo_Costo_Bruto]
	, A.[Articulo_Venta_Bajo_Costo]
	, A.[Articulo_Hijo_Hereda_Precio]
	, A.[Articulo_Express]
	, A.[Articulo_DevuelveProveedor]
	, A.[Articulo_Limite_Desc]
	, A.[Articulo_Activo_Venta]
	, A.[Articulo_Produccion_Post_Venta]
	, A.[Articulo_Venta_Ecommerce]
	, A.[Articulo_Nombre_Ecommerce]
	, A.[Articulo_Visibilidad]
	, A.[Articulo_SAP]
	, A.[CodigoBarra_Id]
	, A.[PrincipioActivo_Id]
	, ISNULL(A.PrincipioActivo_Nombre, 'No definido') AS PrincipioActivo_Nombre
	, A.[Indicador_PadreHijo]
	, A.[Factor_Numerador]
	, A.[Factor_Denominador]
	, A.[Proveedor_Id]
	, ISNULL(A.Proveedor_Nombre, 'No definido') AS Proveedor_Nombre
	, A.[Cuadro]
	, A.[Hash_ArticuloEmp]
    , ISNULL({{ dwh_farinter_hash_column( columns = unique_key_list, table_alias="A") }},'') AS [HashStr_ArtEmp]   
	, ISNULL(ALERT.Cuadro_Basico,CASE WHEN LEN(A.[Cuadro])=1 THEN 'Cuadro_'+A.[Cuadro] ELSE A.[Cuadro] END) as Cuadro_Meta
	, A.[Cuadro_Fecha]
	, Cat.Hash_CategoriaEmp AS Hash_CatEmp
	, Dept.Hash_DeptoArtEmp
	, SubCat.Hash_SubCatsEmp
	, Casa.Hash_CasaEmp
	, Casa.HashStr_CasaEmp
	, Marca.Hash_MarcaEmp
	-- Handling null replacements
	, ISNULL(A.Mecanica, 'No definido') AS Mecanica
	, ISNULL(A.Sintoma, 'No definido') AS Sintoma
	, ISNULL(A.ABC_Cadena, 'E') AS ABC_Cadena
    , case when Aliados.Articulo_Id is null then 'Estandar' else 'Socio' end as Tipo_Aliado
    , SubCat.SubCategoria1Art_Id
    , SubCat.SubCategoria1Art_Nombre
    , SubCat.SubCategoria2Art_Id
    , SubCat.SubCategoria2Art_Nombre
    , SubCat.SubCategoria3Art_Id
    , SubCat.SubCategoria3Art_Nombre
    , SubCat.SubCategoria4Art_Id
    , SubCat.SubCategoria4Art_Nombre
	, ISNULL(ARTCALC.Bit_Cronico,0)	 Bit_Cronico
	, ISNULL(ARTCALC.Bit_Recomendacion,0) Bit_Recomendacion
	, ISNULL(ARTCALC.Bit_MPA,0) Bit_MPA
	, (CASE WHEN Dept.DeptoArt_Nombre IN ('MP&A CONSUMO','MP&A FARMA') THEN 1 ELSE 0 END) Bit_MPA_Depto
	, (CASE WHEN Dept.DeptoArt_Nombre IN ('MP&A CONSUMO','MP&A FARMA') THEN 1 ELSE 0 END) Bit_Marca_Propia
	, ISNULL(ARTCALC.Alerta_Id_Cronico,0)	 Alerta_Cronico
	, ISNULL(ARTCALC.Alerta_Id_Recomendacion,0) Alerta_Recomendacion
	, CONCAT_WS(','
		,CASE WHEN Dept.DeptoArt_Nombre IN ('MP&A CONSUMO','MP&A FARMA') THEN 'MARCA PROPIA' ELSE NULL END
		,CASE WHEN Dept.DeptoArt_Nombre IN ('MP&A CONSUMO','MP&A FARMA') THEN 'MARCA PROPIA' ELSE NULL END
		,CASE WHEN ARTCALC.Bit_Cronico = 1 THEN 'CRONICO' ELSE NULL END
		, CASE WHEN ARTCALC.Bit_Recomendacion = 1 THEN 'RECOMENDADO' ELSE NULL END)
	 AS Etiquetas
	, {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Articulo_Id'], input_length=29, table_alias='')}} [EmpArt_Id]
    , ISNULL(CAST(GETDATE() AS DATETIME),'19000101') AS [Fecha_Carga]
    , ISNULL(CAST(GETDATE() AS DATETIME),'19000101') AS [Fecha_Actualizado]
FROM {{ source('DL_FARINTER', 'DL_Kielsa_Articulo') }} AS A
LEFT JOIN {{ ref('DL_Kielsa_Categoria_Articulo')}} AS Cat
	ON A.Emp_Id = Cat.Emp_Id AND A.Categoria_Id = Cat.Categoria_Id
LEFT JOIN {{ ref('DL_Kielsa_Departamento_Articulo')}} AS Dept
	ON A.Emp_Id = Dept.Emp_Id AND A.Depto_Id = Dept.DeptoArt_Id
LEFT JOIN {{ ref('DL_Kielsa_Marca')}} AS Marca
	ON A.Emp_Id = Marca.Emp_Id AND A.Marca_Id = Marca.Marca_Id
LEFT JOIN {{ ref('BI_Kielsa_Dim_Casa') }} AS Casa
	ON A.Emp_Id = Casa.Emp_Id AND A.Casa_Id = Casa.Casa_Id
LEFT JOIN {{ source('DL_FARINTER', 'DL_Temp_ArticuloAliados_Kielsa')}} Aliados
	ON A.Articulo_Id = Aliados.Articulo_Id_Solo    
	and A.Emp_Id = Aliados.Emp_Id
LEFT JOIN {{ source('DL_FARINTER', 'DL_Kielsa_Articulo_Calc')}} ARTCALC
	ON A.Articulo_Id = ARTCALC.Articulo_Id    
	and A.Emp_Id = ARTCALC.Emp_Id
LEFT JOIN {{ ref('BI_Kielsa_Dim_Alerta') }} ALERT
	ON ALERT.Emp_ID = A.Emp_Id
	AND ARTCALC.Alerta_Id_Recomendacion = ALERT.Alerta_Id
LEFT JOIN {{ source('BI_FARINTER', 'BI_Kielsa_Dim_ArticuloSubCategorias') }} AS SubCat
	ON A.Emp_Id = SubCat.Emp_Id
	AND A.Categoria_Id = SubCat.CategoriaArt_Id
	AND A.SubCategoria_Id = SubCat.SubCategoria1Art_Id
    AND A.SubCategoria2_Id = SubCat.SubCategoria2Art_Id
    AND A.SubCategoria3_Id = SubCat.SubCategoria3Art_Id
    AND A.SubCategoria4_Id = SubCat.SubCategoria4Art_Id
{% if is_incremental() and run_started_at.strftime('%H') | int >= 8 and run_started_at.strftime('%H') | int < 18 %}
  WHERE A.Version_Fecha >= (SELECT CAST(MAX(Fecha_Actualizado) AS DATE) FROM {{this}})
{% else %}
  --FULL
{% endif %}