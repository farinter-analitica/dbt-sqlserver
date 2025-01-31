{% set unique_key_list = ["Regalia_Id",'Suc_Id','Caja_Id','Emp_Id','Detalle_Id'] -%}

{{ 
    config(
        as_columnstore=true,
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
    ) 
}}

{%- if is_incremental() %}
    {%- set last_date = run_single_value_query_on_relation_and_return(query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -7, max(Fecha_Actualizado)), 112), '19000101')  from  """ ~ this, relation_not_found_value='19000101'|string)|string %}
{%- else %}
    {%- set last_date = '19000101' %}
{%- endif %}

with staging as (
    SELECT
        -- Primary Keys and Foreign Keys
        RD.Regalia_Id,
        RD.Emp_Id,
        RD.Suc_Id,
        RD.Caja_Id,
        RD.Detalle_Id,
        RD.Articulo_Id, 
        ART.Articulo_Codigo_Padre AS Articulo_Padre_Id,
        RD.Bodega_Id,
        RD.Consecutivo,
        -- Encabezado
        RE.Cliente_Id,
        RE.Vendedor_Id,
        RE.Identidad_Limpia,
        RE.EmpSucCajReg_Id,
        RE.EmpSuc_Id,
	    RE.EmpCli_Id,
        RE.EmpVen_Id,
        RE.EmpMon_Id,
        RE.Tipo_Origen,
        RE.Operacion_Id,

        -- Date and Time
        RD.Detalle_Fecha as Detalle_Momento,
        CAST(RD.Detalle_Fecha AS DATE) AS Detalle_Fecha,
        DATEPART(HOUR, RD.Detalle_Fecha) AS Detalle_Hora,

        -- Transaction Details
        RD.Detalle_Cantidad as Cantidad_Original,
        CASE WHEN ART.Indicador_PadreHijo = 'H' THEN RD.Detalle_Cantidad/ISNULL(ART.Factor_Denominador, 1)
            ELSE 
            RD.Detalle_Cantidad
            END AS Cantidad_Padre,
        RD.Detalle_Costo_Unitario as Valor_Costo_Unitario,
        RD.Detalle_Precio_Unitario as Precio_Unitario,
        RD.Detalle_Costo_Unitario*Detalle_Cantidad as Valor_Costo_Total,
        RD.Detalle_Total as Valor_Total,
        
        -- Product Information
        --RD.Detalle_Articulo_Nombre as Articulo_Nombre,
       -- RD.Detalle_Interfaz_Autorizacion as Autorizacion,
        RD.Detalle_Impuesto_Porc as Porcentaje_Impuesto,
        RD.Detalle_Impuesto_Porc*RD.Detalle_Total as Valor_Impuesto,

        -- Audit Fields
        RD.Fecha_Carga,
        RD.Fecha_Actualizado

    FROM [DL_FARINTER].[dbo].[DL_Kielsa_Regalia_Detalle] RD --{{ source('DL_FARINTER', 'DL_Kielsa_Regalia_Detalle') }}
    INNER JOIN [BI_Kielsa_Hecho_Regalia_Encabezado] RE --{{ ref('BI_Kielsa_Hecho_Regalia_Encabezado') }}
        ON RD.Regalia_Id = RE.Regalia_Id
        AND RD.Emp_Id = RE.Emp_Id
        AND RD.Suc_Id = RE.Suc_Id
        AND RD.Caja_Id = RE.Caja_Id

    LEFT JOIN [BI_Kielsa_Dim_Articulo] ART -- {{ref ('BI_Kielsa_Dim_Articulo') }} B 
		ON RD.Emp_Id = ART.Emp_Id AND RD.Articulo_Id = ART.Articulo_Id
{% if is_incremental() %}
    WHERE RD.Fecha_Actualizado >= '{{ last_date }}'
{% endif %}
        
)
SELECT *,
	{{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Suc_Id', 'Caja_Id', 'Regalia_Id', 'Detalle_Id'], input_length=49, table_alias='RE')}} AS EmpSucCajRegDet_Id,
	{{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Articulo_Id'], input_length=49, table_alias='RE')}} AS EmpArt_Id
FROM staging RE
