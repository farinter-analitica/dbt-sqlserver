{%- set nombre_esquema_particion = "ps_" + this.identifier + "_fecha" -%}
{%- set on_clause = nombre_esquema_particion ~ "([Fecha_Id])" -%}
{%- set unique_key_list = [
    "Fecha_Id", "Factura_Id", "Suc_Id", "Emp_Id", "Caja_Id", "Tipo_Id", "Pago_Id", "TipoDoc_id",
] -%}

{{-
    config(
        as_columnstore=true,
        tags=["periodo/diario","automation/periodo_por_hora"],
        materialized="incremental",
        incremental_strategy="farinter_merge",
        unique_key=unique_key_list,
        on_schema_change="append_new_columns",
        on_clause_filegroup=on_clause,
        merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
        merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
        pre_hook=[
            "{%- if is_incremental() -%}
				 --Incremental
			{%- else -%}
					EXEC ADM_FARINTER.dbo.pa_inicializar_particiones
						@p_base_datos = '{{this.database}}',
						@p_nombre_esquema_particion = 'ps_{{this.identifier}}_fecha',
						@p_nombre_funcion_particion = 'pf_{{this.identifier}}_fecha',
						@p_periodo_tipo = 'Anual', --'Anual' o 'Mensual'
						@p_tipo_datos = 'Fecha', --'Fecha' o 'AnioMes' o 'FechaHora'
						@p_fecha_base = '2018-01-01'
			{%- endif -%}"
            ],
        post_hook=[
            "{{ dwh_farinter_remove_incremental_temp_table() }}",
            "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ",
                create_clustered=false,
                is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
            "{{ dwh_farinter_create_index(is_incremental=is_incremental(),
                columns=['Fecha_Actualizado'],
                included_columns=['Fecha_Id']) }}",
            "EXEC ADM_FARINTER.dbo.pa_comprimir_indices_particiones_anteriores @p_base_datos = '{{ this.database }}',
                @p_esquema_tabla = '{{ this.schema }}',
                @p_nombre_tabla = '{{ this.identifier }}',
                @p_tipo_datos = 'Fecha';"
        ]
    ) 
}}

{%- set query_empresas -%}
SELECT Empresa_Id, Empresa_Id_Original, Pais_Id
,LS_LDCOM_Replica AS Servidor_Vinculado, D_LDCOM_Replica as Base_Datos
FROM BI_FARINTER.dbo.BI_Kielsa_Dim_Empresa WITH (NOLOCK)
WHERE LS_LDCOM_Replica IS NOT NULL and Es_Empresa_Principal = 1
{%- endset -%}
{%- set empresas = run_query_and_return(query_empresas) -%}

{%- set valid_empresas = [] -%}
{%- for item in empresas -%}
    {%- if check_linked_server(item['Servidor_Vinculado']) -%}
        {%- do valid_empresas.append(item) -%}
    {%- endif -%}
{%- endfor -%}

{%- set v_dias_base = 366 -%}
{%- set v_fecha_inicio = (
    var('P_FECHADESDE_INC')
    or (modules.datetime.datetime.now() - modules.datetime.timedelta(days=v_dias_base))
        .strftime('%Y%m%d')
) -%}
{%- set v_fecha_fin = (
    var('P_FECHAHASTA_EXC') 
    or (modules.datetime.datetime.now() + modules.datetime.timedelta(days=1))
        .strftime('%Y%m%d')
) -%}

WITH datosBase AS (
    {%- for item in valid_empresas -%}
        {%- if not loop.first %} UNION ALL {%- endif %}
        {%- if is_incremental() and not var('P_FECHADESDE_INC') -%}
            {%- set v_query_fecha_incremental -%}
                select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -7, max(Fecha_Actualizado)), 112), '{{ v_fecha_inicio }}') as a 
                from  {{ this }} where Emp_Id = {{ item['Empresa_Id'] }}
            {%- endset -%}
            {%- set v_fecha_inicio = run_single_value_query_on_relation_and_return(
                query=v_query_fecha_incremental,
                relation_not_found_value=v_fecha_inicio|string
            )|string -%}
        {%- endif %}
        SELECT
            ISNULL(CAST({{ item['Empresa_Id'] }} AS SMALLINT), 0) AS [Emp_Id],
            ISNULL(CAST([Pago_Fecha] AS DATE), '1900-01-01') AS [Fecha_Id],
            ISNULL([Consecutivo], 0) AS [Consecutivo_Factura],
            ISNULL(0, 0) AS [Consecutivo_Ticket],
            ISNULL([TipoDoc_id], 0) AS [TipoDoc_id],
            ISNULL([Suc_Id], 0) AS [Suc_Id],
            ISNULL([Caja_Id], 0) AS [Caja_Id],
            ISNULL([Factura_Id], 0) AS [Factura_Id],
            ISNULL([Tipo_Id], 0) AS [Tipo_Id],
            ISNULL([Pago_Id], 0) AS [Pago_Id],
            [Pago_Fecha],
            [Pago_Numero] COLLATE DATABASE_DEFAULT AS [Pago_Numero],
            [Pago_Descripcion] COLLATE DATABASE_DEFAULT AS [Pago_Descripcion],
            [Pago_Aprovacion] COLLATE DATABASE_DEFAULT AS [Pago_Aprovacion],
            [Moneda_Id],
            [Banco_Id],
            [MonederoTarj_Id] COLLATE DATABASE_DEFAULT AS [MonederoTarj_Id],
            [Tarjeta_id],
            [Pago_Total],
            [Pago_Total_Moneda],
            [Pago_Total_Tipo_Cambio],
            [Pago_Suc_Ref],
            [Pago_Caj_Ref],
            [Pago_Fac_Ref],
            [Pago_Cupon_Automatico],
            [Cierre_Id],
            [Pago_Fecha_Vencimiento] COLLATE DATABASE_DEFAULT AS [Pago_Fecha_Vencimiento],
            [Autorizador_Id],
            [Pago_Anulado],
            [Tarjeta_Impuesto]
        FROM {{ item['Servidor_Vinculado'] }}.{{ item['Base_Datos'] }}.dbo.Factura_Forma_Pago WITH (NOLOCK)
        WHERE
            Emp_Id = {{ item['Empresa_Id_Original'] }}
            AND (Pago_Fecha >= '{{ v_fecha_inicio }}' AND Pago_Fecha < '{{ v_fecha_fin }}')
        UNION ALL
        SELECT
            ISNULL(CAST({{ item['Empresa_Id'] }} AS SMALLINT), 0) AS [Emp_Id],
            ISNULL(CAST([Pago_Fecha] AS DATE), '1900-01-01') AS [Fecha_Id],
            ISNULL(0, 0) AS [Consecutivo_Factura],
            ISNULL([Consecutivo], 0) AS [Consecutivo_Ticket],
            ISNULL([TipoDoc_id], 0) AS [TipoDoc_id],
            ISNULL([Suc_Id], 0) AS [Suc_Id],
            ISNULL([Caja_Id], 0) AS [Caja_Id],
            ISNULL([Ticket_Id], 0) AS [Factura_Id],
            [Tipo_Id],
            [Pago_Id],
            [Pago_Fecha],
            [Pago_Numero] COLLATE DATABASE_DEFAULT AS [Pago_Numero],
            [Pago_Descripcion] COLLATE DATABASE_DEFAULT AS [Pago_Descripcion],
            [Pago_Aprovacion] COLLATE DATABASE_DEFAULT AS [Pago_Aprovacion],
            [Moneda_Id],
            [Banco_Id],
            [MonederoTarj_Id] COLLATE DATABASE_DEFAULT AS [MonederoTarj_Id],
            [Tarjeta_id],
            [Pago_Total],
            [Pago_Total_Moneda],
            [Pago_Total_Tipo_Cambio],
            [Pago_Suc_Ref],
            [Pago_Caj_Ref],
            [Pago_Fac_Ref],
            [Pago_Cupon_Automatico],
            [Cierre_Id],
            [Pago_Fecha_Vencimiento] COLLATE DATABASE_DEFAULT AS [Pago_Fecha_Vencimiento],
            [Autorizador_Id],
            [Pago_Anulado],
            [Tarjeta_Impuesto]
        FROM {{ item['Servidor_Vinculado'] }}.{{ item['Base_Datos'] }}.dbo.Ticket_Forma_Pago WITH (NOLOCK)
        WHERE
            Emp_Id = {{ item['Empresa_Id_Original'] }}
            AND (Pago_Fecha >= '{{ v_fecha_inicio }}' AND Pago_Fecha < '{{ v_fecha_fin }}')
    {%- endfor -%}
)

SELECT
    *,
    {% if is_incremental() -%}
        GETDATE() AS [Fecha_Carga],
        GETDATE() AS [Fecha_Actualizado]
    {% else -%}
        CASE WHEN GETDATE()-7 < [Pago_Fecha] THEN GETDATE() ELSE [Pago_Fecha] END AS [Fecha_Carga],
        CASE WHEN GETDATE()-7 < [Pago_Fecha] THEN GETDATE() ELSE [Pago_Fecha] END AS [Fecha_Actualizado]
    {% endif %}
FROM datosBase
