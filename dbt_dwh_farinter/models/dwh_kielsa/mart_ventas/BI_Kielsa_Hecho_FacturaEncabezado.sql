{%- set nombre_esquema_particion = "ps_" + this.identifier + "_fecha" -%}
{%- set on_clause = nombre_esquema_particion ~ "([Factura_Fecha])" -%}
{%- set unique_key_list = ["Factura_Id","Suc_Id","Emp_Id","TipoDoc_Id","Caja_Id","Factura_Fecha"] -%}

{{ 
    config(
		as_columnstore=false,
		tags=["periodo/diario","automation/periodo_por_hora"],
		materialized="incremental",
        incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="append_new_columns",
		on_clause_filegroup = on_clause,
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		pre_hook=["
		{%- if is_incremental() -%}
			--Incremental
		{%- else -%}
			EXEC ADM_FARINTER.dbo.pa_inicializar_particiones
				@p_base_datos = '{{ this.database }}',
				@p_nombre_esquema_particion = '{{ nombre_esquema_particion }}',
				@p_nombre_funcion_particion = 'pf_{{ this.identifier }}_fecha',
				@p_periodo_tipo = 'Anual', --'Anual' o 'Mensual'
				@p_tipo_datos = 'Fecha', --'Fecha' o 'AnioMes'
				@p_fecha_base = '2018-01-01'
		{%- endif -%}"
		],
		post_hook=[
		"{{ dwh_farinter_remove_incremental_temp_table() }}",
		"{{ dwh_farinter_create_clustered_columnstore_index(is_incremental=is_incremental(),
			if_another_exists_drop_it=true) }}",
		"{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", 
			create_clustered=false, 
			is_incremental=is_incremental(), 
			if_another_exists_drop_it=true) }}",
        "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Fecha_Actualizado'], included_columns=['Factura_Fecha']) }}",
        "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Factura_Fecha']) }}",
		"EXEC ADM_FARINTER.dbo.pa_comprimir_indices_particiones_anteriores 
			@p_base_datos = '{{this.database}}',
		 	@p_esquema_tabla = '{{this.schema}}', 
			@p_nombre_tabla = '{{this.identifier}}', 
			@p_tipo_datos = 'Fecha';"		
			]
		
) }}

{% if is_incremental() %}
	{% set last_date = run_single_value_query_on_relation_and_return(
		query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -1, max(Fecha_Actualizado)), 112), '19000101')  from  """ ~ this, 
		relation_not_found_value='19000101'|string)|string %}
{% else %}
    {% set last_date = '19000101' %}
{% endif %}

WITH Facturas AS (
    SELECT --TOP 1000 --noqa: ST06
        ISNULL(CAST(FE.[Factura_Fecha] AS DATE), '19000101') AS Factura_Fecha,
        ISNULL(CAST(FE.[Emp_Id] AS INT), 0) AS [Emp_Id],
        ISNULL(FE.[Suc_Id], 0) AS [Suc_Id], --, FE.[Suc_Id]
        ISNULL(FE.[Bodega_Id], 0) AS [Bodega_Id], --, FE.[Bodega_Id]
        ISNULL(FE.[Caja_Id], 0) AS [Caja_Id], --, FE.[Caja_Id]
        ISNULL(FE.[TipoDoc_Id], 0) AS [TipoDoc_Id], --, FE.[TipoDoc_Id]
        ISNULL(FE.[Factura_Id], 0) AS [Factura_Id], --, FE.[Factura_Id]
        ISNULL(FE.[SubDoc_Id], 0) AS [SubDoc_Id], --, FE.[SubDoc_Id]
        FE.[Consecutivo] AS Consecutivo_Factura,
        CAST(FE.[Factura_Fecha] AS DATETIME) AS Factura_FechaHora,
        CAST(FE.[Factura_Fecha] AS TIME) AS Factura_Hora,
        DATEPART(HOUR, FE.[Factura_Fecha]) AS Hora_Id,
        FE.[MonederoTarj_Id],
        FE.[MonederoTarj_Id_Limpio] AS [Monedero_Id],
        FE.[Cliente_Id],
        FE.[Usuario_Id],
        FE.[Vendedor_Id],
        FE.[Preventa_Id],
        FE.[PreFactura_id],
        FE.Factura_Numero_CAI AS Factura_Clave_Tributaria,
        FE.[Factura_Estado],
        FE.[Factura_Origen],
        CASE
            WHEN FEXP.Factura_Id IS NOT NULL
                THEN 5 --eCommerce
            ELSE CASE FE.Factura_Origen
                WHEN 'FA'
                    THEN 1	--Venta de sucursal
                WHEN 'EX'
                    THEN 2	--Domicilio
                WHEN 'PU'
                    THEN 3	--Recoger en sucursal
                WHEN 'PR'
                    THEN 4	--Preventa
                ELSE 0
            END --Origen desconocido
        END
            AS CanalVenta_Id,
        FE.[AnioMes_Id],
        ISNULL(FE.Factura_Costo, 0) AS Valor_Costo,
        ISNULL(FE.Factura_Costo_Bonificacion, 0) AS Valor_Costo_Bonificacion,
        ISNULL(FE.Factura_Subtotal, 0) AS Valor_Subtotal,
        ISNULL(FE.Factura_Descuento, 0) AS Valor_Descuento,
        ISNULL(FE.Factura_Impuesto, 0) AS Valor_Impuesto,
        ISNULL(FE.Factura_Total, 0) AS Valor_Total,
        ISNULL(FE.Factura_Articulos, 0) AS Cantidad_Articulos,
        ISNULL(FE.Factura_AcumMonedero, 0) AS Valor_Acum_Monedero,
        SUC.HashStr_SucEmp,
        DOC.HashStr_SubDDocEmp,
        CLI.HashStr_CliEmp,
        MON.HashStr_MonEmp,
        MON.EmpMon_Id,
        EMPL.HashStr_EmplEmp,
        BOD.HashStr_SucBodEmp,
        ISNULL(SAM.Tipo_Id, 0) AS Same_Id
        {% if is_incremental() and last_date != '19000101' -%} 
            , ISNULL(GETDATE(), '19000101') AS Fecha_Actualizado
        {% else -%}
            , CASE WHEN FE.[Fecha_Actualizado] > GETDATE() - 7 THEN GETDATE() ELSE ISNULL(FE.[Fecha_Actualizado], '19000101') END AS Fecha_Actualizado
        {% endif %}
    FROM {{ source('DL_FARINTER', 'DL_Kielsa_FacturaEncabezado') }} AS FE
    INNER JOIN {{ source('BI_FARINTER', 'BI_Dim_Calendario') }} AS CAL
        ON CAL.Fecha_Id = CAST(FE.Factura_Fecha AS DATE) AND FE.AnioMes_Id = CAL.AnioMes_Id
    LEFT JOIN {{ ref ('BI_Kielsa_Dim_Monedero') }} AS MON
        ON
            FE.MonederoTarj_Id_Limpio = MON.Monedero_Id
            AND FE.Emp_Id = MON.Emp_Id
    LEFT JOIN {{ source ('BI_FARINTER', 'BI_Kielsa_Dim_Empresa') }} AS E
        ON FE.Emp_Id = E.Empresa_Id
    LEFT JOIN {{ ref ('BI_Kielsa_Dim_Sucursal') }} AS SUC
        ON
            FE.Suc_Id = SUC.Sucursal_Id
            AND FE.Emp_Id = SUC.Emp_Id
    LEFT JOIN {{ source ('BI_FARINTER', 'BI_Hecho_SameSucursales_Kielsa') }} AS SAM
        ON
            FE.Suc_Id = SAM.Sucursal_Id_Solo
            AND FE.Emp_Id = SAM.Pais_Id
            AND CAL.Anio_Calendario = SAM.Anio_Id
            AND CAL.Mes_Calendario = SAM.Mes_Id
    LEFT JOIN {{ ref ('BI_Kielsa_Dim_TipoDocumentoSub') }} AS DOC
        ON
            FE.TipoDoc_Id = DOC.Documento_Id
            AND FE.SubDoc_Id = DOC.SubDocumento_Id
            AND FE.Emp_Id = DOC.Emp_Id
    LEFT JOIN {{ ref ('BI_Kielsa_Dim_Cliente') }} AS CLI
        ON
            FE.Cliente_Id = CLI.Cliente_Id
            AND FE.Emp_Id = CLI.Emp_Id
    LEFT JOIN {{ ref ('BI_Kielsa_Dim_Empleado') }} AS EMPL
        ON
            FE.Vendedor_Id = EMPL.Empleado_Id
            AND FE.Emp_Id = EMPL.Emp_Id
    LEFT JOIN {{ ref ('BI_Kielsa_Dim_Bodega') }} AS BOD
        ON
            FE.Bodega_Id = BOD.Bodega_Id
            AND FE.Suc_Id = BOD.Sucursal_Id
            AND FE.Emp_Id = BOD.Emp_Id
    LEFT JOIN (
        SELECT DISTINCT
            A.Emp_Id,
            A.TipoDoc_Id,
            A.Suc_Id,
            A.Caja_Id,
            A.Factura_Id
        FROM {{ ref('DL_Kielsa_Exp_Factura_Express') }} AS A
        WHERE
            A.Orden_Usuario_Registro = 'WEB'
            AND A.Estatus_Id = 'T'
    ) AS FEXP
        ON
            FE.Emp_Id = FEXP.Emp_Id
            AND FE.TipoDoc_Id = FEXP.TipoDoc_Id
            AND FE.Suc_Id = FEXP.Suc_Id
            AND FE.Caja_Id = FEXP.Caja_Id
            AND FE.Factura_Id = FEXP.Factura_Id
    {% if is_incremental() and last_date != '19000101' -%} 
        WHERE FE.Fecha_Actualizado >= '{{ last_date }}' AND FE.Factura_Fecha >= DATEADD(MONTH, -1, GETDATE())
    {% else -%}
        WHERE FE.Factura_Fecha >= DATEADD(YEAR, -3, GETDATE()) AND FE.AnioMes_Id >= YEAR(DATEADD(YEAR, -3, GETDATE())) * 100 + 1
    {% endif %}
)
{% if not is_incremental() and (modules.datetime.datetime.now() - modules.datetime.timedelta(days=5*365)) < modules.datetime.datetime(2024, 10, 1) %}
--Carga de historico sistema anterior arboleda
    , FacturasArboleda AS (
        SELECT --TOP 1000 --noqa: ST06
            ISNULL(CAST(FE.[Factura_Fecha] AS DATE), '19000101') AS Factura_Fecha,
            ISNULL(CAST(444 AS INT), 0) AS [Emp_Id],
            ISNULL(FE.[Suc_Id], 0) AS [Suc_Id], --, FE.[Suc_Id]
            ISNULL(FE.[Bodega_Id], 0) AS [Bodega_Id], --, FE.[Bodega_Id]
            ISNULL(FE.[Caja_Id], 0) AS [Caja_Id], --, FE.[Caja_Id]
            ISNULL(FE.[TipoDoc_Id], 0) AS [TipoDoc_Id], --, FE.[TipoDoc_Id]
            ISNULL(FE.[Factura_Id], 0) AS [Factura_Id], --, FE.[Factura_Id]
            ISNULL(FE.[SubDoc_Id], 0) AS [SubDoc_Id], --, FE.[SubDoc_Id]
            FE.[Consecutivo] AS Consecutivo_Factura,
            CAST(FE.[Factura_Fecha] AS DATETIME) AS Factura_FechaHora,
            CAST(FE.[Factura_Fecha] AS TIME) AS Factura_Hora,
            DATEPART(HOUR, FE.[Factura_Fecha]) AS Hora_Id,
            FE.[MonederoTarj_Id],
            FE.[MonederoTarj_Id_Limpio] AS [Monedero_Id],
            FE.[Cliente_Id],
            FE.[Usuario_Id],
            FE.[Vendedor_Id],
            FE.[Preventa_Id],
            FE.[PreFactura_id],
            FE.Factura_Numero_CAI AS Factura_Clave_Tributaria,
            FE.[Factura_Estado],
            FE.[Factura_Origen],
            CASE
                WHEN FEXP.Factura_Id IS NOT NULL
                    THEN 5 --eCommerce
                ELSE CASE FE.Factura_Origen
                    WHEN 'FA'
                        THEN 1	--Venta de sucursal
                    WHEN 'EX'
                        THEN 2	--Domicilio
                    WHEN 'PU'
                        THEN 3	--Recoger en sucursal
                    WHEN 'PR'
                        THEN 4	--Preventa
                    ELSE 0
                END --Origen desconocido
            END
                AS CanalVenta_Id,
            FE.[AnioMes_Id],
            ISNULL(FE.Factura_Costo, 0) AS Valor_Costo,
            ISNULL(FE.Factura_Costo_Bonificacion, 0) AS Valor_Costo_Bonificacion,
            ISNULL(FE.Factura_Subtotal, 0) AS Valor_Subtotal,
            ISNULL(FE.Factura_Descuento, 0) AS Valor_Descuento,
            ISNULL(FE.Factura_Impuesto, 0) AS Valor_Impuesto,
            ISNULL(FE.Factura_Total, 0) AS Valor_Total,
            ISNULL(FE.Factura_Articulos, 0) AS Cantidad_Articulos,
            ISNULL(FE.Factura_AcumMonedero, 0) AS Valor_Acum_Monedero,
            SUC.HashStr_SucEmp,
            DOC.HashStr_SubDDocEmp,
            CLI.HashStr_CliEmp,
            MON.HashStr_MonEmp,
            MON.EmpMon_Id,
            EMPL.HashStr_EmplEmp,
            BOD.HashStr_SucBodEmp,
            ISNULL(SAM.Tipo_Id, 0) AS Same_Id,
            ISNULL((FE.[Factura_Fecha]), '19000101') AS Fecha_Actualizado
        FROM DL_FARINTER.[dbo].[DL_Kielsa_FacturaEncabezado_ArboledaOld] AS FE -- {{ source('DL_FARINTER', 'DL_Kielsa_FacturaEncabezado_ArboledaOld') }} FE
        INNER JOIN {{ source('BI_FARINTER', 'BI_Dim_Calendario') }} AS CAL
            ON CAL.Fecha_Id = CAST(FE.Factura_Fecha AS DATE) AND FE.AnioMes_Id = CAL.AnioMes_Id
        LEFT JOIN {{ ref ('BI_Kielsa_Dim_Monedero') }} AS MON
            ON
                FE.MonederoTarj_Id_Limpio = MON.Monedero_Id
                AND FE.Emp_Id = MON.Emp_Id
        LEFT JOIN {{ source ('BI_FARINTER', 'BI_Kielsa_Dim_Empresa') }} AS E
            ON FE.Emp_Id = E.Empresa_Id
        LEFT JOIN {{ ref ('BI_Kielsa_Dim_Sucursal') }} AS SUC
            ON
                FE.Suc_Id = SUC.Sucursal_Id
                AND FE.Emp_Id = SUC.Emp_Id
        LEFT JOIN {{ source ('BI_FARINTER', 'BI_Hecho_SameSucursales_Kielsa') }} AS SAM
            ON
                FE.Suc_Id = SAM.Sucursal_Id_Solo
                AND FE.Emp_Id = SAM.Pais_Id
                AND CAL.Anio_Calendario = SAM.Anio_Id
                AND CAL.Mes_Calendario = SAM.Mes_Id
        LEFT JOIN {{ ref ('BI_Kielsa_Dim_TipoDocumentoSub') }} AS DOC
            ON
                FE.TipoDoc_Id = DOC.Documento_Id
                AND FE.SubDoc_Id = DOC.SubDocumento_Id
                AND FE.Emp_Id = DOC.Emp_Id
        LEFT JOIN {{ ref ('BI_Kielsa_Dim_Cliente') }} AS CLI
            ON
                FE.Cliente_Id = CLI.Cliente_Id
                AND FE.Emp_Id = CLI.Emp_Id
        LEFT JOIN {{ ref ('BI_Kielsa_Dim_Empleado') }} AS EMPL
            ON
                FE.Vendedor_Id = EMPL.Empleado_Id
                AND FE.Emp_Id = EMPL.Emp_Id
        LEFT JOIN {{ ref ('BI_Kielsa_Dim_Bodega') }} AS BOD
            ON
                FE.Bodega_Id = BOD.Bodega_Id
                AND FE.Suc_Id = BOD.Sucursal_Id
                AND FE.Emp_Id = BOD.Emp_Id
        LEFT JOIN (
            SELECT DISTINCT
                A.Emp_Id,
                A.TipoDoc_Id,
                A.Suc_Id,
                A.Caja_Id,
                A.Factura_Id
            FROM {{ ref('DL_Kielsa_Exp_Factura_Express') }} AS A
            WHERE
                A.Orden_Usuario_Registro = 'WEB'
                AND A.Estatus_Id = 'T'
        ) AS FEXP
            ON
                FE.Emp_Id = FEXP.Emp_Id
                AND FE.TipoDoc_Id = FEXP.TipoDoc_Id
                AND FE.Suc_Id = FEXP.Suc_Id
                AND FE.Caja_Id = FEXP.Caja_Id
                AND FE.Factura_Id = FEXP.Factura_Id
        WHERE FE.Factura_Fecha >= DATEADD(YEAR, -3, GETDATE()) AND FE.AnioMes_Id >= YEAR(DATEADD(YEAR, -3, GETDATE())) * 100 + 1
    )

{% endif %}
SELECT
    *, --noqa: ST06
    {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Suc_Id', 'TipoDoc_Id', 'Caja_Id', 'Factura_Id'], input_length=49, table_alias='') }} AS [EmpSucDocCajFac_Id]
FROM Facturas
{% if not is_incremental() and (modules.datetime.datetime.now() - modules.datetime.timedelta(days=5*365)) < modules.datetime.datetime(2024, 10, 1) %}
    UNION ALL
    SELECT
        *, --noqa: ST06
        {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Suc_Id', 'TipoDoc_Id', 'Caja_Id', 'Factura_Id'], input_length=49, table_alias='') }} AS [EmpSucDocCajFac_Id]
    FROM FacturasArboleda
{% endif %}
