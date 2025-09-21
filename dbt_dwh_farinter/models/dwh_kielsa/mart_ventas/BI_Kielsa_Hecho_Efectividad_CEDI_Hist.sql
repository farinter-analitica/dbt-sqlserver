{% set unique_key_list = ["Emp_Id","Sucursal_Id","Articulo_Id", "Efectividad_Id", "Fecha_Id",] %}

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
        ]
	) 
}}

{%- if is_incremental() %}
    {% set last_date = run_single_value_query_on_relation_and_return(query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -120, max(Fecha_Actualizado)), 112), '19000101') as fecha_a from  """ ~ this, relation_not_found_value='19000101'|string)|string %}
{%- else %}
	{% set last_date = '19000101' %}
{%- endif %}
--
WITH DespachoD AS (
    SELECT
        DesDet.Emp_Id AS Emp_Id,
        DesDet.Suc_Id AS Sucursal_Id,
        DesDet.Articulo_Id AS Articulo_Id,
        MAX(DEnc.Desp_Usuario_Aplica) AS Usuario_Id,
        MAX(DesDet.CEDI_Id) AS CEDI_Id,
        DesDet.Pedido_Id AS Pedido_Id,
        MAX(DesDet.SubPedido_Id) AS SubPedido_Id,
        CONVERT(date, DEnc.Desp_Fecha_Aplicado) AS Fecha_Id,
        SUM(Pedido.Detalle_Sugerido) AS Sugerido,
        SUM(DesDet.Detalle_Cantidad_Pedida) AS Pedida,
        SUM(DesDet.Detalle_Cantidad_Auditada) AS Despachada,
        MAX(DesDet.Detalle_Costo_Bruto_Desp) AS Costo_Bruto,
        MAX(DesDet.Detalle_Descuento_Desp) AS Descuento
    FROM DL_FARINTER.dbo.DL_Kielsa_Inv_Despacho_Detalle DesDet --{{ source('DL_FARINTER', 'DL_Kielsa_Inv_Despacho_Detalle') }}
    INNER JOIN DL_FARINTER.dbo.DL_Kielsa_Detalle_Pedido Pedido --{{ source('DL_FARINTER', 'DL_Kielsa_Detalle_Pedido') }}
        ON
            DesDet.Emp_Id = Pedido.Emp_Id AND DesDet.Suc_Id = Pedido.Suc_Id AND DesDet.Pedido_Id = Pedido.Pedido_Id
            AND DesDet.Articulo_Id = Pedido.Articulo_Id
    INNER JOIN DL_FARINTER.dbo.DL_Kielsa_Inv_Despacho_Encabezado DEnc  --{{ source('DL_FARINTER', 'DL_Kielsa_Inv_Despacho_Encabezado') }}
        ON
            DesDet.Emp_Id = DEnc.Emp_Id AND DesDet.CEDI_Id = DEnc.CEDI_Id AND DesDet.Suc_Id = DEnc.Suc_Id
            AND DesDet.Pedido_Id = DEnc.Pedido_Id AND DesDet.SubPedido_Id = DEnc.SubPedido_Id
    WHERE
        DEnc.Desp_Estado IN ('AP', 'RU', 'RE', 'EN')
        {% if is_incremental() -%} 
            AND DEnc.Desp_Fecha_Aplicado > '{{ last_date }}'
        {% else -%}
                   AND DEnc.Desp_Fecha_Aplicado >= '{{ (modules.datetime.datetime.now() - modules.datetime.timedelta(days=360*3)).replace(day=1, month=1, hour=0, minute=0, second=0).strftime('%Y%m%d') }}'
                   {% endif -%}
    GROUP BY
        DesDet.Emp_Id, DesDet.Articulo_Id, DesDet.Suc_Id, DEnc.Desp_Fecha_Aplicado,
        DesDet.Pedido_Id
),

DespachoE AS (
    SELECT
        DesDet.Emp_Id AS Emp_Id,
        DesDet.Suc_Id AS Sucursal_Id,
        DesDet.Articulo_Id AS Articulo_Id,
        MAX(DEnc.Desp_Usuario_Anula) AS Usuario_Id,
        MAX(DesDet.CEDI_Id) AS CEDI_Id,
        DesDet.Pedido_Id AS Pedido_Id,
        MAX(DesDet.SubPedido_Id) AS SubPedido_Id,
        CONVERT(date, DEnc.Desp_Fecha_Anulado) AS Fecha_Id,
        SUM(Pedido.Detalle_Sugerido) AS Sugerido,
        SUM(DesDet.Detalle_Cantidad_Pedida) AS Pedida,
        SUM(DesDet.Detalle_Cantidad_Auditada) AS Despachada,
        MAX(DesDet.Detalle_Costo_Bruto_Desp) AS Costo_Bruto,
        MAX(DesDet.Detalle_Descuento_Desp) AS Descuento
    FROM DL_FARINTER.dbo.DL_Kielsa_Inv_Despacho_Detalle DesDet --{{ source('DL_FARINTER', 'DL_Kielsa_Inv_Despacho_Detalle') }}
    INNER JOIN DL_FARINTER.dbo.DL_Kielsa_Detalle_Pedido Pedido  --{{ source('DL_FARINTER', 'DL_Kielsa_Detalle_Pedido') }}
        ON
            DesDet.Emp_Id = Pedido.Emp_Id AND DesDet.Suc_Id = Pedido.Suc_Id AND DesDet.Pedido_Id = Pedido.Pedido_Id
            AND DesDet.Articulo_Id = Pedido.Articulo_Id
    INNER JOIN DL_FARINTER.dbo.DL_Kielsa_Inv_Despacho_Encabezado DEnc --{{ source('DL_FARINTER', 'DL_Kielsa_Inv_Despacho_Encabezado') }}
        ON
            DesDet.Emp_Id = DEnc.Emp_Id AND DesDet.CEDI_Id = DEnc.CEDI_Id AND DesDet.Suc_Id = DEnc.Suc_Id
            AND DesDet.Pedido_Id = DEnc.Pedido_Id AND DesDet.SubPedido_Id = DEnc.SubPedido_Id
    WHERE
        DEnc.Desp_Estado = 'NO'
        {% if is_incremental() -%} 
            AND DEnc.Desp_Fecha_Aplicado > '{{ last_date }}'
        {% else -%}
                   AND DEnc.Desp_Fecha_Aplicado >= '{{ (modules.datetime.datetime.now() - modules.datetime.timedelta(days=360*3)).replace(day=1, month=1, hour=0, minute=0, second=0).strftime('%Y%m%d') }}'
                   {% endif -%}
    GROUP BY
        DesDet.Emp_Id, DesDet.Suc_Id, DesDet.Articulo_Id, DEnc.Desp_Fecha_Anulado,
        DesDet.Pedido_Id

),

Despachos AS (
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
    SELECT
        Despachos.Emp_Id AS Emp_Id,
        Despachos.Sucursal_Id AS Sucursal_Id,
        Despachos.Articulo_Id AS Articulo_Id,
        MAX(Despachos.Usuario_Id) AS Usuario_Id,
        MAX(Despachos.CEDI_Id) AS CEDI_Id,
        Despachos.Pedido_Id AS Pedido_Id,
        MAX(Despachos.SubPedido_Id) AS SubPedido_Id,
        Despachos.Fecha_Id AS Fecha_Id,
        MAX(Despachos.Sugerido) AS Sugerido,
        MAX(Despachos.Pedida) AS Pedida,
        MAX(Despachos.Despachada) AS Despachada,
        MAX(Despachos.Costo_Bruto) AS Costo_Bruto,
        MAX(Despachos.Descuento) AS Descuento
    FROM Despachos

    GROUP BY
        Despachos.Emp_Id, Despachos.Sucursal_Id, Despachos.Articulo_Id,
        Despachos.Pedido_Id, Despachos.Fecha_Id
),

Alerta AS (

    SELECT
        Alerta.Emp_Id,
        Alerta.Articulo_Id,
        CASE
            WHEN Emp_Id = 3 AND MAX(Alerta.Alerta_Id) BETWEEN 3 AND 6 THEN MAX(Alerta.Alerta_Id)
            WHEN Emp_Id = 2 AND MAX(Alerta.Alerta_Id) BETWEEN 2 AND 4 THEN MAX(Alerta.Alerta_Id)
            WHEN Emp_Id = 1 AND MAX(Alerta.Alerta_Id) BETWEEN 1 AND 3 THEN MAX(Alerta.Alerta_Id)
            WHEN Emp_Id = 5 AND MAX(Alerta.Alerta_Id) BETWEEN 27 AND 35 THEN (35 - MAX(Alerta.Alerta_Id))
            ELSE 0
        END AS PV_Alerta_Id
    FROM DL_FARINTER.dbo.DL_Kielsa_Articulo_Alerta Alerta --{{ source('DL_FARINTER', 'DL_Kielsa_Articulo_Alerta') }}
    WHERE
        (Emp_Id = 3 AND Alerta.Alerta_Id BETWEEN 3 AND 6)
        OR (Emp_Id = 2 AND Alerta.Alerta_Id BETWEEN 2 AND 4)
        OR (Emp_Id = 1 AND Alerta.Alerta_Id BETWEEN 1 AND 3)
        OR (Emp_Id = 5 AND Alerta.Alerta_Id BETWEEN 27 AND 35)
    GROUP BY Emp_Id, Alerta.Articulo_Id
),

Efectividad AS (
    SELECT
        Despacho.Emp_Id AS Emp_Id,
        MAX(Despacho.Sucursal_Id) AS Sucursal_Id,
        MAX(Suc.Zona_Id) AS Zona_Id,
        MAX(Suc.Departamento_Id) AS Departamento_Id,
        MAX(Suc.Municipio_Id) AS Municipio_Id,
        MAX(Suc.Ciudad_Id) AS Ciudad_Id,
        MAX(Suc.TipoSucursal_Id) AS TipoSucursal_Id,
        MAX(ISNULL(ArtInf.Articulo_Id, Art.Articulo_Id)) AS Articulo_Id,
        MAX(ISNULL(Art.Articulo_Codigo_Padre, Despacho.Articulo_Id)) AS ArticuloPadre_Id,
        MAX(ArtInf.Casa_Id) AS Casa_Id,
        MAX(ArtInf.Marca_Id) AS Marca_Id,
        MAX(ArtInf.Categoria_Id) AS CategoriaArt_Id,
        MAX(ArtInf.Depto_Id) AS DeptoArt_Id,
        MAX(ArtInf.SubCategoria_Id) AS SubCategoria1Art_Id,
        MAX(ArtInf.SubCategoria2_Id) AS SubCategoria2Art_Id,
        MAX(ArtInf.SubCategoria3_Id) AS SubCategoria3Art_Id,
        MAX(ArtInf.SubCategoria4_Id) AS SubCategoria4Art_Id,
        MAX(ISNULL(Prov.Proveedor_Id, 0)) AS Proveedor_Id,
        MAX(ISNULL(ArtxMec.MecanicaCanje_Id, 'x')) AS Mecanica_Id,
        MAX(ISNULL(Alerta.PV_Alerta_Id, 0)) AS Cuadro_Id,
        MAX(CASE Art.ABC_Cadena
            WHEN 'A' THEN 1
            WHEN 'B' THEN 2
            WHEN 'C' THEN 3
            WHEN 'D' THEN 4
            WHEN 'E' THEN 5
            ELSE 0
        END) AS ABCCadena_Id,
        0 AS AlertaInv_Id,
        0 AS DiasInv_Id,
        MAX(CASE WHEN Despacho.Usuario_Id = 0 THEN 'x' ELSE Despacho.usuario_Id END) AS Usuario_Id,
        MAX(
            CONVERT(nvarchar, Despacho.CEDI_Id) + '-' + CONVERT(nvarchar, Despacho.Pedido_Id)
            + '-' + CONVERT(nvarchar, Despacho.SubPedido_Id)
        ) AS Efectividad_Id,
        CASE
            WHEN
                SUM(Despacho.Despachada) > 0
                AND Despacho.Fecha_Id BETWEEN (GETDATE() - 1) AND GETDATE()
                THEN 1
            ELSE 0
        END AS EstadoDespacho_Id,
        Despacho.Fecha_Id AS Fecha_Id,
        YEAR(Despacho.Fecha_Id) AS Anio_Id,
        DATEPART(QUARTER, Despacho.Fecha_Id) AS Trimestre_Id,
        MONTH(Despacho.Fecha_Id) AS Mes_Id,
        DATEPART(WEEKDAY, DATEADD(DAY, 1, Despacho.Fecha_Id)) AS Dias_Id,
        DAY(Despacho.Fecha_Id) AS Dia_Id,--/isnull(D.Articulo_Factor_Unitario,1)
        SUM(CASE WHEN Despacho.Sugerido > 0 THEN Despacho.Sugerido ELSE Despacho.Pedida END) AS Efectividad_Sugerido,
        SUM(Despacho.Pedida) AS Efectividad_Pedido,
        SUM(Despacho.Despachada) AS Efectividad_Despacho,
        MAX(CASE WHEN Art.Indicador_PadreHijo = 'P' THEN ISNULL(Art.Factor_Denominador, 1) ELSE 1 END) AS Efectividad_FactorUnitario,
        MAX(Despacho.Costo_Bruto) AS Efectividad_Costo_Bruto,
        MAX(Despacho.Descuento) AS Efectividad_Descuento,
        MAX(Suc.EmpSuc_Id) AS EmpSuc_Id,
        MAX(Art.EmpArt_Id) AS EmpArt_Id
    FROM Despacho Despacho
    INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Sucursal Suc -- {{ ref ('BI_Kielsa_Dim_Sucursal') }}
        ON Despacho.Emp_Id = Suc.Emp_Id AND Despacho.Sucursal_Id = Suc.Sucursal_Id
    INNER JOIN DL_FARINTER.dbo.DL_Kielsa_Articulo_Info ArtInf --{{ source('DL_FARINTER', 'DL_Kielsa_Articulo_Info') }}
        ON Despacho.Emp_Id = ArtInf.Emp_Id AND Despacho.Articulo_Id = ArtInf.Articulo_Id
    LEFT JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Articulo Art -- {{ ref ('BI_Kielsa_Dim_Articulo') }}
        ON Despacho.Emp_Id = Art.Emp_Id AND Despacho.Articulo_Id = Art.Articulo_Id
    LEFT OUTER JOIN Alerta Alerta
        ON Despacho.Emp_Id = Alerta.Emp_Id AND Despacho.Articulo_Id = Alerta.Articulo_Id
    LEFT OUTER JOIN AN_FARINTER.[dbo].[AN_Cal_CasaXProveedor_Kielsa] Prov --{{ source('AN_FARINTER', 'AN_Cal_CasaXProveedor_Kielsa') }}
        ON Despacho.Emp_Id = Prov.Pais_Id AND Art.Casa_Id = Prov.Casa_Id AND Despacho.Fecha_Id BETWEEN Prov.Inicio AND Prov.Final
    LEFT OUTER JOIN DL_FARINTER.[dbo].[DL_TC_ArticuloXMecanica_Kielsa] ArtxMec --{{ ref( 'DL_TC_ArticuloXMecanica_Kielsa') }}
        ON
            Despacho.Emp_Id = ArtxMec.Emp_Id AND Despacho.Articulo_Id = ArtxMec.Articulo_Id
            AND Despacho.Fecha_Id BETWEEN Prov.Inicio AND Prov.Final

    GROUP BY
        Despacho.Fecha_Id,
        Despacho.Emp_Id,
        Art.Articulo_Id,
        Suc.Sucursal_Id

)

SELECT
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
    CAST(Efectividad.Efectividad_Pedido AS int) AS Efectividad_Pedido,
    CAST(Efectividad.Efectividad_Despacho AS int) AS Efectividad_Despacho,
    Efectividad.Efectividad_FactorUnitario,
    Efectividad.Efectividad_Costo_Bruto,
    Efectividad.Efectividad_Descuento,
    Efectividad.EmpSuc_Id,
    Efectividad.EmpArt_Id,
    GETDATE() AS Fecha_Actualizado

--INTO #PruebaEfectividad
FROM Efectividad

--SELECT * from #PruebaEfectividad
