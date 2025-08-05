{% set unique_key_list = [
    "Sociedad_Id", "Pedido_Id", "Pedido_Posicion_Id", "Factura_Id"
] %}
{{
    config(
        tags=["automation/periodo_mensual_inicio"],
        materialized="view",
        on_schema_change="append_new_columns",
        post_hook=[
            "{{ dwh_farinter_remove_incremental_temp_table() }}",
        ],
    )
}}

-- Modelo DBT: BIV_SAP_FlujoFacturacionPosicionPedido
-- Potencial reemplazo de transacción ZRAF
SELECT
    FLUJO.Sociedad_Id,
    FLUJO.Centro_Id,
    FLUJO.Almacen_Id,
    FLUJO.Cliente_Id,
    C.Cliente_Nombre,
    FLUJO.Vendedor_Id_Pedido,
    FLUJO.Vendedor_Id_Factura,
    FLUJO.PedidoCliente_Id,
    PEDC.AUART AS ClasePedido_Id,
    FLUJO.Pedido_Id,
    FLUJO.Pedido_Posicion_Id,
    FLUJO.Articulo_Id,
    FLUJO.Lote_Id_Min,
    PEDP.LPRIO AS PrioridadEntrega_Id,
    PEDP.ABGRU AS MotivoRechazo_Id,
    PEDP.VSTEL AS PuestoExpedicion_Id,
    FLUJO.NotaEntrega_Id,
    FLUJO.Cantidad_NotaEntrega,
    FLUJO.SalidaMercancias_Id,
    FLUJO.Cantidad_SalidaMercancias,
    FLUJO.Factura_Id,
    FACTC.KIDNO AS Factura_Serie,
    FACTC.FKART AS ClaseFactura_Id,
    FLUJO.GrupoPrecioCliente_Id,
    FLUJO.Cantidad_Factura,
    FLUJO.Venta_Neta_Factura,
    FLUJO.Venta_Bruta_Factura,
    FLUJO.PrecioUnitario_Factura,
    PEDC.ERNAM AS Pedido_CreadoPor,
    PEDC.VTWEG AS CanalDistribucion_Id_Pedido,
    CANAL.CanalDist_Nombre AS CanalDistribucion_Nombre_Pedido,
    PEDC.VKBUR AS OficinaVentas_Id_Pedido,
    CALPEDIDO.AnioMes_Id AS AnioMesCreado_Id,
    CALPEDIDO.Fecha_Id AS FechaCreado_Pedido,
    FLUJO.HoraCreado_Pedido,
    FLUJO.FechaCreado_NotaEntrega,
    FLUJO.HoraCreado_NotaEntrega,
    FLUJO.FechaCreado_SalidaMercancias,
    FLUJO.HoraCreado_SalidaMercancias,
    CALFACTURA.AnioMes_Id AS AnioMesCreado_Factura,
    CALFACTURA.Fecha_Id AS FechaCreado_Factura,
    FLUJO.HoraCreado_Factura,
    FLUJO.Fecha_SalidaRepartidor,
    FLUJO.Hora_SalidaRepartidor,
    FLUJO.Repartidor_Id,
    FLUJO.Cantidad_Posiciones_Factura,
    FLUJO.Cantidad_SKUS,
    COALESCE(MOTIVO.BEZEI, 'X') AS MotivoRechazo_Nombre,
    CASE WHEN
        COALESCE(Cantidad_Devuelta, 0.0) = FLUJO.Cantidad_Factura
        OR FLUJO.Venta_Neta_Factura BETWEEN COALESCE(Valor_Devuelto_Neto, 0.0) * 0.98 AND COALESCE(Valor_Devuelto_Neto, 0.0) * 1.02
        THEN 'X'
    ELSE FACTC.FKSTO END AS Factura_Anulada,
    COALESCE(REP.NAME_REP, 'X') AS Repartidor_Nombre,
    COALESCE(FLUJO.Cantidad_Devuelta, 0.0) AS Cantidad_Devuelta,
    COALESCE(FLUJO.Valor_Devuelto_Neto, 0.0) AS Valor_Devuelto_Neto,
    COALESCE(FLUJO.Ultima_NotaCredito_Id, 'X') AS Ultima_NotaCredito_Id,
    COALESCE(FLUJO.Cantidad_Factura_Original, 0.0) AS Cantidad_Factura_Original,
    COALESCE(FLUJO.Valor_Neto_Original, 0.0) AS Valor_Neto_Original,
    CASE
        WHEN
            FLUJO.FechaCreado_Pedido <= FLUJO.FechaCreado_NotaEntrega
            AND FLUJO.FechaCreado_NotaEntrega > '1900-01-01'
            THEN
                DATEDIFF(
                    MINUTE,
                    CAST(FLUJO.FechaCreado_Pedido AS DATETIME) + CAST(FLUJO.HoraCreado_Pedido AS DATETIME),
                    CAST(FLUJO.FechaCreado_NotaEntrega AS DATETIME) + CAST(FLUJO.HoraCreado_NotaEntrega AS DATETIME)
                )
    END AS Tiempo_Pedido_NotaEntrega,
    CASE
        WHEN
            FLUJO.FechaCreado_NotaEntrega <= FLUJO.FechaCreado_SalidaMercancias
            AND FLUJO.FechaCreado_SalidaMercancias > '1900-01-01'
            THEN
                DATEDIFF(
                    MINUTE,
                    CAST(FLUJO.FechaCreado_NotaEntrega AS DATETIME) + CAST(FLUJO.HoraCreado_NotaEntrega AS DATETIME),
                    CAST(FLUJO.FechaCreado_SalidaMercancias AS DATETIME) + CAST(FLUJO.HoraCreado_SalidaMercancias AS DATETIME)
                )
    END AS Tiempo_NotaEntrega_SalidaMercancias,
    CASE
        WHEN
            FLUJO.FechaCreado_SalidaMercancias <= FLUJO.FechaCreado_Factura
            AND FLUJO.FechaCreado_Factura > '1900-01-01'
            THEN
                DATEDIFF(
                    MINUTE,
                    CAST(FLUJO.FechaCreado_SalidaMercancias AS DATETIME) + CAST(FLUJO.HoraCreado_SalidaMercancias AS DATETIME),
                    CAST(FLUJO.FechaCreado_Factura AS DATETIME) + CAST(FLUJO.HoraCreado_Factura AS DATETIME)
                )
    END AS Tiempo_SalidaMercancias_Factura,
    CASE
        WHEN
            FLUJO.FechaCreado_Factura <= FLUJO.Fecha_SalidaRepartidor
            AND FLUJO.Fecha_SalidaRepartidor > '1900-01-01'
            THEN
                DATEDIFF(
                    MINUTE,
                    CAST(FLUJO.FechaCreado_Factura AS DATETIME) + CAST(FLUJO.HoraCreado_Factura AS DATETIME),
                    CAST(FLUJO.Fecha_SalidaRepartidor AS DATETIME) + CAST(FLUJO.Hora_SalidaRepartidor AS DATETIME)
                )
    END AS Tiempo_Factura_SalidaRepartidor
FROM (
    SELECT
        FLUJO.Sociedad_Id,
        FLUJO.Centro_Id,
        FLUJO.Almacen_Id,
        FLUJO.Cliente_Id,
        FLUJO.Vendedor_Id_Pedido,
        FLUJO.Vendedor_Id_Factura,
        FLUJO.PedidoCliente_Id,
        FLUJO.Pedido_Id,
        FLUJO.Pedido_Posicion_Id,
        FLUJO.TipoDocumentoPedido_Id,
        FLUJO.Articulo_Id,
        FLUJO.Cantidad_Pedida,
        FLUJO.NotaEntrega_Id,
        FLUJO.Factura_Id,
        FLUJO.AnioCreado_Id,
        FLUJO.AnioMesCreado_Id,
        FLUJO.FechaCreado_Pedido,
        FLUJO.HoraCreado_Pedido,
        FLUJO.AnioMesCreado_Factura,
        FLUJO.FechaCreado_Factura,
        FLUJO.HoraCreado_Factura,
        FLUJO.Fecha_SalidaRepartidor,
        FLUJO.Hora_SalidaRepartidor,
        FLUJO.Repartidor_Id,
        COALESCE(MIN(CASE WHEN FLUJO.Lote_Id = 'X' OR FLUJO.Lote_Id = '' THEN NULL ELSE FLUJO.Lote_Id END), 'X') AS Lote_Id_Min,
        SUM(FLUJO.Cantidad_NotaEntrega) AS Cantidad_NotaEntrega,
        COALESCE(MIN(CASE
            WHEN FLUJO.SalidaMercancias_Id = 'X'
                THEN NULL
            ELSE FLUJO.SalidaMercancias_Id
        END), 'X') AS SalidaMercancias_Id,
        SUM(FLUJO.Cantidad_SalidaMercancias) AS Cantidad_SalidaMercancias,
        MAX(FLUJO.GrupoPrecioCliente_Id) AS GrupoPrecioCliente_Id,
        SUM(FLUJO.Cantidad_Factura) AS Cantidad_Factura,
        SUM(FLUJO.Venta_Neta_Factura) AS Venta_Neta_Factura,
        SUM(FLUJO.Venta_Bruta_Factura) AS Venta_Bruta_Factura,
        AVG(FLUJO.PrecioUnitario_Factura) AS PrecioUnitario_Factura,
        MAX(FLUJO.FechaCreado_NotaEntrega) AS FechaCreado_NotaEntrega,
        MAX(FLUJO.HoraCreado_NotaEntrega) AS HoraCreado_NotaEntrega,
        CAST(COALESCE(MIN(CASE
            WHEN FLUJO.SalidaMercancias_Id = 'X'
                THEN NULL
            ELSE FLUJO.FechaCreado_SalidaMercancias
        END), '1900-01-01') AS DATE) AS FechaCreado_SalidaMercancias,
        CAST(COALESCE(MIN(CASE
            WHEN FLUJO.SalidaMercancias_Id = 'X'
                THEN NULL
            ELSE FLUJO.HoraCreado_SalidaMercancias
        END), '00:00') AS TIME(0)) AS HoraCreado_SalidaMercancias,
        COUNT(FLUJO.Factura_Posicion_Id) AS Cantidad_Posiciones_Factura,
        COUNT(DISTINCT FLUJO.Articulo_Id) AS Cantidad_SKUS,
        SUM(DEVUELTO.Cantidad_Devuelta) AS Cantidad_Devuelta,
        SUM(DEVUELTO.Valor_Devuelto_Neto) AS Valor_Devuelto_Neto,
        MAX(DEVUELTO.Ultima_NotaCredito_Id) AS Ultima_NotaCredito_Id,
        SUM(ORIGINAL.Cantidad_Factura_Original) AS Cantidad_Factura_Original,
        SUM(ORIGINAL.Valor_Neto_Original) AS Valor_Neto_Original
    FROM {{ source('BI_FARINTER', 'BI_SAP_Hecho_FlujoFacturacion') }} AS FLUJO
    LEFT JOIN (
        SELECT
            Factura_Id_Devuelta,
            Factura_Posicion_Id_Devuelta,
            AnioMesCreado_Pedido_Devuelto,
            SUM(Cantidad_Factura) AS Cantidad_Devuelta,
            SUM(Venta_Neta_Factura) AS Valor_Devuelto_Neto,
            MAX(Factura_Id) AS Ultima_NotaCredito_Id
        FROM {{ source('BI_FARINTER', 'BI_SAP_Hecho_FlujoFacturacion') }}
        WHERE TipoDocumentoPedido_Id = 'H' --Devoluciones
        GROUP BY
            Factura_Id_Devuelta,
            Factura_Posicion_Id_Devuelta,
            AnioMesCreado_Pedido_Devuelto
    ) AS DEVUELTO
        ON
            FLUJO.AnioMesCreado_Id = DEVUELTO.AnioMesCreado_Pedido_Devuelto
            AND FLUJO.Factura_Id = DEVUELTO.Factura_Id_Devuelta
            AND FLUJO.Factura_Posicion_Id = DEVUELTO.Factura_Posicion_Id_Devuelta
    LEFT JOIN (
        SELECT
            Factura_Id,
            Factura_Posicion_Id,
            AnioMesCreado_Id,
            Pedido_Id,
            Pedido_Posicion_Id,
            SUM(Cantidad_Factura) AS Cantidad_Factura_Original,
            SUM(Venta_Neta_Factura) AS Valor_Neto_Original
        FROM {{ source('BI_FARINTER', 'BI_SAP_Hecho_FlujoFacturacion') }}
        WHERE TipoDocumentoPedido_Id = 'C' --Pedidos Normales
        GROUP BY
            Factura_Id,
            Factura_Posicion_Id,
            AnioMesCreado_Id,
            Pedido_Id,
            Pedido_Posicion_Id
    ) AS ORIGINAL
        ON
            FLUJO.AnioMesCreado_Pedido_Devuelto = ORIGINAL.AnioMesCreado_Id
            AND FLUJO.Factura_Id_Devuelta = ORIGINAL.Factura_Id
            AND FLUJO.Factura_Posicion_Id_Devuelta = ORIGINAL.Factura_Posicion_Id
            AND FLUJO.Pedido_Id_Devuelto = ORIGINAL.Pedido_Id
            AND FLUJO.Pedido_Posicion_Id_Devuelto = ORIGINAL.Pedido_Posicion_Id
            AND FLUJO.AnioMesCreado_Pedido_Devuelto > 1900
    GROUP BY
        FLUJO.Sociedad_Id,
        FLUJO.PedidoCliente_Id,
        FLUJO.Pedido_Id,
        FLUJO.Pedido_Posicion_Id,
        FLUJO.TipoDocumentoPedido_Id,
        FLUJO.Articulo_Id,
        FLUJO.NotaEntrega_Id,
        FLUJO.Cantidad_Pedida,
        FLUJO.Factura_Id,
        FLUJO.Centro_Id,
        FLUJO.Almacen_Id,
        FLUJO.Cliente_Id,
        FLUJO.Vendedor_Id_Pedido,
        FLUJO.Vendedor_Id_Factura,
        FLUJO.AnioCreado_Id,
        FLUJO.AnioMesCreado_Id,
        FLUJO.FechaCreado_Pedido,
        FLUJO.HoraCreado_Pedido,
        FLUJO.AnioMesCreado_Factura,
        FLUJO.FechaCreado_Factura,
        FLUJO.HoraCreado_Factura,
        FLUJO.Fecha_SalidaRepartidor,
        FLUJO.Hora_SalidaRepartidor,
        FLUJO.Repartidor_Id
) AS FLUJO
INNER JOIN {{ source('BI_FARINTER', 'BI_Dim_Cliente_SAP') }} AS C
    ON FLUJO.Cliente_Id = C.Cliente_Id
LEFT JOIN {{ source('DL_FARINTER', 'DL_SAP_ZFAR_SDT_0001') }} AS REP
    ON FLUJO.Repartidor_Id = REP.COD_REP
INNER JOIN {{ source('DL_FARINTER', 'DL_SAP_VBAK') }} AS PEDC
    ON
        FLUJO.Pedido_Id = PEDC.VBELN
        AND FLUJO.AnioCreado_Id = PEDC.AnioCreado_Id
INNER JOIN {{ source('DL_FARINTER', 'DL_SAP_VBAP') }} AS PEDP
    ON
        FLUJO.Pedido_Id = PEDP.VBELN
        AND FLUJO.Pedido_Posicion_Id = PEDP.POSNR
        AND FLUJO.AnioMesCreado_Id = PEDP.AnioMesCreado_Id
LEFT JOIN {{ source('DL_FARINTER', 'DL_SAP_TVAG') }} AS MOTIVO
    ON PEDP.ABGRU = MOTIVO.ABGRU
LEFT JOIN {{ source('BI_FARINTER', 'BI_SAP_Dim_CanalDist') }} AS CANAL
    ON PEDC.VTWEG = CANAL.CanalDist_Id
LEFT JOIN {{ source('DL_FARINTER', 'DL_SAP_VBRK') }} AS FACTC
    ON
        FLUJO.Factura_Id = FACTC.VBELN
        AND FLUJO.AnioMesCreado_Factura = FACTC.AnioMesCreado_Id
INNER JOIN {{ source('BI_FARINTER', 'BI_Dim_Calendario') }} AS CALPEDIDO
    ON
        FLUJO.AnioMesCreado_Id = CALPEDIDO.AnioMes_Id
        AND FLUJO.FechaCreado_Pedido = CALPEDIDO.Fecha_Id
INNER JOIN {{ source('BI_FARINTER', 'BI_Dim_Calendario') }} AS CALFACTURA
    ON
        FLUJO.AnioMesCreado_Factura = CALFACTURA.AnioMes_Id
        AND FLUJO.FechaCreado_Factura = CALFACTURA.Fecha_Id
/*
Para pruebas
--LEFT JOIN BI_FARINTER.dbo.BI_Dim_Casa_SAP CASA
--	ON CASA.Casa_Id = FLUJO.Casa_Id_Historia
--WHERE FLUJO.AnioMesCreado_Id = 202303
--SELECT * FROM [dbo].[BIV_SAP_FlujoFacturacionPosicionPedido] WHERE AnioMesCreado_Id = 202303
*/
