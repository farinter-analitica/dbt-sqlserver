{% set unique_key_list = ["Pais_Id", "Cliente_Id"] %}

{{- 
	config(
		group="kielsa_analitica_atributos",
		as_columnstore=true,
		materialized="table",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga", "Fecha_Actualizado"],
        tags=["automation/periodo_diario", "periodo_unico/si", "automation_only"],
		post_hook=[
			"{{ dwh_farinter_remove_incremental_temp_table() }}",
			"{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
			"{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Fecha_Actualizado']) }}"
		],
        meta={"owners": ["edwin.martinez@farinter.com"]}
	)
-}}
--dbt
WITH agr_monedero AS (
    SELECT
        M.Monedero_Id,
        M.Emp_Id,
        MAGR.Fecha_Primer_Factura,
        MAGR.Fecha_Ultima_Factura,
        MAGR.Cantidad_Facturas,
        MAGR.Cantidad_Dias,
        M.Celular,
        M.Telefono,
        M.Tipo_Plan,
        M.Edad,
        M.Correo,
        M.Genero,
        M.RangoEdad,
        M.Saldo_Puntos,
        M.Ingreso,
        M.Monedero_Nombre,
        MAGR.Fecha_Actualizado
    FROM {{ ref('BI_Kielsa_Agr_Monedero') }} AS MAGR
    INNER JOIN {{ ref('BI_Kielsa_Dim_Monedero') }} AS M
        ON
            MAGR.Monedero_Id = M.Monedero_Id
            AND MAGR.Emp_Id = M.Emp_Id
    WHERE M.Emp_Id = 1
),

base AS (
    /* -- Base Aggregation from Sales History and Monedero Dimension  -- */
    SELECT
        B.Emp_Id AS Pais_Id,
        B.Monedero_Id AS Cliente_Id,
        B.Monedero_Nombre AS Nombre,
        B.Celular,
        B.Telefono,
        B.Tipo_Plan,
        B.Edad,
        B.Correo,
        B.Genero,
        B.RangoEdad,
        B.Saldo_Puntos,
        B.Ingreso,
        B.Fecha_Primer_Factura AS Primer_Compra,
        B.Fecha_Ultima_Factura AS Ultima_Compra,
        B.Cantidad_Facturas AS Trx_Total,
        B.Cantidad_Dias AS Dias_Total,
        ISNULL(A.Ticket_Promedio, 0) AS Ticket_Promedio,
        ISNULL(A.Recencia, 9999) AS Recencia,
        ISNULL(A.Marcas_Propias, 0) AS Marcas_Propias,
        ISNULL(A.Margen_Promedio, 0) AS Margen_Promedio,
        ISNULL(A.Fecha_Actualizado, B.Fecha_Actualizado) AS Fecha_Actualizado
    FROM agr_monedero AS B
    LEFT JOIN
        (
            SELECT
                A.Pais_Id,
                A.Monedero_Id,
                SUM(A.Venta_Neta) / (1.0 * COUNT(DISTINCT A.Factura_Id)) AS Ticket_Promedio,
                DATEDIFF(DAY, MAX(A.Fecha_Id), GETDATE()) AS Recencia,
                SUM(CASE WHEN A.DeptoArt_Id IN ('1-4', '1-5') THEN A.Venta_Neta ELSE 0 END) / SUM(A.Venta_Neta) AS Marcas_Propias,
                1.0 * SUM(A.Utilidad) / SUM(A.Venta_Neta) AS Margen_Promedio,
                MAX(A.Fecha_Id) AS Fecha_Actualizado
            FROM DL_FARINTER.dbo.DL_Acum_VentasHist_Kielsa AS A -- {{ ref('DL_Acum_VentasHist_Kielsa') }}
            WHERE
                A.Pais_Id = 1
                AND A.Venta_Neta > 0
                AND A.Fecha_Id >= DATEFROMPARTS(
                    YEAR(DATEADD(MONTH, -6, GETDATE())),
                    MONTH(DATEADD(MONTH, -6, GETDATE())),
                    1
                )
            GROUP BY A.Pais_Id, A.Monedero_Id
        ) AS A
        ON
            B.Monedero_Id = A.Monedero_Id
            AND B.Emp_Id = A.Pais_Id
    WHERE [AN_FARINTER].[dbo].[AN_fnc_Verificacion Id](B.Emp_Id, B.Monedero_Id) = 1 -- noqa: 
),

seguimiento_carrito AS (
    SELECT
        EMP_ID_LDCOM,
        Identidad
    FROM CRM_FARINTER.dbo.CRM_Kielsa_CarritoSeguimiento --{{ source('CRM_FARINTER', 'CRM_Kielsa_CarritoSeguimiento') }}
    WHERE
        Indicador_Carrito_Cerrado = 0
        AND NOT EXISTS (
            SELECT 1
            FROM CRM_FARINTER.dbo.[CAMPAÑA] AS C --{{ source('CRM_FARINTER', 'CAMPANA') }} -- noqa: RF05
            INNER JOIN CRM_FARINTER.dbo.CLIENTE_X_LISTA AS CL --{{ source('CRM_FARINTER', 'CLIENTE_X_LISTA') }}
                ON C.LISTA_ID = CL.LISTID
            WHERE
                GETDATE() BETWEEN C.FECHA_INICIO AND C.FECHA_FINAL
                AND C.ID_EMPRESA = CRM_Kielsa_CarritoSeguimiento.ID_EMPRESA
                AND CL.DNI = CRM_Kielsa_CarritoSeguimiento.Identidad
            GROUP BY CL.DNI, C.ID_EMPRESA
        )
    GROUP BY EMP_ID_LDCOM, Identidad
),

final AS (
    /* -- Join base with lookup tables, applying the same COALESCE logic as the SP update -- */
    SELECT
        base.*,
        '' AS [Forma Pago], -- noqa: RF05
        COALESCE(Sucursal.Sucursal_Nombre, '') AS Sucursal_Nombre,
        COALESCE(Sucursal.Zona_Nombre, '') AS Zona_Nombre,
        COALESCE(Sucursal.Departamento_Nombre, '') AS Departamento_Nombre,
        COALESCE(Sucursal.Ciudad_Nombre, '') AS Ciudad_Nombre,
        COALESCE(TipoSucursal.TipoSucursal_Nombre, '') AS TipoSucursal_Nombre,
        COALESCE(CatHorario.Categoria, '') AS Hora,
        COALESCE(CatDiaMes.Categoria, '') AS Dia_Mes,
        COALESCE(Dias.Dias_Nombre, '') AS Dia_Semana,
        COALESCE(Canal.CanalVenta_Nombre, '') AS Canal,
        COALESCE(DeptoArt.DeptoArt_Nombre, '') AS Depto_Articulo,
        COALESCE(CatEnfermedades.Categoria, '') AS Enfermedades,
        COALESCE(CatCanje.Participacion, CatEnfermedades.Participacion, 0) AS Participacion,
        COALESCE(CatCanje.Categoria, 'N/A') AS Canje,
        COALESCE(TipoCliente.TipoCliente_Nombre, '') AS TipoCliente,
        COALESCE(CMETRIC.Prom_EntreTrx, 0) AS PromedioEntre_Trx,
        COALESCE(CMETRIC.Max_DiasEntreTrx, 0) AS MaxEntre_Trx,
        COALESCE(CESTADOS.CicloVida / 30.0, 0) AS Ciclo_Vida,
        COALESCE(CESTADOS.Actividad, '') AS Actividad,
        COALESCE(CESTADOS.Nivel_Lealtad_Hoy, '') AS Lealtad,
        COALESCE(CESTADOS.Nivel_Lealtad_Ayer, '') AS Lealtad_Ayer,
        COALESCE(CESTADOS.Estado_Lealtad_Hoy, '') AS Lealtad_Estado,
        COALESCE(CESTADOS.Estado_Lealtad_Ayer, '') AS Estado_Lealtad_Ayer,
        COALESCE(CESTADOS.Alerta_Estado_Hoy, '') AS Alerta_Lealtad,
        COALESCE(CESTADOS.Alerta_Estado_Ayer, '') AS Alerta_Estado_Ayer,
        COALESCE(CESTADOS.UltimaCompra_Devolucion, 0) AS UltimaCompra_Devolucion,
        COALESCE(CESTADOS.Compra_Meses_Distintos, 0) AS Compra_Meses_Distintos,
        COALESCE(CESTADOS.Compra_X_Mes_Minimo, 0) AS Compra_X_Mes_Minimo,
        COALESCE(CESTADOS.Compra_Mes_7, 0) AS Compra_Mes_7,
        COALESCE(CESTADIST.CLV, 0) AS CLV,
        COALESCE(CESTADIST.Grupo_CLV, '') AS Grupo_CLV,
        COALESCE(CESTADIST.Grupo_Frecuencia, '') AS Grupo_Frecuencia,
        COALESCE(CESTADIST.Grupo_Margen, '') AS Grupo_Margen,
        COALESCE(CESTADIST.Grupo_Ticket, '') AS Grupo_Ticket,
        COALESCE(CESTADIST.RangoCLV, '') AS RangoCLV,
        COALESCE(CESTADIST.RangoFrecuencia, '') AS RangoFrecuencia,
        COALESCE(CESTADIST.RangoMargen, '') AS RangoMargen,
        COALESCE(CESTADIST.RangoTicketPromedio, '') AS RangoTicketPromedio,
        COALESCE(CMETRIC.Trx_Total / (7 * 1.0), 0) AS Frecuencia_Mensual,
        COALESCE(CESTADIST.Tipo_TendFrecuencia, '') AS Tipo_TendFrecuencia,
        COALESCE(CESTADIST.Tipo_TendMargen, '') AS Tipo_TendMargen,
        COALESCE(CESTADIST.Tipo_TendTicketProm, '') AS Tipo_TendTicketProm,
        COALESCE(CESTADOS.FechaInicio_UltimoCiCloVida, GETDATE()) AS FechaInicio_UltimoCiCloVida,
        CASE WHEN CARSEG.Identidad IS NOT NULL THEN 1 ELSE 0 END AS Notificar_Carrito_Pendiente
    FROM base
    LEFT JOIN AN_FARINTER.dbo.AN_Cal_ClientesCategorias_Kielsa AS CatSucursal -- {{ source('AN_FARINTER', 'AN_Cal_ClientesCategorias_Kielsa') }}
        ON
            base.Pais_Id = CatSucursal.Pais_Id
            AND base.Cliente_Id = CatSucursal.Cliente_Id
            AND CatSucursal.Tipo_Id = 'Sucursal'
    LEFT JOIN AN_FARINTER.dbo.AN_Cal_ClientesCategorias_Kielsa AS CatSegmento
        ON
            base.Pais_Id = CatSegmento.Pais_Id
            AND base.Cliente_Id = CatSegmento.Cliente_Id
            AND CatSegmento.Tipo_Id = 'Segmento'
    LEFT JOIN AN_FARINTER.dbo.AN_Cal_ClientesCategorias_Kielsa AS CatHorario
        ON
            base.Pais_Id = CatHorario.Pais_Id
            AND base.Cliente_Id = CatHorario.Cliente_Id
            AND CatHorario.Tipo_Id = 'Horario'
    LEFT JOIN AN_FARINTER.dbo.AN_Cal_ClientesCategorias_Kielsa AS CatDiaMes
        ON
            base.Pais_Id = CatDiaMes.Pais_Id
            AND base.Cliente_Id = CatDiaMes.Cliente_Id
            AND CatDiaMes.Tipo_Id = 'Dia_Mes'
    LEFT JOIN AN_FARINTER.dbo.AN_Cal_ClientesCategorias_Kielsa AS CatDiaSemana
        ON
            base.Pais_Id = CatDiaSemana.Pais_Id
            AND base.Cliente_Id = CatDiaSemana.Cliente_Id
            AND CatDiaSemana.Tipo_Id = 'Dia_Semana'
    LEFT JOIN AN_FARINTER.dbo.AN_Cal_ClientesCategorias_Kielsa AS CatCanal
        ON
            base.Pais_Id = CatCanal.Pais_Id
            AND base.Cliente_Id = CatCanal.Cliente_Id
            AND CatCanal.Tipo_Id = 'Canal'
    LEFT JOIN AN_FARINTER.dbo.AN_Cal_ClientesCategorias_Kielsa AS CatDepto_Art
        ON
            base.Pais_Id = CatDepto_Art.Pais_Id
            AND base.Cliente_Id = CatDepto_Art.Cliente_Id
            AND CatDepto_Art.Tipo_Id = 'Depto_Art'
    LEFT JOIN AN_FARINTER.dbo.AN_Cal_ClientesCategorias_Kielsa AS CatEnfermedades
        ON
            base.Pais_Id = CatEnfermedades.Pais_Id
            AND base.Cliente_Id = CatEnfermedades.Cliente_Id
            AND CatEnfermedades.Tipo_Id = 'Enfermedades'
    LEFT JOIN AN_FARINTER.dbo.AN_Cal_ClientesCategorias_Kielsa AS CatCanje
        ON
            base.Pais_Id = CatCanje.Pais_Id
            AND base.Cliente_Id = CatCanje.Cliente_Id
            AND CatCanje.Tipo_Id = 'Canje'
    LEFT JOIN AN_FARINTER.dbo.AN_Cal_ClientesCategorias_Kielsa AS CatAseguradora
        ON
            base.Pais_Id = CatAseguradora.Pais_Id
            AND base.Cliente_Id = CatAseguradora.Cliente_Id
            AND CatAseguradora.Tipo_Id = 'Aseguradora'
    LEFT JOIN BI_FARINTER.dbo.BI_Dim_Sucursal_Kielsa AS Sucursal -- {{ source('BI_FARINTER', 'BI_Dim_Sucursal_Kielsa') }}
        ON CatSucursal.Categoria = Sucursal.Sucursal_Id
    LEFT JOIN BI_FARINTER.dbo.BI_Dim_TipoSucursal_Kielsa AS TipoSucursal -- {{ source('BI_FARINTER', 'BI_Dim_TipoSucursal_Kielsa') }}
        ON CatSegmento.Categoria = TipoSucursal.TipoSucursal_Id
    LEFT JOIN BI_FARINTER.dbo.BI_Dim_Dias AS Dias -- {{ source('BI_FARINTER', 'BI_Dim_Dias') }}
        ON CONVERT(INT, CatDiaSemana.Categoria) = Dias.Dias_Id
    LEFT JOIN BI_FARINTER.dbo.BI_Dim_CanalVenta_Kielsa AS Canal -- {{ source('BI_FARINTER', 'BI_Dim_CanalVenta_Kielsa') }}
        ON CONVERT(INT, CatCanal.Categoria) = Canal.CanalVenta_Id
    LEFT JOIN BI_FARINTER.dbo.BI_Dim_DeptoArt_Kielsa AS DeptoArt -- {{ source('BI_FARINTER', 'BI_Dim_DeptoArt_Kielsa') }}
        ON CatDepto_Art.Categoria = DeptoArt.DeptoArt_Id
    LEFT JOIN BI_FARINTER.dbo.BI_Dim_TipoCliente_Kielsa AS TipoCliente -- {{ source('BI_FARINTER', 'BI_Dim_TipoCliente_Kielsa') }}
        ON CatAseguradora.Categoria = TipoCliente.TipoCliente_Id
    LEFT JOIN AN_FARINTER.dbo.AN_Cal_ClientesMetricas_Kielsa AS CMETRIC -- {{ source('AN_FARINTER', 'AN_Cal_ClientesMetricas_Kielsa') }}
        ON base.Pais_Id = CMETRIC.Pais_Id AND base.Cliente_Id = CMETRIC.Cliente_Id
    LEFT JOIN AN_FARINTER.dbo.AN_Cal_ClientesEstadisticas_Kielsa AS CESTADIST -- {{ source('AN_FARINTER', 'AN_Cal_ClientesEstadisticas_Kielsa') }}
        ON base.Pais_Id = CESTADIST.Pais_Id AND base.Cliente_Id = CESTADIST.Cliente_Id
    LEFT JOIN AN_FARINTER.dbo.AN_Cal_ClientesEstados_Kielsa AS CESTADOS -- {{ source('AN_FARINTER', 'AN_Cal_ClientesEstados_Kielsa') }}
        ON base.Pais_Id = CESTADOS.Pais_Id AND base.Cliente_Id = CESTADOS.Cliente_Id
    LEFT JOIN seguimiento_carrito AS CARSEG
        ON
            base.Pais_Id = CARSEG.EMP_ID_LDCOM
            AND base.Cliente_Id = CARSEG.Identidad COLLATE DATABASE_DEFAULT -- noqa: RF02
)

SELECT *
FROM final
