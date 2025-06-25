import datetime as dt
from textwrap import dedent


import dagster_kielsa_gf.assets.control_incentivos.esquemas as esquemas
import polars as pl

from dagster_kielsa_gf.assets.control_incentivos.config import (
    LazyFrameWithMeta,
    ProcConfig,
)


def get_data(
    query: str, connection: str, schema: pl.Schema | None = None
) -> pl.LazyFrame:
    df = pl.read_database(query, connection, schema_overrides=schema).lazy()
    return df


def get_regalias_data(
    proc_config: ProcConfig,
) -> LazyFrameWithMeta:
    query_trx = dedent(
        f"""
            SELECT
                {f"TOP ({proc_config.limit})" if proc_config.limit is not None else ""}
                RE.Regalia_Id,
                RE.Regalia_Fecha as Fecha_Id,
                RE.Regalia_Momento AS Regalia_FechaHora,
                RE.Emp_Id,
                RE.Suc_Id,
                RE.Bodega_Id,
                RE.Caja_Id,
                RE.EmpSucCajReg_Id,
                RD.Detalle_Id,
                RD.Articulo_Id,
                RD.Articulo_Padre_Id,
                RE.Cliente_Id,
                RE.Identidad_Limpia,
                RE.Mov_Id,
                RE.Vendedor_Id,
                RE.Operacion_Id,
                RE.Preventa_Id,
                RE.Tipo_Origen,
                RD.Detalle_Momento,
                RD.Cantidad_Original,
                RD.Cantidad_Padre,
                RD.Valor_Costo_Unitario,
                RD.Valor_Costo_Total,
                RD.Precio_Unitario,
                RD.Valor_Impuesto,
                RE.EmpSuc_Id,
                RE.EmpCli_Id,
                RE.EmpVen_Id,
                RE.EmpMon_Id,
                RD.EmpArt_Id,
                RD.EmpSucCajRegDet_Id
            FROM [BI_FARINTER].[dbo].[BI_Kielsa_Hecho_Regalia_Detalle] RD
            INNER JOIN  [BI_FARINTER].[dbo].[BI_Kielsa_Hecho_Regalia_Encabezado] RE
            ON RD.Regalia_Id = RE.Regalia_Id
            AND RD.Emp_Id = RE.Emp_Id
            AND RD.Suc_Id = RE.Suc_Id
            AND RD.Bodega_Id = RE.Bodega_Id
            AND RD.Caja_Id = RE.Caja_Id
            WHERE RE.Regalia_Fecha >= CAST('{proc_config.fecha_inicio.strftime("%Y%m%d")}' AS DATE)
            AND RE.Regalia_Fecha <= CAST('{proc_config.fecha_fin.strftime("%Y%m%d")}' AS DATE)
            AND RE.Emp_Id IN ({", ".join(map(str, proc_config.empresas_id))})
        """
    )
    df = get_data(
        query_trx,
        connection=proc_config.connection_str,
        schema=esquemas.RegaliasSchema.to_schema(),
    )
    df = df.select(
        [
            "Emp_Id",
            "Suc_Id",
            "Caja_Id",
            "Vendedor_Id",
            "Regalia_Id",
            "Detalle_Id",
            "Fecha_Id",
            "Articulo_Padre_Id",
            "EmpSucCajRegDet_Id",
            "Cantidad_Padre",
            "Valor_Costo_Total",
        ]
    )
    return LazyFrameWithMeta(
        df,
        primary_keys=("Emp_Id", "Suc_Id", "Caja_Id", "Regalia_Id", "Detalle_Id"),
        date_name="Fecha_Id",
        emp_id_name="Emp_Id",
        schema=esquemas.RegaliasSchema,
    )


def get_ventas_data(
    proc_config,
) -> LazyFrameWithMeta:
    query_trx = dedent(
        f"""
            SELECT
                {f"TOP ({proc_config.limit})" if proc_config.limit is not None else ""}
                FE.Emp_Id,
                FE.Suc_Id,
                FE.TipoDoc_id,
                FE.Caja_Id,
                FE.Factura_Id,
                FE.Factura_Fecha AS Fecha_Id,
                MAX(FE.Vendedor_Id) AS Vendedor_Id,
                MAX(FE.Usuario_Id) AS Usuario_Id,
                MAX(FP.CanalVenta_Id) AS CanalVenta_Id,
                --ISNULL(FE.Cliente_Id, 0) AS Cliente_Id,
                FE.EmpSucDocCajFac_Id,
                --ISNULL(FE.Monedero_Id, 'X') AS Monedero_Id,
                ISNULL(MAX(M.Tipo_Plan), 'X') AS TipoPlan_Nombre,
                ISNULL(MAX(TC.TipoCliente_Id), 0) AS TipoCliente_Id,
                ISNULL(MAX(TC.TipoCliente_Nombre), 'X') AS TipoCliente_Nombre,
                FP.Articulo_Id,
                --FP.Detalle_Id,
                SUM(FP.Cantidad_Padre) AS Cantidad_Padre,
                SUM(FP.Valor_Acum_Monedero) AS Valor_Acum_Monedero,
                SUM(FP.Valor_Neto) AS Valor_Neto,
                SUM(FP.Valor_Utilidad) AS Valor_Utilidad,
                SUM(FP.Descuento_Proveedor) AS Valor_Descuento_Proveedor
            FROM BI_FARINTER.dbo.BI_Kielsa_Hecho_FacturaEncabezado FE
            INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Hecho_FacturaPosicion FP
                ON FE.Emp_Id = FP.Emp_Id
                AND FE.Suc_Id = FP.Suc_Id
                AND FE.TipoDoc_id = FP.TipoDoc_id
                AND FE.Caja_Id = FP.Caja_Id
                AND FE.Factura_Id = FP.Factura_Id
                AND FE.Factura_Fecha = FP.Factura_Fecha
            LEFT JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Monedero M
                ON FE.Emp_Id = M.Emp_Id
                AND FE.Monedero_Id = M.Monedero_Id
            LEFT JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Cliente C
                ON FE.Emp_Id = C.Emp_Id
                AND FE.Cliente_Id = C.Cliente_Id
            LEFT JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_TipoCliente TC
                ON FE.Emp_Id = TC.Emp_Id
                AND C.Tipo_Cliente_Id = TC.TipoCliente_Id
            WHERE FE.Factura_Fecha>= CAST('{proc_config.fecha_inicio.strftime("%Y%m%d")}' AS DATE)
            AND FE.Factura_Fecha<= CAST('{proc_config.fecha_fin.strftime("%Y%m%d")}' AS DATE)
            AND FE.Emp_Id IN ({", ".join(map(str, proc_config.empresas_id))})
            GROUP BY 
            FE.Emp_Id,
            FE.Suc_Id,
            FE.TipoDoc_id,
            FE.Factura_Fecha,
            FE.Caja_Id,
            FE.Factura_Id,
            FE.EmpSucDocCajFac_Id,
            FP.Articulo_Id
        """
    )
    df = get_data(
        query_trx,
        connection=proc_config.connection_str,
        schema=esquemas.VentasSchema.to_schema(),
    )
    df = esquemas.set_casting(df)
    return LazyFrameWithMeta(
        df,
        primary_keys=(
            "Emp_Id",
            "Suc_Id",
            "TipoDoc_id",
            "Caja_Id",
            "Factura_Id",
            "Articulo_Id",
            # "Detalle_Id",
        ),
        date_name="Fecha_Id",
        emp_id_name="Emp_Id",
        schema=esquemas.VentasSchema,
    )


def get_calendario_data(
    proc_config,
) -> LazyFrameWithMeta:
    query_trx = dedent(
        f"""
            SELECT 
                {f"TOP ({proc_config.limit})" if proc_config.limit is not None else ""}
                [Fecha_Calendario],
                [Anio_Calendario],
                [Mes_Calendario],
                [Dia_Calendario],
                [Trimestre_Calendario],
                [Semana_del_Trimestre],
                [Dia_del_Trimestre],
                [Mes_Inicio],
                [Mes_Fin],
                [Es_Fin_Mes],
                [Semana_del_Mes],
                [Semana_Inicio],
                [Semana_Fin],
                [Mes_Nombre],
                [Mes_Nombre_Corto],
                [Dia_de_la_Semana],
                [Dia_Nombre],
                [Dia_del_Anio],
                [Anio_ISO],
                [Semana_del_Anio_ISO],
                [Mes_ISO],
                [Dia_del_Anio_ISO],
                [Es_Fin_Anio],
                [Es_dia_Habil],
                [NoLaboral_Paises],
                [Es_Inicio_Anio],
                [Es_Inicio_Mes],
                [AnioMes_Id],
                NULL AS Dummy
            FROM [BI_FARINTER].[dbo].[BI_Dim_Calendario_Dinamico] CAL
            WHERE Fecha_Calendario >= CAST('{proc_config.fecha_inicio.strftime("%Y%m%d")}' AS DATE)
            AND Fecha_Calendario <= CAST('{proc_config.fecha_fin.strftime("%Y%m%d")}' AS DATE)
        """
    )
    df = get_data(query_trx, connection=proc_config.connection_str)
    df = esquemas.set_casting_id(df)
    df = df.with_columns(pl.col("Fecha_Calendario").cast(pl.Date))
    return LazyFrameWithMeta(
        df,
        primary_keys=("Fecha_Calendario",),
        date_name="Fecha_Calendario",
        emp_id_name=None,
    )


def get_articulos_data(
    proc_config,
) -> LazyFrameWithMeta:
    query_trx = dedent(
        f"""
            SELECT  
                {f"TOP ({proc_config.limit})" if proc_config.limit is not None else ""}
                [Articulo_Id],
                [Emp_Id],
                [Casa_Id],
                [Casa_Nombre],
                [Bit_Marca_Propia],
                NULL AS Dummy
            FROM [BI_FARINTER].[dbo].[BI_Kielsa_Dim_Articulo] Art
            WHERE  Emp_Id IN ({", ".join(map(str, proc_config.empresas_id))})
        """
    )
    df = get_data(query_trx, proc_config.connection_str)
    df = esquemas.set_casting_id(df)
    return LazyFrameWithMeta(
        df,
        primary_keys=(
            "Emp_Id",
            "Articulo_Id",
        ),
        date_name=None,
        emp_id_name="Emp_Id",
    )


def get_vendedor_data(
    proc_config,
) -> LazyFrameWithMeta:
    query_trx = dedent(
        f"""
            SELECT  
                {f"TOP ({proc_config.limit})" if proc_config.limit is not None else ""}
                    E.[Empleado_Id]
                ,E.[Empleado_Nombre]
                ,E.[Rol_Id]
                ,E.[Usuario_Id]
                ,E.[Rol]
                ,E.[Emp_Id]
                ,E.[Hash_EmpleadoEmp]
                ,E.[Sucursal_Id_Asignado_Meta]
                ,E.[Sucursal_Id_Asignado]
                ,E.[Bit_Activo]
                ,E.[EmpEmpl_Id]
                ,E.[HashStr_EmplEmp]
                ,E.[Vendedor_Id]
                ,E.[Vendedor_Nombre]
                ,SR.[Rol_Nombre]
                ,SR.[Rol_Jerarquia]
            FROM [BI_FARINTER].[dbo].[BI_Kielsa_Dim_Empleado] E
            LEFT JOIN [DL_FARINTER].[dbo].[DL_Kielsa_Seg_Rol] SR
            ON E.Rol_Id = SR.Rol_Id
            AND E.Emp_Id = SR.Emp_Id
            WHERE  E.Emp_Id IN ({", ".join(map(str, proc_config.empresas_id))})
        """
    )
    df = get_data(query_trx, proc_config.connection_str)
    df = esquemas.set_casting_id(df)
    return LazyFrameWithMeta(
        df,
        primary_keys=(
            "Emp_Id",
            "Vendedor_Id",
        ),
        date_name=None,
        emp_id_name="Emp_Id",
        schema=esquemas.VendedorSchema,
    )


def get_usuario_sucursal_data(
    proc_config,
) -> LazyFrameWithMeta:
    query_trx = dedent(
        f"""
            SELECT
                {f"TOP ({proc_config.limit})" if proc_config.limit is not None else ""}
                    Usuario_Id
                    ,Suc_Id
                    ,Emp_Id
                    ,Rol_Sucursal
                    ,Rol_Id
                    ,Rol_Nombre
                    ,Rol_Jerarquia
                    ,Vendedor_Id
                    ,Usuario_Nombre
                    ,Bit_Activo
                    ,EmpSuc_Id
                    ,EmpSucUsu_Id
                    ,EmpRol_Id
                    ,EmpVen_Id
            FROM [BI_FARINTER].[dbo].[BI_Kielsa_Dim_UsuarioSucursal]
            WHERE Emp_Id IN ({", ".join(map(str, proc_config.empresas_id))})                      
        """
    )
    df = get_data(query_trx, proc_config.connection_str)
    df = esquemas.set_casting_id(df)
    dfm: LazyFrameWithMeta = LazyFrameWithMeta(
        df,
        primary_keys=(
            "Emp_Id",
            "Usuario_Id",
        ),
        date_name=None,
        emp_id_name="Emp_Id",
        schema=esquemas.UsuarioSucursalSchema,
    )
    return dfm


def get_rol_data(
    proc_config,
) -> LazyFrameWithMeta:
    query_trx = dedent(
        f"""
            SELECT  
                {f"TOP ({proc_config.limit})" if proc_config.limit is not None else ""}
                [Emp_Id],
                [Rol_Id],
                [Rol_Nombre],
                [Rol_Padre],
                [Rol_Jerarquia]
            FROM [DL_FARINTER].[dbo].[DL_Kielsa_Seg_Rol]
            WHERE Emp_Id IN ({", ".join(map(str, proc_config.empresas_id))})
        """
    )
    df = get_data(query_trx, proc_config.connection_str)
    df = esquemas.set_casting_id(df)
    return LazyFrameWithMeta(
        df,
        primary_keys=(
            "Emp_Id",
            "Rol_Id",
        ),
        date_name=None,
        emp_id_name="Emp_Id",
        schema=esquemas.RolSchema,
    )


if __name__ == "__main__":
    from dagster_shared_gf.resources.sql_server_resources import dwh_farinter_bi

    connection_str = dwh_farinter_bi.get_arrow_odbc_conn_string()

    class self_config:
        connection_str = connection_str
        fecha_inicio = dt.date(2025, 5, 1)
        fecha_fin = dt.date(2025, 5, 31)
        empresas_id = {5}
        limit = None

    dfm = get_ventas_data(self_config)
    print(dfm.frame.collect().describe())
    print(f"dfm.primary_keys={dfm.primary_keys}")
    print(f"dfm.date_name={dfm.date_name}")
    print(f"dfm.emp_id_name={dfm.emp_id_name}")
