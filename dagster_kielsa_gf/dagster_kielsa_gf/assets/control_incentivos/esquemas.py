from dagster_kielsa_gf.assets.control_incentivos.config import (
    ColumnMeta as ColMeta,
    SchemaMeta,
)
import polars as pl
import polars.selectors as cs


class RegaliasSchema(SchemaMeta):
    Regalia_Id = ColMeta()
    Fecha_Id = ColMeta()
    Regalia_FechaHora = ColMeta()
    Emp_Id = ColMeta()
    Suc_Id = ColMeta()
    Bodega_Id = ColMeta()
    Caja_Id = ColMeta()
    EmpSucCajReg_Id = ColMeta()
    Detalle_Id = ColMeta()
    Articulo_Id = ColMeta()
    Articulo_Padre_Id = ColMeta()
    Cliente_Id = ColMeta()
    Identidad_Limpia = ColMeta()
    Mov_Id = ColMeta()
    Vendedor_Id = ColMeta()
    Operacion_Id = ColMeta()
    Preventa_Id = ColMeta()
    Tipo_Origen = ColMeta()
    Detalle_Momento = ColMeta()
    Cantidad_Original = ColMeta()
    Cantidad_Padre = ColMeta()
    Valor_Costo_Unitario = ColMeta()
    Valor_Costo_Total = ColMeta()
    Precio_Unitario = ColMeta()
    Valor_Impuesto = ColMeta()
    EmpSuc_Id = ColMeta()
    EmpCli_Id = ColMeta()
    EmpVen_Id = ColMeta()
    EmpMon_Id = ColMeta()
    EmpArt_Id = ColMeta()
    EmpSucCajRegDet_Id = ColMeta()


class VentasSchema(SchemaMeta):
    Emp_Id = ColMeta()
    Suc_Id = ColMeta()
    Caja_Id = ColMeta()
    Regalia_Id = ColMeta()
    Detalle_Id = ColMeta()
    Fecha_Id = ColMeta()
    Cliente_Id = ColMeta()
    Monedero_Id = ColMeta()
    Vendedor_Id = ColMeta()
    TipoPlan_Nombre = ColMeta()
    TipoCliente_Id2 = ColMeta()
    TipoCliente_Nombre = ColMeta()
    Articulo_Id = ColMeta()
    Detalle_Id = ColMeta()
    Cantidad_Padre = ColMeta()
    Valor_Acum_Monedero = ColMeta()
    Valor_Neto = ColMeta()


def set_casting_id(df: pl.LazyFrame) -> pl.LazyFrame:
    df = df.with_columns((cs.numeric() & cs.ends_with("_Id")).cast(pl.Int32))
    return df


def set_casting(df: pl.LazyFrame, fecha_col: str = "Fecha_Id") -> pl.LazyFrame:
    df = df.with_columns((cs.numeric() - cs.ends_with("_Id")).cast(pl.Float64))
    df = set_casting_id(df)
    df = df.with_columns(pl.col(fecha_col).cast(pl.Date))
    return df
