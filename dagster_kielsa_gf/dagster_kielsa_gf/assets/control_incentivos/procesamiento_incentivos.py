from collections import deque
import datetime as dt
from textwrap import dedent
from typing import Callable

import polars as pl
import polars.selectors as cs

from dagster_shared_gf.resources.sql_server_resources import SQLServerResource


built_in_print = print
DEBUG = False
DEBUG_KEYS = []


def print(msg):
    if DEBUG:
        built_in_print(msg)


# Configure Polars display settings
cfg = pl.Config(
    set_tbl_rows=20,  # Maximum number of rows to display
    set_tbl_cols=15,  # Maximum number of columns to display
    # fmt_str_lengths=30,  # Maximum string length to display
    # tbl_hide_column_data_types=False,  # Show column data types
    # tbl_hide_dataframe_shape=False,  # Show dataframe dimensions
    set_tbl_formatting="UTF8_FULL_CONDENSED",  # Use full ASCII borders for better readability
    set_float_precision=3,  # Set float precision to 2 decimal places
    # tbl_cell_alignment="RIGHT",  # Right-align cell content
    # tbl_width_chars=None,  # Auto-determine table width
    # ascii_tables=False,  # Use Unicode instead of ASCII for tables
    # verbose=False,  # Don't show verbose output
)


# Custom print function for better dataframe visualization
def print_df(
    df: pl.DataFrame | pl.LazyFrame,
    message: str,
    debug: bool = DEBUG,
    debug_keys: list[str] = DEBUG_KEYS,
    save_parquet: bool = False,
    parquet_path: str | None = None,
    parquet_rows_limit: int = 1000,
):
    """
    Print a dataframe with additional context information.

    Args:
        df: Polars DataFrame to print
        message: Optional message to display before the dataframe
    """

    if (
        debug
        and message
        and (any(key in message for key in debug_keys) or debug_keys == [])
    ):
        if isinstance(df, pl.LazyFrame):
            df = df.collect(engine="auto")

        print(f"\n{'=' * 80}\n{message}\n{'=' * 80}")

        print(
            f"\nShape: {df.shape} | Memory usage: {df.estimated_size(unit='mb'):.2f} MB"
        )

        # Print head paginated by columns
        n_rows_to_print = 20
        n_cols_limit = int(cfg.state().get("tbl_cols", 15) or 15)
        total_cols = df.width
        df_head = df.head(n_rows_to_print)

        if total_cols <= n_cols_limit:
            print(df_head)
        else:
            for i in range(0, total_cols, n_cols_limit):
                end_idx = min(i + n_cols_limit, total_cols)
                chunk = df_head.select(df_head.columns[i:end_idx])
                if total_cols > n_cols_limit:
                    print(f"\n--- Columns {i + 1} to {end_idx} ---")
                print(chunk)

        print(f"\n{'=' * 80}\n")
        print(df.describe())

        if save_parquet:
            if parquet_path is None:
                parquet_path = (
                    f".cache/{message.replace(' ', '_').replace(':', '')}.parquet"
                )
            df.limit(parquet_rows_limit).write_parquet(parquet_path)
            print(f"DataFrame saved to {parquet_path}")


def get_data(query: str, dwh_farinter_bi: SQLServerResource) -> pl.LazyFrame:
    df = pl.read_database(query, dwh_farinter_bi.get_arrow_odbc_conn_string()).lazy()
    return df


class ProcesamientoIncentivos:
    def __init__(
        self,
        dwh_farinter_bi: SQLServerResource,
        fecha_inicio: dt.date,
        fecha_fin: dt.date,
        empresas_id: set[int] = {1},  # Empresas Id de las que se extraerán los datos
        limit: int | None = None,
    ):
        self.dwh_farinter_bi = dwh_farinter_bi
        self.fecha_inicio = fecha_inicio
        self.fecha_fin = fecha_fin
        self.empresas_id = empresas_id
        self.limit = limit

    def get_regalias_data(
        self,
        fecha_inicio: dt.date | None = None,
        fecha_fin: dt.date | None = None,
        limit: int | None = None,
    ) -> pl.LazyFrame:
        fecha_inicio = fecha_inicio or self.fecha_inicio
        fecha_fin = fecha_fin or self.fecha_fin
        limit = limit or self.limit
        query_trx = dedent(
            f"""
                SELECT
                    {f"TOP ({limit})" if limit is not None else ""}
                    RE.Regalia_Id,
                    RE.Regalia_Fecha,
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
                    RD.EmpArt_Id
                FROM [BI_FARINTER].[dbo].[BI_Kielsa_Hecho_Regalia_Detalle] RD
                INNER JOIN  [BI_FARINTER].[dbo].[BI_Kielsa_Hecho_Regalia_Encabezado] RE
                ON RD.Regalia_Id = RE.Regalia_Id
                AND RD.Emp_Id = RE.Emp_Id
                AND RD.Suc_Id = RE.Suc_Id
                AND RD.Bodega_Id = RE.Bodega_Id
                AND RD.Caja_Id = RE.Caja_Id
                WHERE RE.Regalia_Fecha >= '{fecha_inicio.strftime("%Y%m%d")}' 
                AND RE.Regalia_Fecha <= '{fecha_fin.strftime("%Y%m%d")}'
                AND RE.Emp_Id IN ({", ".join(map(str, self.empresas_id))})
            """
        )

        df = get_data(query_trx, self.dwh_farinter_bi)
        df = df.with_columns((cs.numeric() - cs.ends_with("_Id")).cast(pl.Float64))
        df = df.with_columns((cs.numeric() & cs.ends_with("_Id")).cast(pl.Int32))
        return df

    def get_calendario_data(
        self,
        fecha_inicio: dt.date | None = None,
        fecha_fin: dt.date | None = None,
        limit: int | None = None,
    ) -> pl.LazyFrame:
        fecha_inicio = fecha_inicio or self.fecha_inicio
        fecha_fin = fecha_fin or self.fecha_fin
        limit = limit or self.limit
        query_trx = dedent(
            f"""
                SELECT 
                    {f"TOP ({limit})" if limit is not None else ""}
                    [Fecha_Calendario],
                    [Anio_Calendario],
                    [Mes_Calendario],
                    [Dia_Calendario],
                    [Trimestre_Calendario],
                    [Semana_del_Trimestre],
                    [Dia_del_Trimestre],
                    --[AnioMes],
                    [Mes_Inicio],
                    [Mes_Fin],
                    [Es_Fin_Mes],
                    --[Dias_en_Mes],
                    [Semana_del_Mes],
                    [Semana_Inicio],
                    [Semana_Fin],
                    [Mes_Nombre],
                    [Mes_Nombre_Corto],
                    [Dia_de_la_Semana],
                    [Dia_Nombre],
                    [Dia_del_Anio],
                    --[Fecha_Id],
                    [Anio_ISO],
                    [Semana_del_Anio_ISO],
                    [Mes_ISO],
                    [Dia_del_Anio_ISO],
                    [Es_Fin_Anio],
                    [Es_dia_Habil],
                    [NoLaboral_Paises],
                    --[NoLaboral_Sociedades],
                    [Es_Inicio_Anio],
                    --[Es_Bisiesto],
                    [Es_Inicio_Mes],
                    --[Semana_del_Anio],
                    [AnioMes_Id],
                    --[Periodo_Diario],
                    --[Periodo_Diario_Orden],
                    --[Periodo_Anual],
                    --[Periodo_Anual_Orden],
                    --[Periodo_Mensual],
                    --[Periodo_Mensual_Orden],
                    --[Meses_Desde_Fecha_Procesado],
                    --[Años_Desde_Fecha_Procesado],
                    --[Fecha_Procesado],
                    --[Mes_Relativo],
                    --[Anio_Relativo],
                    --[Dia_Relativo],
                    --[Mes_NN_Relativo],
                    --[Mes_Relativo_Tipo_Semestral],
                    --[Mes_Relativo_Tipo_Trimestral],
                    --[Dia_Relativo_Rango_30Dias],
                    --[Dia_Relativo_Rango_30Dias_Orden],
                    --[Ciclo12_N],
                    --[Ciclo12_Periodo],
                    NULL AS Dummy
                FROM [BI_FARINTER].[dbo].[BI_Dim_Calendario_Dinamico] CAL
                WHERE Fecha_Calendario >= '{fecha_inicio.replace(year=fecha_inicio.year - 1).strftime("%Y%m%d")}' 
                AND Fecha_Calendario <= '{fecha_fin.replace(year=fecha_fin.year + 1).strftime("%Y%m%d")}'
            """
        )

        df = get_data(query_trx, dwh_farinter_bi)
        df = df.with_columns((cs.numeric() & cs.ends_with("_Id")).cast(pl.Int32))

        return df.drop("Dummy")

    def get_articulos_data(
        self,
        fecha_inicio: dt.date | None = None,
        fecha_fin: dt.date | None = None,
        limit: int | None = None,
    ) -> pl.LazyFrame:
        fecha_inicio = fecha_inicio or self.fecha_inicio
        fecha_fin = fecha_fin or self.fecha_fin
        limit = limit or self.limit
        query_trx = dedent(
            f"""
                SELECT  
                    {f"TOP ({limit})" if limit is not None else ""}
                    [Articulo_Id],
                    [Emp_Id],
                    --[Version_Id],
                    --[Version_Fecha],
                    --[Nauca_Id],
                    [Casa_Id],
                    [Casa_Nombre],
                    --[DeptoArt_Id],
                    --[DeptoArt_Nombre],
                    --[Categoria_Id],
                    --[Categoria_Nombre],
                    --[Tipo_Articulo_id],
                    --[Articulo_Nombre],
                    --[Articulo_Nombre_Corto],
                    --[Articulo_Codigo_Padre],
                    --[Articulo_Custom1],
                    --[Articulo_Puntaje],
                    --[Articulo_Rotacion_Dias],
                    --[Articulo_Rotacion_Maximo],
                    --[Articulo_Rotacion_Minimo],
                    --[Articulo_Costo_Actual],
                    --[Articulo_Margen1],
                    --[Articulo_Precio1],
                    --[Articulo_Compuesto],
                    --[Articulo_Activo],
                    --[Articulo_Vigencia_Inicio],
                    --[Articulo_Vigencia_Fin],
                    --[Articulo_Ultimo_costo],
                    --[Articulo_Sugerencia],
                    --[Articulo_Cantidad_Minima],
                    --[Articulo_Costo_Actual_Dolar],
                    --[Articulo_Ultimo_Costo_Dolar],
                    --[Articulo_Tipo_Cambio],
                    --[Articulo_Modelo],
                    --[Articulo_Costo_Neto],
                    --[Articulo_Costo_Bruto],
                    --[Articulo_Venta_Bajo_Costo],
                    --[Articulo_Hijo_Hereda_Precio],
                    --[Articulo_Express],
                    --[Articulo_DevuelveProveedor],
                    --[Articulo_Limite_Desc],
                    --[Articulo_Activo_Venta],
                    --[Articulo_Produccion_Post_Venta],
                    --[Articulo_Venta_Ecommerce],
                    --[Articulo_Nombre_Ecommerce],
                    --[Articulo_Visibilidad],
                    --[Articulo_SAP],
                    --[CodigoBarra_Id],
                    --[PrincipioActivo_Id],
                    --[PrincipioActivo_Nombre],
                    --[Indicador_PadreHijo],
                    --[Factor_Numerador],
                    --[Factor_Denominador],
                    --[Proveedor_Id],
                    --[Proveedor_Nombre],
                    --[Cuadro],
                    --[Hash_ArticuloEmp],
                    --[HashStr_ArtEmp],
                    --[Cuadro_Meta],
                    --[Cuadro_Fecha],
                    --[Hash_CatEmp],
                    --[Hash_DeptoArtEmp],
                    --[Hash_SubCatsEmp],
                    --[Hash_CasaEmp],
                    --[HashStr_CasaEmp],
                    --[Hash_MarcaEmp],
                    --[Mecanica],
                    --[Sintoma],
                    --[ABC_Cadena],
                    --[Tipo_Aliado],
                    --[SubCategoria1Art_Id],
                    --[SubCategoria1Art_Nombre],
                    --[SubCategoria2Art_Id],
                    --[SubCategoria2Art_Nombre],
                    --[SubCategoria3Art_Id],
                    --[SubCategoria3Art_Nombre],
                    --[SubCategoria4Art_Id],
                    --[SubCategoria4Art_Nombre],
                    --[Bit_Cronico],
                    --[Bit_Recomendacion],
                    --[Bit_MPA],
                    --[Bit_MPA_Depto],
                    [Bit_Marca_Propia],
                    --[Alerta_Cronico],
                    --[Alerta_Recomendacion],
                    --[Etiquetas],
                    --[Fecha_Carga],
                    --[Fecha_Actualizado],
                    --[EmpArt_Id],
                    --[Marca_Id],
                    --[Marca_Nombre],
                    NULL AS Dummy
                FROM [BI_FARINTER].[dbo].[BI_Kielsa_Dim_Articulo] Art
                WHERE  Emp_Id IN ({", ".join(map(str, self.empresas_id))})
            """
        )

        df = get_data(query_trx, self.dwh_farinter_bi)
        df = df.with_columns((cs.numeric() & cs.ends_with("_Id")).cast(pl.Int32))

        return df

    def collect_dataframes(self):
        self.df_regalias = self.get_regalias_data()
        self.df_calendario = self.get_calendario_data()
        self.df_articulos = self.get_articulos_data()

    def process_regalias_incentivo(self) -> pl.LazyFrame:
        # Canjes: Reporte por regalías convertidos a padres jueves a miércoles cada semana,
        # se divide en dos semana en fin de mes → Filtrar por casa y quitar marcas propias → Pharmedic paga 8 LPS,
        # demás por casa id 15 LPS independientemente valor del canje, se paga por unidad de canje.

        def calc_emp_1(df: pl.LazyFrame) -> pl.LazyFrame:
            """Calculo de incentivo para empresa 1 - Honduras"""
            incentivo_por_defecto = 15
            map_incentivo_casa = {
                266: 8,  # PHARMEDIC
                869: 8,  # PHARMEDIC NGM
            }

            df_map_incentivo_casa = pl.LazyFrame(
                {
                    "Casa_Id": map_incentivo_casa.keys(),
                    "incentivo_casa": map_incentivo_casa.values(),
                }
            )
            df = (
                df.with_columns(
                    canje_aplica_incentivo=(pl.col("Bit_Marca_Propia") != 1)
                )
                .join(df_map_incentivo_casa, on="Casa_Id", how="left")
                .with_columns(
                    valor_incentivo_unitario=pl.when(pl.col("canje_aplica_incentivo"))
                    .then(
                        pl.coalesce(
                            pl.col("incentivo_casa"), pl.lit(incentivo_por_defecto)
                        )
                    )
                    .otherwise(pl.lit(0.0))
                )
                .with_columns(
                    valor_incentivo_total=pl.col("Cantidad_Padre")
                    * pl.col("valor_incentivo_unitario")
                )
            )

            return df

        # Todas las funciones deberan retornar el mismo resultado

        def cal_emp_defecto(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns(
                canje_aplica_incentivo=0,
                valor_incentivo_unitario=pl.lit(None),
                valor_incentivo_total=pl.lit(None),
            )

        emp_func: dict[int, Callable[[pl.LazyFrame], pl.LazyFrame]] = {
            1: calc_emp_1,
        }

        df = (
            self.df_regalias.clone()
            .select(
                [
                    "Emp_Id",
                    "Suc_Id",
                    pl.col("Regalia_Fecha").cast(pl.Date).alias("Fecha_Id"),
                    "Articulo_Padre_Id",
                    "Vendedor_Id",
                    "Cantidad_Padre",
                    "Valor_Costo_Total",
                ]
            )
            .group_by(
                [
                    "Emp_Id",
                    "Suc_Id",
                    "Fecha_Id",
                    "Articulo_Padre_Id",
                    "Vendedor_Id",
                ]
            )
            .agg(
                pl.col("Cantidad_Padre").sum().alias("Cantidad_Padre"),
                pl.col("Valor_Costo_Total").sum().alias("Valor_Costo_Total"),
            )
        )
        df_art = self.df_articulos.clone().select(
            ["Emp_Id", "Articulo_Id", "Bit_Marca_Propia", "Casa_Id"]
        )
        df = df.join(
            df_art,
            left_on=["Emp_Id", "Articulo_Padre_Id"],
            right_on=["Emp_Id", "Articulo_Id"],
            how="inner",
        ).drop(pl.selectors.ends_with("_right"))

        incentivos_procesados: deque = deque()
        for df_emp in df.collect(engine="streaming").partition_by("Emp_Id"):
            incentivos_procesados.append(
                emp_func.get(df_emp["Emp_Id"][0], cal_emp_defecto)(df_emp.lazy())
            )

        return pl.concat(incentivos_procesados)


if __name__ == "__main__":
    from dagster_shared_gf.resources.sql_server_resources import dwh_farinter_bi

    DEBUG = True
    DEBUG_KEYS = ["DF Regalias Incentivos", "sql"]
    SAVE_PARQUET = True

    procesador_incentivos = ProcesamientoIncentivos(
        dwh_farinter_bi=dwh_farinter_bi,
        fecha_inicio=dt.date(2025, 1, 1),
        fecha_fin=dt.date(2025, 1, 5),
    )

    print_params = {
        "debug": DEBUG,
        "debug_keys": DEBUG_KEYS,
        "save_parquet": SAVE_PARQUET,
    }

    procesador_incentivos.collect_dataframes()

    df_regalias = procesador_incentivos.df_regalias
    print_df(
        df_regalias,
        "DF Regalias",
        **print_params,
    )
    df_calendario = procesador_incentivos.df_calendario
    print_df(
        df_calendario,
        "DF Calendario",
        **print_params,
    )

    df_articulos = procesador_incentivos.df_articulos
    print_df(
        df_articulos,
        "DF Articulos",
        **print_params,
    )

    df_regalias_incentivo = procesador_incentivos.process_regalias_incentivo()
    print_df(
        df_regalias_incentivo,
        "DF Regalias Incentivos",
        **print_params,
    )
