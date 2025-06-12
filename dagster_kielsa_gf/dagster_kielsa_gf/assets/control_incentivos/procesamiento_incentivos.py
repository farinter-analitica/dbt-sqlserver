import datetime as dt
from textwrap import dedent

import polars as pl
import polars.selectors as cs

from dagster_shared_gf.resources.sql_server_resources import SQLServerResource
from dagster_kielsa_gf.assets.control_incentivos.reglas_incentivos import (
    build_registry_incentivo,
    ReglaIncentivoRegistry,
)
from dagster_kielsa_gf.assets.control_incentivos.config import (
    DataFramesInput,
    DataFramesOutput,
    LazyFrameWithMeta,
)

built_in_print = print
DEBUG = False
DEBUG_KEYS = []


def print(msg):
    if DEBUG:
        built_in_print(msg)


cfg = pl.Config(
    set_tbl_rows=20,
    set_tbl_cols=15,
    set_tbl_formatting="UTF8_FULL_CONDENSED",
    set_float_precision=3,
)


def print_df(
    df: pl.DataFrame | pl.LazyFrame,
    message: str,
    debug: bool = DEBUG,
    debug_keys: list[str] = DEBUG_KEYS,
    save_parquet: bool = False,
    parquet_path: str | None = None,
    parquet_rows_limit: int = 100000,
):
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
        empresas_id: set[int] = {1},
        limit: int | None = None,
        regla_registry: ReglaIncentivoRegistry | None = None,
    ):
        self.dwh_farinter_bi = dwh_farinter_bi
        self.fecha_inicio = fecha_inicio
        self.fecha_fin = fecha_fin
        self.empresas_id = empresas_id
        self.limit = limit
        self.regla_registry = regla_registry or build_registry_incentivo()

    def get_regalias_data(
        self,
        fecha_inicio: dt.date | None = None,
        fecha_fin: dt.date | None = None,
        limit: int | None = None,
    ) -> LazyFrameWithMeta:
        fecha_inicio = fecha_inicio or self.fecha_inicio
        fecha_fin = fecha_fin or self.fecha_fin
        limit = limit or self.limit
        query_trx = dedent(
            f"""
                SELECT
                    {f"TOP ({limit})" if limit is not None else ""}
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
                WHERE RE.Regalia_Fecha >= '{fecha_inicio.strftime("%Y%m%d")}' 
                AND RE.Regalia_Fecha <= '{fecha_fin.strftime("%Y%m%d")}'
                AND RE.Emp_Id IN ({", ".join(map(str, self.empresas_id))})
            """
        )
        df = get_data(query_trx, self.dwh_farinter_bi)
        df = df.with_columns((cs.numeric() - cs.ends_with("_Id")).cast(pl.Float64))
        df = df.with_columns((cs.numeric() & cs.ends_with("_Id")).cast(pl.Int32))
        df = df.with_columns(pl.col("Fecha_Id").cast(pl.Date))
        return LazyFrameWithMeta(
            df,
            primary_keys=("Emp_Id", "Suc_Id", "Caja_Id", "Regalia_Id", "Detalle_Id"),
            date_name="Fecha_Id",
            emp_id_name="Emp_Id",
        )

    def get_ventas_data(
        self,
        fecha_inicio: dt.date | None = None,
        fecha_fin: dt.date | None = None,
        limit: int | None = None,
    ) -> LazyFrameWithMeta:
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
                    RD.EmpArt_Id,
                    RD.EmpSucCajRegDet_Id
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
        df = df.with_columns(pl.col("Regalia_Fecha").cast(pl.Date))
        return LazyFrameWithMeta(
            df,
            primary_keys=("Emp_Id", "Suc_Id", "Caja_Id", "Detalle_Id"),
            date_name="Fecha_Id",
            emp_id_name="Emp_Id",
        )

    def get_calendario_data(
        self,
        fecha_inicio: dt.date | None = None,
        fecha_fin: dt.date | None = None,
        limit: int | None = None,
    ) -> LazyFrameWithMeta:
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
                WHERE Fecha_Calendario >= '{fecha_inicio.replace(year=fecha_inicio.year - 1).strftime("%Y%m%d")}' 
                AND Fecha_Calendario <= '{fecha_fin.replace(year=fecha_fin.year + 1).strftime("%Y%m%d")}'
            """
        )
        df = get_data(query_trx, self.dwh_farinter_bi)
        df = df.with_columns((cs.numeric() & cs.ends_with("_Id")).cast(pl.Int32))
        df = df.with_columns(pl.col("Fecha_Calendario").cast(pl.Date))
        return LazyFrameWithMeta(
            df,
            primary_keys=("Fecha_Calendario",),
            date_name="Fecha_Calendario",
            emp_id_name=None,
        )

    def get_articulos_data(
        self,
        fecha_inicio: dt.date | None = None,
        fecha_fin: dt.date | None = None,
        limit: int | None = None,
    ) -> LazyFrameWithMeta:
        fecha_inicio = fecha_inicio or self.fecha_inicio
        fecha_fin = fecha_fin or self.fecha_fin
        limit = limit or self.limit
        query_trx = dedent(
            f"""
                SELECT  
                    {f"TOP ({limit})" if limit is not None else ""}
                    [Articulo_Id],
                    [Emp_Id],
                    [Casa_Id],
                    [Casa_Nombre],
                    [Bit_Marca_Propia],
                    NULL AS Dummy
                FROM [BI_FARINTER].[dbo].[BI_Kielsa_Dim_Articulo] Art
                WHERE  Emp_Id IN ({", ".join(map(str, self.empresas_id))})
            """
        )
        df = get_data(query_trx, self.dwh_farinter_bi)
        df = df.with_columns((cs.numeric() & cs.ends_with("_Id")).cast(pl.Int32))
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
        self,
        fecha_inicio: dt.date | None = None,
        fecha_fin: dt.date | None = None,
        limit: int | None = None,
    ) -> LazyFrameWithMeta:
        fecha_inicio = fecha_inicio or self.fecha_inicio
        fecha_fin = fecha_fin or self.fecha_fin
        limit = limit or self.limit
        query_trx = dedent(
            f"""
                SELECT  
                    {f"TOP ({limit})" if limit is not None else ""}
                    [Empleado_Id]
                    ,[Empleado_Nombre]
                    ,[Rol_Id]
                    ,[Usuario_Id]
                    ,[Rol]
                    ,[Emp_Id]
                    ,[Hash_EmpleadoEmp]
                    ,[Sucursal_Id_Asignado_Meta]
                    ,[Sucursal_Id_Asignado]
                    ,[Bit_Activo]
                    ,[EmpEmpl_Id]
                    ,[HashStr_EmplEmp]
                    ,[Vendedor_Id]
                    ,[Vendedor_Nombre]
                FROM [BI_FARINTER].[dbo].[BI_Kielsa_Dim_Empleado]
                WHERE  Emp_Id IN ({", ".join(map(str, self.empresas_id))})
            """
        )
        df = get_data(query_trx, self.dwh_farinter_bi)
        df = df.with_columns((cs.numeric() & cs.ends_with("_Id")).cast(pl.Int32))
        return LazyFrameWithMeta(
            df,
            primary_keys=(
                "Emp_Id",
                "Vendedor_Id",
            ),
            date_name=None,
            emp_id_name="Emp_Id",
        )

    def extract_dataframes(self) -> "ProcesamientoIncentivos":
        """
        Recopila los dataframes necesarios para el procesamiento de incentivos.
        Retorna self para permitir el encadenamiento de métodos.
        """
        self.dfm_regalias = self.get_regalias_data()
        self.dfm_calendario = self.get_calendario_data()
        self.dfm_articulos = self.get_articulos_data()
        self.dfm_vendedores = self.get_vendedor_data()

        return self

    def preparar_data_regalias(self) -> LazyFrameWithMeta:
        # Agrupa por Emp_Id y Fecha_Id para aplicar la regla correspondiente a cada grupo
        df_regalias = self.dfm_regalias.frame
        df_regalias = df_regalias.select(
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

        # Esto agrega una columna "regla" con la instancia de la regla correspondiente
        df_regalias = self.regla_registry.asignar_regla_a_dataframe(
            df_regalias,
            emp_id_col=self.dfm_regalias.emp_id_name or "Emp_Id",
            fecha_col=self.dfm_regalias.date_name or "Fecha_Id",
        )
        return self.dfm_regalias.with_frame(
            df_regalias,
        )

    @property
    def df_input_dict(self) -> DataFramesInput:
        if not hasattr(self, "_df_input"):
            raise ValueError("Los dataframes de entrada no han sido procesados.")
        return self._df_input

    def _process_df_input_dict(self) -> "ProcesamientoIncentivos":
        """
        Procesa y almacena los dataframes de entrada requeridos.
        Retorna self para permitir el encadenamiento de métodos.
        """
        self._df_input: DataFramesInput = DataFramesInput(
            regalias=self.preparar_data_regalias(),
            calendario=self.dfm_calendario,
            articulos=self.dfm_articulos,
            vendedores=self.dfm_vendedores,
        )
        return self

    def _process_df_output_dict(self) -> "ProcesamientoIncentivos":
        """
        Procesa y almacena los dataframes de salida requeridos.
        Retorna self para permitir el encadenamiento de métodos.
        """
        resultados: list[DataFramesOutput] = []
        # Particiona por la columna de regla (instancia)
        for regla in self.regla_registry.map_reglas.values():
            print(f"Procesando regla {regla.regla_nombre}")
            output_dict = regla.procesar(self.df_input_dict)
            resultados.append(output_dict)

        print(f"Procesando regla {self.regla_registry.regla_por_defecto.regla_nombre}")
        regla_defecto = self.regla_registry.regla_por_defecto.procesar(
            self.df_input_dict
        )
        resultados.append(regla_defecto)

        print(f"Construyendo resultado final {'...'}")
        resultado_final = DataFramesOutput(
            regalias_incentivo=self.dfm_regalias.with_frame(
                pl.concat(
                    [r.regalias_incentivo.frame for r in resultados],
                    how="diagonal_relaxed",
                ),
            ),
        )

        self._df_output = resultado_final

        return self

    def estandarizar_dataframes(self) -> "ProcesamientoIncentivos":
        """
        Estandariza los dataframes de salida para que tengan las mismas columnas llave.
        Retorna self para permitir el encadenamiento de métodos.
        """
        raise NotImplementedError("Este método no está implementado.")
        return self

    @property
    def df_output(self) -> DataFramesOutput:
        if not hasattr(self, "_df_output"):
            raise ValueError("Los dataframes de salida no han sido procesados.")
        return self._df_output

    def process_dataframes(self) -> "ProcesamientoIncentivos":
        """
        Procesa los dataframes de salida requeridos.
        Retorna self para permitir el encadenamiento de métodos.
        """
        self._process_df_input_dict()
        self._process_df_output_dict()

        return self


if __name__ == "__main__":
    from dagster_shared_gf.resources.sql_server_resources import dwh_farinter_bi

    DEBUG = True
    DEBUG_KEYS = ["DF Regalias Incentivos", "DF Regalias In", "sql"]
    SAVE_PARQUET = True

    procesador_incentivos = ProcesamientoIncentivos(
        dwh_farinter_bi=dwh_farinter_bi,
        fecha_inicio=dt.date(2025, 5, 1),
        fecha_fin=dt.date(2025, 5, 7),
        empresas_id={5},
    )

    print_params = {
        "debug": DEBUG,
        "debug_keys": DEBUG_KEYS,
        "save_parquet": SAVE_PARQUET,
    }

    procesador_incentivos.extract_dataframes()

    df_regalias = procesador_incentivos.dfm_regalias
    print_df(
        df_regalias.frame,
        "DF Regalias",
        **print_params,
    )
    df_calendario = procesador_incentivos.dfm_calendario
    print_df(
        df_calendario.frame,
        "DF Calendario",
        **print_params,
    )

    df_articulos = procesador_incentivos.dfm_articulos
    print_df(
        df_articulos.frame,
        "DF Articulos",
        **print_params,
    )

    df_vendedores = procesador_incentivos.dfm_vendedores
    print_df(
        df_vendedores.frame,
        "DF Vendedores",
        **print_params,
    )

    procesador_incentivos.extract_dataframes()
    procesador_incentivos.process_dataframes()

    df_regalias_in = procesador_incentivos.df_input_dict.regalias
    print_df(
        df_regalias_in.frame,
        "DF Regalias In",
        **print_params,
    )

    df_regalias_incentivo = procesador_incentivos.df_output.regalias_incentivo
    print_df(
        df_regalias_incentivo.frame,
        "DF Regalias Incentivos",
        **print_params,
    )
