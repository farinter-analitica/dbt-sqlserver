import datetime as dt

import polars as pl

from dagster_kielsa_gf.assets.control_incentivos.reglas_incentivos import (
    build_registry_incentivo,
    ReglaIncentivoRegistry,
)
from dagster_kielsa_gf.assets.control_incentivos.config import (
    DataFramesInput,
    DataFramesOutput,
    LazyFrameWithMeta,
    ProcConfig,
)
from dagster_kielsa_gf.assets.control_incentivos.extractores import (
    get_regalias_data,
    get_calendario_data,
    get_articulos_data,
    get_vendedor_data,
    get_ventas_data,
    get_usuario_sucursal_data,
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


class ProcesamientoIncentivos:
    config: ProcConfig
    _dfm_input: DataFramesInput | None = None
    _dfm_output: DataFramesOutput | None = None

    def __init__(
        self,
        connection_str: str,
        fecha_inicio: dt.date,
        fecha_fin: dt.date,
        empresas_id: set[int] | None = None,
        limit: int | None = None,
        regla_registry: ReglaIncentivoRegistry | None = None,
    ):
        if empresas_id is None:
            empresas_id = {1, 5}
        self.config = ProcConfig(
            connection_str=connection_str,
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin,
            empresas_id=empresas_id,
            limit=limit,
        )
        self.empresas_id = empresas_id
        self.limit = limit
        self.regla_registry = regla_registry or build_registry_incentivo()

    def extract_dataframes(self) -> "ProcesamientoIncentivos":
        """
        Recopila los dataframes necesarios para el procesamiento de incentivos.
        Retorna self para permitir el encadenamiento de métodos.
        """
        self.dfm_regalias = self.inyectar_regla(get_regalias_data(self.config))
        self.dfm_calendario = get_calendario_data(self.config)
        self.dfm_articulos = get_articulos_data(self.config)
        self.dfm_vendedores = get_vendedor_data(self.config)
        self.dfm_ventas = self.inyectar_regla(get_ventas_data(self.config))
        self.dfm_usuarios_sucursales = get_usuario_sucursal_data(self.config)

        return self

    def inyectar_regla(self, dfm: LazyFrameWithMeta) -> LazyFrameWithMeta:
        # Agrupa por Emp_Id y Fecha_Id para aplicar la regla correspondiente a cada grupo
        df = dfm.frame

        # Esto agrega una columna "regla" con la instancia de la regla correspondiente
        df = self.regla_registry.asignar_regla_a_dataframe(
            df,
            emp_id_col=dfm.emp_id_name or "Emp_Id",
            fecha_col=dfm.date_name or "Fecha_Id",
        )
        return dfm.with_frame(
            df,
        )

    @property
    def dfm_input(self) -> DataFramesInput:
        if self._dfm_input is None:
            raise ValueError("Los dataframes de entrada no han sido procesados.")
        return self._dfm_input

    def procesar_dataframes(self) -> "ProcesamientoIncentivos":
        """
        Procesa y almacena los dataframes de entrada requeridos.
        Retorna self para permitir el encadenamiento de métodos.
        """
        self._dfm_input = DataFramesInput(
            regalias=self.dfm_regalias,
            calendario=self.dfm_calendario,
            articulos=self.dfm_articulos,
            vendedores=self.dfm_vendedores,
            ventas=self.dfm_ventas,
            usuarios_sucursales=self.dfm_usuarios_sucursales,
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
            output_dict = regla.procesar(self.dfm_input)
            resultados.append(output_dict)

        print(f"Procesando regla {self.regla_registry.regla_por_defecto.regla_nombre}")
        regla_defecto = self.regla_registry.regla_por_defecto.procesar(self.dfm_input)
        resultados.append(regla_defecto)

        print(f"Construyendo resultado final {'...'}")
        resultado_final = DataFramesOutput(
            regalias_incentivo=self.dfm_input.regalias.with_frame(
                pl.concat(
                    [r.regalias_incentivo.frame for r in resultados],
                    how="diagonal_relaxed",
                ),
            ),
            detalle_incentivo=LazyFrameWithMeta(
                frame=pl.concat(
                    [r.detalle_incentivo.frame for r in resultados],
                    how="diagonal_relaxed",
                ),
                primary_keys=(
                    "Fecha_Id",
                    "Emp_Id",
                    "Suc_Id",
                    "Vendedor_Id",
                    "Articulo_Id",
                    "CanalVenta_Id",
                    "Detalle_Id",
                ),
                date_name="Fecha_Id",
                emp_id_name="Emp_Id",
            ).validate_primary_keys(),
        )

        self._dfm_output = resultado_final

        return self

    @property
    def dfm_output(self) -> DataFramesOutput:
        if self._dfm_output is None:
            raise ValueError("Los dataframes de salida no han sido procesados.")
        return self._dfm_output

    def procesar_reglas(self) -> "ProcesamientoIncentivos":
        """
        Procesa los dataframes de salida requeridos.
        Retorna self para permitir el encadenamiento de métodos.
        """
        self._process_df_output_dict()

        return self

    def procesar(self) -> "ProcesamientoIncentivos":
        """
        Procesa todo.
        Retorna self para permitir el encadenamiento de métodos.
        """
        self.extract_dataframes()
        self.procesar_dataframes()
        self.procesar_reglas()
        return self


if __name__ == "__main__":
    from dagster_shared_gf.resources.sql_server_resources import dwh_farinter_bi

    DEBUG = True
    # DEBUG_KEYS = ["DF Regalias Incentivos","DF Ventas Incentivos", "DF Vendedores",  "Extract Process", "DF Regalias In", ...]
    DEBUG_KEYS = ["DF Ventas Incentivos", "DF Vendedores"]
    SAVE_PARQUET = True

    procesador_incentivos = ProcesamientoIncentivos(
        connection_str=dwh_farinter_bi.get_arrow_odbc_conn_string(),
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

    df_regalias = procesador_incentivos.dfm_regalias.frame
    print_df(
        df_regalias,
        "DF Regalias",
        **print_params,
    )
    df_calendario = procesador_incentivos.dfm_calendario.frame
    print_df(
        df_calendario,
        "DF Calendario",
        **print_params,
    )

    df_articulos = procesador_incentivos.dfm_articulos.frame
    print_df(
        df_articulos,
        "DF Articulos",
        **print_params,
    )

    df_vendedores = procesador_incentivos.dfm_vendedores.frame
    print_df(
        df_vendedores,
        "DF Vendedores",
        **print_params,
    )

    df_ventas = procesador_incentivos.dfm_ventas.frame
    print_df(
        df_ventas,
        "DF Ventas",
        **print_params,
    )

    if "Extract Process" in DEBUG_KEYS:
        procesador_incentivos.extract_dataframes()
        procesador_incentivos.procesar_dataframes()
        procesador_incentivos.procesar_reglas()

        df_regalias_in = procesador_incentivos.dfm_input.regalias.frame
        print_df(
            df_regalias_in,
            "DF Regalias In",
            **print_params,
        )

        df_regalias_incentivo = (
            procesador_incentivos.dfm_output.regalias_incentivo.frame
        )
        print_df(
            df_regalias_incentivo,
            "DF Regalias Incentivos",
            **print_params,
        )
