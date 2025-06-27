from dagster_kielsa_gf.assets.control_incentivos.reglas_incentivos import config
import polars as pl
import datetime as dt
from dagster_kielsa_gf.assets.control_incentivos import (
    extractores,
    procesamiento_incentivos,
)
from dagster_shared_gf.resources.sql_server_resources import dwh_farinter_bi
from pathlib import Path

DEBUG = True
DEBUG_KEYS = []

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
        print(f"\n{'=' * 80}\n{message}\n{'=' * 80}")
        if isinstance(df, pl.LazyFrame):
            print(df.explain(engine="streaming"))
            df = df.collect(engine="auto")
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
            if not Path(parquet_path).parent.exists():
                Path(parquet_path).parent.mkdir(parents=True, exist_ok=True)
            df.limit(parquet_rows_limit).write_parquet(parquet_path)
            print(f"DataFrame saved to {parquet_path}")


procesador_incentivos = procesamiento_incentivos.ProcesamientoIncentivos(
    connection_str=dwh_farinter_bi.get_arrow_odbc_conn_string(),
    fecha_inicio=dt.date(2025, 6, 1),
    fecha_fin=dt.date(2025, 6, 7),
    empresas_id={5},
)


if __name__ == "__main__":
    DEBUG = True
    # DEBUG_KEYS = ["DF Regalias Incentivos","DF Ventas Incentivos", "DF Vendedores",
    # "Extract Process", "DF Regalias In", DF Usuarios Sucursales, ...]
    DEBUG_KEYS = [
        # "DF Usuarios Sucursales",
        # "DF Ventas Incentivos",
        # "DF Jerarquia Vendedores",
        # "DF Jerarquia Roles",
        "DF Regalias Incentivos",
    ]

    SAVE_PARQUET = True

    dataframes = (
        procesador_incentivos.extract_dataframes().procesar_dataframes().dfm_input
    )

    dfm_jerarquia_roles = config.ReglaIncentivoSV2025().jerarquia_roles(
        df_roles=dataframes.roles
    )

    print_df(
        dfm_jerarquia_roles.frame,
        "DF Jerarquia Roles",
        save_parquet=SAVE_PARQUET,
        debug_keys=DEBUG_KEYS,
    )

    dfm_usuarios_sucursales = extractores.get_usuario_sucursal_data(
        procesador_incentivos.config
    )

    print_df(
        dfm_usuarios_sucursales.frame,
        "DF Usuarios Sucursales",
        save_parquet=SAVE_PARQUET,
        debug_keys=DEBUG_KEYS,
    )

    df_jerarquia_vendedores = (
        config.ReglaIncentivoSV2025()
        .crear_matriz_jerarquia_vendedores(
            dfmu=dataframes.usuarios_sucursales, dfmr=dataframes.roles
        )
        .frame
    )

    print_df(
        df_jerarquia_vendedores,
        "DF Jerarquia Vendedores",
        save_parquet=SAVE_PARQUET,
        debug_keys=DEBUG_KEYS,
    )

    dfm_ventas_incentivos = config.ReglaIncentivoSV2025().procesar_ventas(
        dataframes=procesador_incentivos.extract_dataframes()
        .procesar_dataframes()
        .dfm_input
    )

    print_df(
        dfm_ventas_incentivos.frame,
        "DF Ventas Incentivos",
        save_parquet=SAVE_PARQUET,
        debug_keys=DEBUG_KEYS,
        parquet_rows_limit=1000000,
    )

    dfm_regalias_incentivos = config.ReglaIncentivoSV2025().procesar_regalias(
        dataframes=procesador_incentivos.extract_dataframes()
        .procesar_dataframes()
        .dfm_input
    )

    print_df(
        dfm_regalias_incentivos.frame,
        "DF Regalias Incentivos",
        save_parquet=SAVE_PARQUET,
        debug_keys=DEBUG_KEYS,
        parquet_rows_limit=1000000,
    )
