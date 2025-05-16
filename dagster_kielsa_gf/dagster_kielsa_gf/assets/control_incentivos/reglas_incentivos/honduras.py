import polars as pl
import datetime as dt
from dagster_kielsa_gf.assets.control_incentivos.reglas_incentivos.base import (
    BaseReglaIncentivo,
)
from dagster_kielsa_gf.assets.control_incentivos.reglas_incentivos.config import (
    EmpresaID,
    DataFramesInputDict,
    DataFramesOutputDict,
)


class ReglaIncentivoHN2025(BaseReglaIncentivo):
    @property
    def EMP_ID(self) -> frozenset[EmpresaID]:
        return frozenset([1])

    @property
    def VALID_FROM(self) -> dt.date:
        return dt.date(2020, 1, 1)

    @property
    def VALID_UNTIL(self) -> dt.date:
        return dt.date(9999, 12, 31)

    def procesar(self, dataframes: DataFramesInputDict) -> DataFramesOutputDict:
        df_regalias = dataframes["regalias"]
        df_articulos = dataframes["articulos"]

        incentivo_por_defecto = self.config.get("incentivo_por_defecto", 15)
        map_incentivo_casa = self.config.get(
            "map_incentivo_casa",
            {
                266: 8,  # PHARMEDIC
                869: 8,  # PHARMEDIC NGM
            },
        )

        df_map_incentivo_casa = pl.LazyFrame(
            {
                "Casa_Id": list(map_incentivo_casa.keys()),
                "incentivo_casa": list(map_incentivo_casa.values()),
            }
        )

        df_result = (
            df_regalias.join(
                df_articulos.select(
                    ["Emp_Id", "Articulo_Id", "Bit_Marca_Propia", "Casa_Id"]
                ),
                left_on=["Emp_Id", "Articulo_Padre_Id"],
                right_on=["Emp_Id", "Articulo_Id"],
                how="inner",
            )
            .with_columns(canje_aplica_incentivo=(pl.col("Bit_Marca_Propia") != 1))
            .join(df_map_incentivo_casa, on="Casa_Id", how="left")
            .with_columns(
                valor_incentivo_unitario=pl.when(pl.col("canje_aplica_incentivo"))
                .then(
                    pl.coalesce(pl.col("incentivo_casa"), pl.lit(incentivo_por_defecto))
                )
                .otherwise(pl.lit(0.0))
            )
            .with_columns(
                valor_incentivo_total=pl.col("Cantidad_Padre")
                * pl.col("valor_incentivo_unitario")
            )
        )

        resultado = DataFramesOutputDict(regalias_incentivo=df_result)
        return resultado
