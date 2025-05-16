import polars as pl
import datetime as dt
from dagster_kielsa_gf.assets.control_incentivos.reglas_incentivos.base import (
    BaseReglaIncentivo,
)
from dagster_kielsa_gf.assets.control_incentivos.reglas_incentivos.config import (
    EmpresaID,
    DataFramesInputDict,
    DataFramesOutputDict,
    EMPRESAS_ID,
)


class ReglaIncentivoDefault(BaseReglaIncentivo):
    @property
    def EMP_ID(self) -> frozenset[EmpresaID]:
        # Aplica a todas las empresas por defecto
        return EMPRESAS_ID

    @property
    def VALID_FROM(self) -> dt.date:
        # Desde siempre
        return dt.date(1900, 1, 1)

    @property
    def VALID_UNTIL(self) -> dt.date:
        # Hasta siempre
        return dt.date(9999, 12, 31)

    def procesar(self, dataframes: DataFramesInputDict) -> DataFramesOutputDict:
        # Espera los dataframes relevantes en el diccionario
        df_regalias = dataframes["regalias"]
        # Las columnas de salida deben ser las esperadas por el flujo
        df_result = df_regalias.with_columns(
            canje_aplica_incentivo=pl.lit(0).cast(pl.Int32),
            valor_incentivo_unitario=pl.lit(None).cast(pl.Float64),
            valor_incentivo_total=pl.lit(None).cast(pl.Float64),
        )

        resultado = DataFramesOutputDict(regalias_incentivo=df_result)
        # Retorna el diccionario de salida con las columnas esperadas
        return resultado

    @property
    def es_regla_por_defecto(self) -> bool:
        return True
