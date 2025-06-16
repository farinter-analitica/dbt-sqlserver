import polars as pl
import datetime as dt
from dagster_kielsa_gf.assets.control_incentivos.reglas_incentivos.base import (
    BaseReglaIncentivo,
)
from dagster_kielsa_gf.assets.control_incentivos.config import (
    EmpresaID,
    DataFramesInput,
    DataFramesOutput,
    EMPRESAS_ID,
    LazyFrameWithMeta,
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

    def filtrar(self, dfs_in: DataFramesInput) -> DataFramesInput:
        return super().filtrar(dfs_in)

    def procesar(self, dataframes: DataFramesInput) -> DataFramesOutput:
        dataframes = self.filtrar(dataframes)

        resultado = DataFramesOutput(
            regalias_incentivo=self.procesar_regalias(dataframes)
        )
        # Retorna el diccionario de salida con las columnas esperadas
        return resultado

    def procesar_regalias(self, dataframes: DataFramesInput) -> LazyFrameWithMeta:
        """Tambien conocidos como canjes"""
        # Espera los dataframes relevantes en el diccionario
        df_regalias = dataframes.regalias.frame
        # Las columnas de salida deben ser las esperadas minimas por el flujo
        df_result = df_regalias.with_columns(
            regalia_aplica_incentivo=pl.lit(0).cast(pl.Int32),
            regalia_valor_incentivo_unitario=pl.lit(None).cast(pl.Float64),
            regalia_valor_incentivo_total=pl.lit(None).cast(pl.Float64),
        )

        return dataframes.regalias.with_frame(df_result)

    def procesar_ventas(self, dataframes: DataFramesInput) -> LazyFrameWithMeta:
        # Espera los dataframes relevantes en el diccionario
        df_ventas = dataframes.regalias.frame
        # Las columnas de salida deben ser las esperadas minimas por el flujo
        df_result = df_ventas.with_columns(
            venta_aplica_incentivo=pl.lit(0).cast(pl.Int32),
            venta_valor_incentivo_unitario=pl.lit(None).cast(pl.Float64),
            venta_valor_incentivo_total=pl.lit(None).cast(pl.Float64),
        )

        return dataframes.regalias.with_frame(df_result)

    @property
    def es_regla_por_defecto(self) -> bool:
        return True
