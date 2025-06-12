import polars as pl
import datetime as dt
from dagster_kielsa_gf.assets.control_incentivos.reglas_incentivos.base import (
    BaseReglaIncentivo,
)
from dagster_kielsa_gf.assets.control_incentivos.config import (
    EmpresaID,
    DataFramesInput,
    DataFramesOutput,
    LazyFrameWithMeta,
)


class ReglaIncentivoSV2025(BaseReglaIncentivo):
    @property
    def EMP_ID(self) -> frozenset[EmpresaID]:
        return frozenset([5])

    @property
    def VALID_FROM(self) -> dt.date:
        return dt.date(2020, 1, 1)

    @property
    def VALID_UNTIL(self) -> dt.date:
        return dt.date(2025, 5, 31)

    def filtrar(self, dfs_in: DataFramesInput) -> DataFramesInput:
        return super().filtrar(dfs_in)

    def procesar(self, dataframes: DataFramesInput) -> DataFramesOutput:
        dataframes = self.filtrar(dataframes)
        dfk_regalias_incentivo = self.procesar_regalias(dataframes)
        resultado = DataFramesOutput(regalias_incentivo=dfk_regalias_incentivo)
        return resultado

    def procesar_regalias(self, dataframes: DataFramesInput) -> LazyFrameWithMeta:
        """Tambien conocidos como canjes"""
        df_regalias = dataframes.regalias.frame
        df_articulos = dataframes.articulos.frame
        if dataframes.vendedores is None:
            raise ValueError("No se proporcionaron vendedores, se necesita el rol")
        df_vendedores = dataframes.vendedores.frame
        incentivo_por_defecto = self.config.get("incentivo_por_defecto", 0.75)
        map_incentivo_casa = self.config.get(
            "map_incentivo_casa",
            {  # Por el momento no hay especificos
            },
        )

        map_part_rol = self.config.get(
            "map_part_rol",
            {
                "Jefe de Farmacia": 0.2050,
                "Sub Jefe de Farmacia": 0.10,
                "Dependiente-Pre venta": 0.64,
                "Cajero - Vendedor": 0.64,  # Duplicado Dependiente por estandar de HN
                "Cajero - Lider": 0.64,  # Duplicado Dependiente por estandar de HN
                "Supervisor de Zona": 0.045,
                "Gerente de Ventas": 0.01,
            },
        )

        df_map_incentivo_casa = pl.LazyFrame(
            {
                "Casa_Id": list(map_incentivo_casa.keys()),
                "incentivo_casa": list(map_incentivo_casa.values()),
            },
            schema={"Casa_Id": pl.Int32, "incentivo_casa": pl.Float64},
        )

        df_part_rol = pl.LazyFrame(
            {
                "Rol": list(map_part_rol.keys()),
                "part_rol": list(map_part_rol.values()),
            },
            schema={"Rol": pl.String, "part_rol": pl.Float64},
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
            .join(
                df_vendedores.select(["Emp_Id", "Vendedor_Id", "Rol"]),
                on=["Emp_Id", "Vendedor_Id"],
                how="inner",
            )
            .join(
                df_part_rol,
                on="Rol",
                how="left",
            )
            .with_columns(regalia_aplica_incentivo=(pl.lit(True)))
            .join(df_map_incentivo_casa, on="Casa_Id", how="left")
            .with_columns(
                regalia_valor_incentivo_unitario=pl.when(
                    pl.col("regalia_aplica_incentivo")
                )
                .then(
                    pl.coalesce(pl.col("incentivo_casa"), pl.lit(incentivo_por_defecto))
                    * pl.coalesce(pl.col("part_rol"), pl.lit(0.0))
                )
                .otherwise(pl.lit(0.0))
            )
            .with_columns(
                regalia_valor_incentivo_total=pl.col("Cantidad_Padre")
                * pl.col("regalia_valor_incentivo_unitario")
            )
        )

        return dataframes.regalias.with_frame(
            df_result,
        )
