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
from dagster_kielsa_gf.assets.control_incentivos import esquemas


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
            regalias_incentivo=self.procesar_regalias(dataframes),
            detalle_incentivo=self.procesar_ventas(dataframes),
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
        sv = esquemas.VentasSchema
        svv = esquemas.VendedorSchema
        dfm_ventas = dataframes.ventas
        df_ventas = dfm_ventas.frame
        df_vendedores = dataframes.vendedores.frame

        # Distribuir Asegurados (al final es por articulo, asegurados es por factura)
        df_ventas = df_ventas.with_columns(
            (
                sv.TipoCliente_Nombre.expr.str.contains("(?i).*ASEGURAD.*")
                | sv.TipoPlan_Nombre.expr.str.contains("(?i).*ASEGURAD.*")
            ).alias("Es_Factura_Asegurada"),
            pl.concat_str(sv.EmpSucDocCajFac_Id, sv.Articulo_Id, separator="-").alias(
                "EmpSucDocCajFacArt_Id"
            ),
        )

        # Calcular Ctd_Posiciones por EmpSucDocCajFac_Id
        df_ctd_posiciones = df_ventas.group_by(sv.EmpSucDocCajFac_Id).agg(
            pl.col("EmpSucDocCajFacArt_Id").n_unique().alias("Ctd_Posiciones"),
            pl.col("Regla_Nombre").first().alias("Regla_Nombre"),
        )

        # Unir de nuevo para tener Ctd_Posiciones y Regla_Nombre en df_ventas
        df_ventas = df_ventas.join(
            df_ctd_posiciones,
            on=sv.EmpSucDocCajFac_Id,
            how="left",
        )

        df_ventas = df_ventas.with_columns(
            (
                pl.when(pl.col("Es_Factura_Asegurada") == 1)
                .then(1.0 / pl.col("Ctd_Posiciones"))
                .otherwise(0.0)
            ).alias("Ctd_Asegurados_Distribuido")
        )

        # Traer Rol
        df_ventas = df_ventas.join(
            df_vendedores.select(
                svv.Emp_Id,
                svv.Vendedor_Id,
                svv.Sucursal_Id_Asignado.expr.alias("Suc_Id"),
                svv.Rol_Nombre,
            ),
            on=[sv.Emp_Id, sv.Vendedor_Id, sv.Suc_Id],
        )

        # Ventas propias
        df_ventas = (
            df_ventas.group_by(
                [
                    sv.Emp_Id,
                    sv.Suc_Id,
                    sv.Vendedor_Id,
                    sv.Articulo_Id,
                    sv.Fecha_Id,
                    svv.Rol_Nombre,
                    sv.CanalVenta_Id,
                ]
            )
            .agg(
                sv.Cantidad_Padre.expr.sum().alias("Cantidad_Padre"),
                sv.Valor_Neto.expr.sum().alias("Valor_Neto"),
                pl.col("Ctd_Asegurados_Distribuido")
                .sum()
                .alias("Ctd_Asegurados_Distribuido"),
                pl.col("Regla_Nombre").first().alias("Regla_Nombre"),
            )
            .with_columns(
                pl.lit("Propio").alias("Rol_Descendiente"),
            )
        )

        df_ventas_vendedores = df_ventas.with_columns(
            pl.col("Cantidad_Padre").fill_null(0),
            pl.col("Valor_Neto").fill_null(0),
            pl.col("Ctd_Asegurados_Distribuido").fill_null(0),
            pl.concat_str(
                [
                    pl.col("Rol_Descendiente"),
                ],
                separator="-",
            ).alias("Detalle_Id"),
            pl.lit(0).cast(pl.Int32).alias("venta_aplica_incentivo"),
            pl.lit(None).cast(pl.Float64).alias("venta_valor_incentivo_unitario"),
            pl.lit(None).cast(pl.Float64).alias("venta_valor_incentivo_total"),
        )

        return dfm_ventas.with_frame(
            df_ventas_vendedores,
            primary_keys=(
                "Fecha_Id",
                "Emp_Id",
                "Suc_Id",
                "Vendedor_Id",
                "Articulo_Id",
                "CanalVenta_Id",
                "Detalle_Id",
            ),
        ).validate_primary_keys()

    @property
    def es_regla_por_defecto(self) -> bool:
        return True
