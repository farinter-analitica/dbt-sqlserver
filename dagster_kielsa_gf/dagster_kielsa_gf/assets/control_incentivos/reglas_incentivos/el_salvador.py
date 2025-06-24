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
from dagster_kielsa_gf.assets.control_incentivos import esquemas


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
        dfk_detalle_incentivo = self.procesar_ventas(dataframes)
        resultado = DataFramesOutput(
            regalias_incentivo=dfk_regalias_incentivo,
            detalle_incentivo=dfk_detalle_incentivo,
        )
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
            df_result.drop(pl.selectors.ends_with("_Right")),
        )

    def jerarquia_roles(self) -> LazyFrameWithMeta:
        roles = pl.DataFrame(
            [
                {"Rol_Nombre": "Gerente de Ventas", "Rol_Responsable": None},
                {
                    "Rol_Nombre": "Supervisor de Zona",
                    "Rol_Responsable": "Gerente de Ventas",
                },
                {
                    "Rol_Nombre": "Jefe de Farmacia",
                    "Rol_Responsable": "Supervisor de Zona",
                },
                {
                    "Rol_Nombre": "Sub Jefe de Farmacia",
                    "Rol_Responsable": "Jefe de Farmacia",
                },
                {"Rol_Nombre": "Cajero", "Rol_Responsable": "Sub Jefe de Farmacia"},
                {
                    "Rol_Nombre": "Dependiente-Pre venta",
                    "Rol_Responsable": "Sub Jefe de Farmacia",
                },
            ]
        )
        relaciones_directas = roles.filter(
            pl.col("Rol_Responsable").is_not_null()
        ).select(
            pl.col("Rol_Responsable").alias("Rol_Ancestro"),
            pl.col("Rol_Nombre").alias("Rol_Descendiente"),
        )

        # Relaciones propias (cada rol es su propio descendiente)
        relaciones_propias = roles.select(
            pl.col("Rol_Nombre").alias("Rol_Ancestro"),
            pl.lit("Propio").alias("Rol_Descendiente"),
        )

        todas_las_relaciones = [relaciones_directas, relaciones_propias]

        frontera = relaciones_directas

        while True:
            # Unimos la frontera actual con las relaciones directas para encontrar el siguiente nivel.
            # Ejemplo: (Ancestro: A, Descendiente: B) une con (Ancestro: B, Descendiente: C)
            # El resultado es una nueva relación (Ancestro: A, Descendiente: C)
            nuevos_descendientes = frontera.join(
                relaciones_directas,
                left_on="Rol_Descendiente",
                right_on="Rol_Ancestro",
            ).select(
                pl.col("Rol_Ancestro"),  # El ancestro original de la frontera
                pl.col("Rol_Descendiente_right").alias("Rol_Descendiente"),
            )

            # Si el DataFrame resultante está vacío, hemos recorrido toda la jerarquía.
            if nuevos_descendientes.height == 0:
                break

            # Agregamos las nuevas relaciones encontradas a nuestra lista.
            todas_las_relaciones.append(nuevos_descendientes)

            # La nueva frontera para la siguiente iteración son los descendientes que acabamos de encontrar.
            frontera = nuevos_descendientes

        # 3. Concatenamos todos los niveles de relaciones en un solo DataFrame.
        # Esto nos da el mapeo completo de cada ancestro con todos sus descendientes.
        df_jerarquia_roles = pl.concat(todas_las_relaciones).rename(
            {"Rol_Ancestro": "Rol_Responsable", "Rol_Descendiente": "Rol_Descendiente"}
        )

        return LazyFrameWithMeta(
            df_jerarquia_roles.lazy(),
            primary_keys=("Rol_Responsable", "Rol_Descendiente"),
            validar_llave_primaria=True,
        )

    def crear_matriz_jerarquia_vendedores(
        self, dfm: LazyFrameWithMeta
    ) -> LazyFrameWithMeta:
        us = esquemas.UsuarioSucursalSchema
        df_us = dfm.frame
        jerarquia_roles = self.jerarquia_roles().frame

        df_us = (
            df_us.select(
                us.Emp_Id,
                us.Suc_Id,
                us.Usuario_Id,
                us.Rol_Nombre,
                us.Rol_Jerarquia,
                us.Vendedor_Id,
            )
            .filter(
                us.Emp_Id.expr.is_in(self.emp_ids) & us.Vendedor_Id.expr.is_not_null()
            )
            .with_row_index()
            .join(
                jerarquia_roles,
                left_on="Rol_Nombre",
                right_on="Rol_Responsable",
            )
        )

        return dfm.with_frame(
            df_us,
            primary_keys=(us.Emp_Id, us.Suc_Id, us.Vendedor_Id, "Rol_Descendiente"),
            validar_llave_primaria=True,
        )

    def procesar_ventas(self, dataframes: DataFramesInput) -> LazyFrameWithMeta:
        sv = esquemas.VentasSchema
        svv = esquemas.VendedorSchema
        dfm_ventas = dataframes.ventas
        df_ventas = dfm_ventas.frame
        df_vendedores = dataframes.vendedores.frame
        dfm_matriz_jerarquia = self.crear_matriz_jerarquia_vendedores(
            dataframes.usuarios_sucursales
        )
        df_matriz_jerarquia = dfm_matriz_jerarquia.frame

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
        df_ventas = df_ventas.group_by(
            [
                sv.Emp_Id,
                sv.Suc_Id,
                sv.Vendedor_Id,
                sv.Articulo_Id,
                sv.Fecha_Id,
                svv.Rol_Nombre,
                sv.CanalVenta_Id,
            ]
        ).agg(
            sv.Cantidad_Padre.expr.sum().alias("Cantidad_Padre"),
            sv.Valor_Neto.expr.sum().alias("Valor_Neto"),
            pl.col("Ctd_Asegurados_Distribuido")
            .sum()
            .alias("Ctd_Asegurados_Distribuido"),
            pl.col("Regla_Nombre").first().alias("Regla_Nombre"),
        )

        df_ventas_propias = (
            df_matriz_jerarquia.filter(pl.col("Rol_Descendiente") == "Propio")
            .join(
                df_ventas,
                left_on=["Emp_Id", "Suc_Id", "Vendedor_Id"],
                right_on=[sv.Emp_Id, sv.Suc_Id, sv.Vendedor_Id],
                how="inner",
            )
            .drop(pl.selectors.ends_with("_right"))
        )

        # Ventas descendientes
        df_ventas_rol = df_ventas.group_by(
            [
                sv.Emp_Id,
                sv.Suc_Id,
                sv.Articulo_Id,
                sv.Fecha_Id,
                svv.Rol_Nombre,
                sv.CanalVenta_Id,
            ]
        ).agg(
            sv.Cantidad_Padre.expr.sum().alias("Cantidad_Padre"),
            sv.Valor_Neto.expr.sum().alias("Valor_Neto"),
            pl.col("Ctd_Asegurados_Distribuido")
            .sum()
            .alias("Ctd_Asegurados_Distribuido"),
            pl.col("Regla_Nombre").first().alias("Regla_Nombre"),
        )

        df_ventas_descendientes = (
            df_matriz_jerarquia.filter(pl.col("Rol_Descendiente") != "Propio")
            .join(
                df_ventas_rol,
                left_on=["Emp_Id", "Suc_Id", "Rol_Descendiente"],
                right_on=[sv.Emp_Id, sv.Suc_Id, "Rol_Nombre"],
                how="inner",
            )
            .drop(pl.selectors.ends_with("_right"))
        )

        # Ventas totales propios + descendientes
        df_ventas_vendedores = pl.concat([df_ventas_propias, df_ventas_descendientes])
        df_ventas_vendedores = df_ventas_vendedores.with_columns(
            pl.col("Cantidad_Padre").fill_null(0),
            pl.col("Valor_Neto").fill_null(0),
            pl.col("Ctd_Asegurados_Distribuido").fill_null(0),
            pl.concat_str(
                [
                    pl.col("Rol_Descendiente"),
                ],
                separator="-",
            ).alias("Detalle_Id"),
            pl.lit(1).cast(pl.Int32).alias("venta_aplica_incentivo"),
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
        )
