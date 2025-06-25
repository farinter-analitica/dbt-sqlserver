from typing import TypedDict
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

    def jerarquia_roles(self, df_roles: LazyFrameWithMeta) -> LazyFrameWithMeta:
        emp_id = list(self.emp_ids)[-1]
        Rol = TypedDict(
            "Rol",
            Rol_Nombre=str,
            Rol_Responsable=str | None,
            Emp_Id=int,
        )
        roles_dict: list[Rol] = [
            {
                "Rol_Nombre": "Gerente de Ventas",
                "Rol_Responsable": None,
                "Emp_Id": emp_id,
            },
            {
                "Rol_Nombre": "Supervisor de Zona",
                "Rol_Responsable": "Gerente de Ventas",
                "Emp_Id": emp_id,
            },
            {
                "Rol_Nombre": "Jefe de Farmacia",
                "Rol_Responsable": "Supervisor de Zona",
                "Emp_Id": emp_id,
            },
            {
                "Rol_Nombre": "Sub Jefe de Farmacia",
                "Rol_Responsable": "Jefe de Farmacia",
                "Emp_Id": emp_id,
            },
            {
                "Rol_Nombre": "Cajero",
                "Rol_Responsable": "Sub Jefe de Farmacia",
                "Emp_Id": emp_id,
            },
            {
                "Rol_Nombre": "Dependiente-Pre venta",
                "Rol_Responsable": "Sub Jefe de Farmacia",
                "Emp_Id": emp_id,
            },
        ]
        roles_jerarquia: pl.DataFrame = pl.DataFrame(roles_dict)
        relaciones_directas = roles_jerarquia.filter(
            pl.col("Rol_Responsable").is_not_null()
        ).select(
            pl.col("Rol_Responsable").alias("Rol_Ancestro"),
            pl.col("Rol_Nombre").alias("Rol_Descendiente"),
            pl.col("Emp_Id"),
        )

        # Relaciones propias (cada rol es su propio descendiente)
        relaciones_propias = roles_jerarquia.select(
            pl.col("Rol_Nombre").alias("Rol_Ancestro"),
            pl.lit("Propio").alias("Rol_Descendiente"),
            pl.col("Emp_Id"),
        )

        todas_las_relaciones = [relaciones_directas, relaciones_propias]

        frontera = relaciones_directas

        while True:
            # Unimos la frontera actual con las relaciones directas para encontrar el siguiente nivel.
            # Ejemplo: (Ancestro: A, Descendiente: B) une con (Ancestro: B, Descendiente: C)
            # El resultado es una nueva relación (Ancestro: A, Descendiente: C)
            nuevos_descendientes = frontera.join(
                relaciones_directas,
                left_on=["Rol_Descendiente", "Emp_Id"],
                right_on=["Rol_Ancestro", "Emp_Id"],
            ).select(
                pl.col("Rol_Ancestro"),  # El ancestro original de la frontera
                pl.col("Rol_Descendiente_right").alias("Rol_Descendiente"),
                pl.col("Emp_Id"),
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
        df_jerarquia_roles = (
            pl.concat(todas_las_relaciones)
            .rename(
                {
                    "Rol_Ancestro": "Rol_Responsable",
                    "Rol_Descendiente": "Rol_Descendiente",
                }
            )
            .lazy()
        )

        sr = esquemas.RolSchema
        df_jerarquia_roles = (
            df_jerarquia_roles.join(
                df_roles.frame.select(
                    sr.Emp_Id,
                    sr.Rol_Nombre.expr.alias("Rol_Responsable"),
                    sr.Rol_Id.expr.alias("Rol_Id_Responsable"),
                ),
                on=[sr.Emp_Id, "Rol_Responsable"],
                how="left",
            )
            .join(
                df_roles.frame.select(
                    sr.Emp_Id,
                    sr.Rol_Nombre.expr.alias("Rol_Descendiente"),
                    sr.Rol_Id.expr.alias("Rol_Id_Descendiente"),
                ),
                on=[sr.Emp_Id, "Rol_Descendiente"],
                how="left",
            )
            .with_columns(
                # Llenar el propio
                pl.col("Rol_Id_Descendiente").fill_null(pl.col("Rol_Id_Responsable")),
            )
        )

        return LazyFrameWithMeta(
            df_jerarquia_roles,
            primary_keys=("Rol_Responsable", "Rol_Descendiente"),
            validar_llave_primaria=True,
        )

    def crear_matriz_jerarquia_vendedores(
        self, dfmu: LazyFrameWithMeta, dfmr: LazyFrameWithMeta
    ) -> LazyFrameWithMeta:
        us = esquemas.UsuarioSucursalSchema
        df_us = dfmu.frame.filter(
            (pl.col("Rol_Id").is_not_null()) & (pl.col("Rol_Id") != 0)
        )
        jerarquia_roles = self.jerarquia_roles(dfmr).frame

        df_us = (
            df_us.select(
                us.Emp_Id,
                us.Suc_Id,
                us.Usuario_Id,
                us.Rol_Nombre,
                us.Rol_Id,
                us.Rol_Jerarquia,
                us.Vendedor_Id,
            )
            .filter(us.Emp_Id.expr.is_in(self.emp_ids))
            .with_row_index()
            .join(
                jerarquia_roles,
                left_on=["Emp_Id", "Rol_Id"],
                right_on=["Emp_Id", "Rol_Id_Responsable"],
                how="inner",
            )
        )

        return dfmu.with_frame(
            df_us,
            primary_keys=(us.Emp_Id, us.Suc_Id, us.Usuario_Id, "Rol_Id_Descendiente"),
            # validar_llave_primaria=True,
        )

    def procesar_ventas(self, dataframes: DataFramesInput) -> LazyFrameWithMeta:
        dfm_ventas = self.procesar_ventas_base(dataframes)
        df_ventas = dfm_ventas.frame

        # Reproceso por roles jerarquicos
        sv = esquemas.VentasSchema
        svv = esquemas.VendedorSchema
        dfm_usuario_sucursal = dataframes.usuarios_sucursales
        dfm_matriz_jerarquia = self.crear_matriz_jerarquia_vendedores(
            dfm_usuario_sucursal, dataframes.roles
        )
        df_matriz_jerarquia = dfm_matriz_jerarquia.frame
        columnas = {
            "Fecha_Id",
            "EmpSucDocCajFac_Id",
            "CanalVenta_Id",
            "Emp_Id",
            "Suc_Id",
            "Vendedor_Id",
            "Usuario_Id",
            "Rol_Id",
            "Rol_Id_Descendiente",
            "Articulo_Id",
            "Cantidad_Padre",
            "Valor_Neto",
            "Regla_Hash",
        }
        df_matriz_jerarquia = df_matriz_jerarquia.select(
            "Emp_Id",
            "Suc_Id",
            "Vendedor_Id",
            "Usuario_Id",
            "Rol_Id",
            "Rol_Id_Descendiente",
        )
        df_ventas_propias = (
            df_matriz_jerarquia.filter(
                pl.col("Rol_Id") == pl.col("Rol_Id_Descendiente")
            )
            .join(
                df_ventas,
                left_on=["Emp_Id", "Suc_Id", "Usuario_Id"],
                right_on=[sv.Emp_Id, sv.Suc_Id, sv.Usuario_Id],
                how="inner",
            )
            .drop(pl.selectors.ends_with("_right"))
        ).select(columnas)

        # Ventas descendientes para el mismo vendedor
        df_ventas_rol = df_ventas.drop([sv.Usuario_Id, sv.Vendedor_Id]).with_columns(
            svv.Rol_Id.expr.alias("Rol_Id_Descendiente")
        )

        df_ventas_descendientes = (
            df_matriz_jerarquia.filter(
                pl.col("Rol_Id") != pl.col("Rol_Id_Descendiente")
            )
            .join(
                df_ventas_rol,
                left_on=["Emp_Id", "Suc_Id", "Rol_Id_Descendiente"],
                right_on=[sv.Emp_Id, sv.Suc_Id, "Rol_Id_Descendiente"],
                how="inner",
            )
            .drop(pl.selectors.ends_with("_right"))
        ).select(columnas)

        # Ventas sin rol jerarquico
        df_ventas_sin_rol = (
            df_ventas.filter(pl.col("Rol_Id") == 0)
            .with_columns(pl.lit(0).alias("Rol_Id_Descendiente"))
            .select(columnas)
        )

        # Fin reproceso por roles
        # Ventas totales propios + descendientes
        df_ventas_vendedores = pl.concat(
            [df_ventas_propias, df_ventas_descendientes, df_ventas_sin_rol],
        )
        df_ventas_vendedores = df_ventas_vendedores.with_columns(
            pl.col("Cantidad_Padre").fill_null(0),
            pl.col("Valor_Neto").fill_null(0),
            pl.lit(1).cast(pl.Int32).alias("venta_aplica_incentivo"),
            pl.lit(None).cast(pl.Float64).alias("venta_valor_incentivo_unitario"),
            pl.lit(None).cast(pl.Float64).alias("venta_valor_incentivo_total"),
        )

        return dfm_ventas.with_frame(
            df_ventas_vendedores,
        )
