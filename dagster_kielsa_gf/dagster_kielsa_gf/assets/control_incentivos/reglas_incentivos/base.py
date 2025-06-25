from abc import ABC, abstractmethod
import datetime as dt
import hashlib
from dagster_kielsa_gf.assets.control_incentivos import esquemas
from dagster_kielsa_gf.assets.control_incentivos.config import (
    LazyFrameWithMeta,
    EmpresaID,
    DataFramesInput,
    DataFramesOutput,
)
import polars as pl


class BaseReglaIncentivo(ABC):
    @property
    @abstractmethod
    def EMP_ID(self) -> frozenset[EmpresaID]:
        """IDs de empresa a los que aplica la regla."""
        pass

    @property
    @abstractmethod
    def VALID_FROM(self) -> dt.date:
        """Fecha desde la que aplica la regla. Usar dt.date(1900, 1, 1) para desde siempre."""
        pass

    @property
    @abstractmethod
    def VALID_UNTIL(self) -> dt.date:
        """Fecha hasta la que aplica la regla. Usar dt.date(9999, 12, 31) para hasta siempre."""
        pass

    @property
    def regla_nombre(self) -> str:
        return type(self).__name__

    @property
    def regla_hash(self) -> int:
        # SQL Server admite enteros en el rango de -2,147,483,648 a 2,147,483,647 (entero con signo de 4 bytes)
        # Usaremos un entero con signo de 4 bytes y nos aseguraremos de que el valor esté en este rango
        hash_bytes = hashlib.sha256(self.regla_nombre.encode()).digest()[:4]
        value = int.from_bytes(hash_bytes, "big", signed=True)
        # Aseguramos que el valor esté en el rango de int de SQL Server
        if value < -2_147_483_648 or value > 2_147_483_647:
            value = (
                (value + 2_147_483_648) % (2_147_483_647 - (-2_147_483_648) + 1)
            ) + (-2_147_483_648)
        return value

    def __init__(self, config: dict | None = None):
        self.config = config or {}

    def aplica_a(self, emp_id: EmpresaID, fecha: dt.date) -> bool:
        """
        Determina si una regla de incentivo aplica para una empresa específica en una fecha dada.

        Args:
            emp_id (EmpresaID): El identificador de la empresa a evaluar.
            fecha (dt.date): La fecha para la cual se verifica la aplicabilidad de la regla.

        Returns:
            bool: True si la regla aplica para la empresa y fecha especificadas, False en caso contrario.
        """
        return emp_id in self.EMP_ID and self.VALID_FROM <= fecha <= self.VALID_UNTIL

    @abstractmethod
    def procesar(self, dataframes: DataFramesInput) -> DataFramesOutput:
        """
        Aplica la lógica de incentivo usando los dataframes relevantes.
        Cada regla puede usar solo los dataframes que necesita del diccionario.
        """
        pass

    @abstractmethod
    def filtrar(self, dfs_in: DataFramesInput) -> DataFramesInput:
        """
        Se deben filtrar los dataframes necesarios.
        Las reglas no deben devolver solapamientos en resultados.

        Reemplazar o añadir a este paso de acuerdo a necesidad.
        Tomar en cuenta el cuidado con referencias y clonación.
        """
        nuevos = {}
        for name, lfk in dfs_in.items():
            lf = lfk.frame
            if "Regla_Hash" in lfk.frame.limit(1).collect(engine="streaming").columns:
                lf = lf.filter(pl.col("Regla_Hash") == self.regla_hash)
            # if lfk.emp_id_name:
            #     lf = lf.filter(pl.col(lfk.emp_id_name).is_in(self.emp_ids))
            # if lfk.date_name:
            #     lf = lf.filter(
            #         pl.col(lfk.date_name).is_between(self.fecha_desde, self.fecha_hasta)
            #     )
            # clonamos con la nueva LazyFrame
            nuevos[name] = lfk.with_frame(lf)

        return DataFramesInput(**nuevos)

    @property
    def es_regla_por_defecto(self) -> bool:
        """
        Indica si la regla es una regla por defecto.
        """
        return False

    @property
    def emp_ids(self) -> frozenset[EmpresaID]:
        emp_id = getattr(self, "EMP_ID", None)
        if emp_id is None:
            raise ValueError(
                f"EMP_ID no puede ser None en ninguna regla, incumple: {self.regla_nombre}"
            )
        if isinstance(emp_id, (list, set, tuple)):
            return frozenset(emp_id)
        return emp_id

    @property
    def fecha_desde(self) -> dt.date:
        fecha = getattr(self, "VALID_FROM", None)
        if fecha is None:
            raise ValueError(
                f"VALID_FROM no puede ser None en ninguna regla, incumple: {self.regla_nombre}"
            )
        return fecha

    @property
    def fecha_hasta(self) -> dt.date:
        fecha = getattr(self, "VALID_UNTIL", None)
        if fecha is None:
            raise ValueError(
                f"VALID_UNTIL no puede ser None en ninguna regla, incumple: {self.regla_nombre}"
            )
        return fecha

    @property
    def rango_fechas(self) -> tuple[dt.date, dt.date]:
        return (self.fecha_desde, self.fecha_hasta)

    def procesar_regalias(self, dataframes: DataFramesInput) -> LazyFrameWithMeta:
        raise NotImplementedError("La regla no implementa procesar_regalias")

    def procesar_ventas(self, dataframes: DataFramesInput) -> LazyFrameWithMeta:
        raise NotImplementedError("La regla no implementa procesar_ventas")

    def procesar_ventas_base(self, dataframes: DataFramesInput) -> LazyFrameWithMeta:
        sv = esquemas.VentasSchema
        dfm_ventas = dataframes.ventas
        df_ventas = dfm_ventas.frame
        dfm_usuario_sucursal = dataframes.usuarios_sucursales
        sm = esquemas.UsuarioSucursalSchema
        df_usuario_sucursal = dfm_usuario_sucursal.frame

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

        # Agrupar
        # Ventas propias
        df_ventas = df_ventas.group_by(
            [
                sv.Fecha_Id,
                sv.EmpSucDocCajFac_Id,
                sv.Articulo_Id,
            ]
        ).agg(
            sv.Cantidad_Padre.expr.sum(),
            sv.Valor_Neto.expr.sum(),
            sv.Valor_Utilidad.expr.sum(),
            sv.Valor_Descuento_Proveedor.expr.sum(),
            pl.col("Regla_Hash").first(),
            sv.Emp_Id.expr.first(),
            sv.Suc_Id.expr.first(),
            sv.CanalVenta_Id.expr.first(),
            sv.Vendedor_Id.expr.first(),
            sv.Usuario_Id.expr.first(),
            pl.col("Es_Factura_Asegurada").first(),
        )

        # Traer Rol
        df_ventas = (
            df_ventas.join(
                df_usuario_sucursal.select(
                    sm.Emp_Id,
                    sm.Usuario_Id,
                    sm.Suc_Id,
                    sm.Rol_Id,
                ),
                on=[sv.Emp_Id, sv.Usuario_Id, sv.Suc_Id],
                how="left",
            )
            .with_columns(
                pl.col("Rol_Id").fill_null(0),
            )
            .drop(pl.selectors.ends_with("_right"))
        )

        df_ventas_vendedores = df_ventas.with_columns(
            pl.col("Cantidad_Padre").fill_null(0),
            pl.col("Valor_Neto").fill_null(0),
            pl.lit(1).cast(pl.Int32).alias("venta_aplica_incentivo"),
            pl.lit(None).cast(pl.Float64).alias("venta_valor_incentivo_unitario"),
            pl.lit(None).cast(pl.Float64).alias("venta_valor_incentivo_total"),
        )

        return dfm_ventas.with_frame(
            df_ventas_vendedores,
            primary_keys=(sv.Fecha_Id, sv.EmpSucDocCajFac_Id, sv.Articulo_Id),
        )
