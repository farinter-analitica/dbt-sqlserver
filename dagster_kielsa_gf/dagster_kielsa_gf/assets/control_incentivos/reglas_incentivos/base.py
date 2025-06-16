from abc import ABC, abstractmethod
import datetime as dt
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
            if "Regla_Nombre" in lfk.frame.columns:
                lf = lf.filter(pl.col("Regla_Nombre") == self.regla_nombre)
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
