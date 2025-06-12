from collections.abc import Iterator
from typing import Literal, get_args, Optional
from dataclasses import dataclass, fields
import polars as pl

# Define los IDs válidos en el Literal
EmpresaID = Literal[1, 2, 3, 4, 5]

# Deriva la lista de IDs válidos a partir del Literal
EMPRESAS_ID: frozenset[EmpresaID] = frozenset(get_args(EmpresaID))


@dataclass
class LazyFrameWithMeta:
    frame: pl.LazyFrame
    primary_keys: tuple[str, ...]
    date_name: Optional[str]
    emp_id_name: Optional[str]

    def __post_init__(self):
        # Ensure all primary_keys exist in the LazyFrame schema
        frame_columns = set(self.frame.columns)
        missing = [k for k in self.primary_keys if k not in frame_columns]
        if missing:
            raise ValueError(f"Primary keys not found in frame columns: {missing}")

        # Ensure primary_keys are unique
        if len(self.primary_keys) != len(set(self.primary_keys)):
            raise ValueError("Primary keys must be unique")

        if self.date_name and self.date_name not in frame_columns:
            raise ValueError("El campo fecha asignado debe existir")

        if self.emp_id_name and self.emp_id_name not in frame_columns:
            raise ValueError("El campo empresa asignado debe existir")

    def with_frame(self, lf: pl.LazyFrame) -> "LazyFrameWithMeta":
        """
        Crea una nueva instancia de LazyFrameWithMeta usando el nuevo LazyFrame `lf`
        pero conservando primary_keys, date_name y emp_id_name de esta instancia.
        """
        return LazyFrameWithMeta(
            frame=lf,
            primary_keys=self.primary_keys,
            date_name=self.date_name,
            emp_id_name=self.emp_id_name,
        )


# Entrada: dataframes disponibles para las reglas
@dataclass
class DataFramesInput:
    regalias: LazyFrameWithMeta
    calendario: LazyFrameWithMeta
    articulos: LazyFrameWithMeta
    # ventas: DataFrameWithPK
    vendedores: LazyFrameWithMeta
    # sucursales: pl.LazyFrame
    # Agrega aquí otros dataframes relevantes según tu dominio

    def __iter__(self) -> Iterator[tuple[str, LazyFrameWithMeta]]:
        """
        Itera sobre todos los atributos tipo DataFrameWithPK del dataclass,
        devolviendo tuplas (nombre_atributo, valor).
        """
        for fld in fields(self):
            value = getattr(self, fld.name)
            if isinstance(value, LazyFrameWithMeta):
                yield fld.name, value

    def items(self) -> Iterator[tuple[str, LazyFrameWithMeta]]:
        """
        Alias estilo dict.items() para iterar sobre los dataframes.
        """
        return iter(self)

    def keys(self) -> Iterator[str]:
        """
        Itera sobre los nombres de los atributos tipo DataFrameWithPK.
        """
        for name, _ in self:
            yield name

    def values(self) -> Iterator[LazyFrameWithMeta]:
        """
        Itera sobre los valores (DataFrameWithPK) del dataclass.
        """
        for _, df in self:
            yield df


# Salida: resultados procesados por la regla
@dataclass
class DataFramesOutput:
    regalias_incentivo: LazyFrameWithMeta
    resumen: Optional[LazyFrameWithMeta] = None
    # Puedes agregar más salidas según sea necesario
