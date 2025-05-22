from typing import Literal, get_args, Optional
from dataclasses import dataclass
import polars as pl

# Define los IDs válidos en el Literal
EmpresaID = Literal[1, 2, 3, 4, 5]

# Deriva la lista de IDs válidos a partir del Literal
EMPRESAS_ID: frozenset[EmpresaID] = frozenset(get_args(EmpresaID))


@dataclass
class DataFrameWithPK:
    frame: pl.LazyFrame
    primary_keys: tuple[str, ...]

    def __post_init__(self):
        # Ensure all primary_keys exist in the LazyFrame schema
        frame_columns = set(self.frame.columns)
        missing = [k for k in self.primary_keys if k not in frame_columns]
        if missing:
            raise ValueError(f"Primary keys not found in frame columns: {missing}")

        # Ensure primary_keys are unique
        if len(self.primary_keys) != len(set(self.primary_keys)):
            raise ValueError("Primary keys must be unique")


# Entrada: dataframes disponibles para las reglas
@dataclass
class DataFramesInput:
    regalias: DataFrameWithPK
    calendario: DataFrameWithPK
    articulos: DataFrameWithPK
    # vendedores: pl.LazyFrame
    # sucursales: pl.LazyFrame
    # Agrega aquí otros dataframes relevantes según tu dominio


# Salida: resultados procesados por la regla
@dataclass
class DataFramesOutput:
    regalias_incentivo: DataFrameWithPK
    resumen: Optional[DataFrameWithPK] = None
    # Puedes agregar más salidas según sea necesario
