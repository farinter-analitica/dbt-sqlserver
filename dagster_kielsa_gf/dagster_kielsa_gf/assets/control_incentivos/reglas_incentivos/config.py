from typing import Literal, get_args
from typing import TypedDict, NotRequired
import polars as pl

# Define los IDs válidos en el Literal
EmpresaID = Literal[1, 2, 3]

# Deriva la lista de IDs válidos a partir del Literal
EMPRESAS_ID = frozenset(get_args(EmpresaID))


# Diccionario de entrada: dataframes disponibles para las reglas
class DataFramesInputDict(TypedDict):
    regalias: pl.LazyFrame
    calendario: pl.LazyFrame
    articulos: pl.LazyFrame
    # vendedores: pl.LazyFrame
    # sucursales: pl.LazyFrame
    # Agrega aquí otros dataframes relevantes según tu dominio


# Diccionario de salida: resultados procesados por la regla
class DataFramesOutputDict(TypedDict):
    regalias_incentivo: pl.LazyFrame
    resumen: NotRequired[pl.LazyFrame]
    # Puedes agregar más salidas según sea necesario
