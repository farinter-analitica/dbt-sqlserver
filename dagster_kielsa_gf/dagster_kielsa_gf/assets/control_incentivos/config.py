from collections.abc import Iterator
import datetime as dt
from typing import Callable, Literal, get_args, Optional
from dataclasses import dataclass, fields
import polars as pl
from polars.schema import SchemaInitDataType

# Define los IDs válidos en el Literal
EmpresaID = Literal[1, 2, 3, 4, 5]

# Deriva la lista de IDs válidos a partir del Literal
EMPRESAS_ID: frozenset[EmpresaID] = frozenset(get_args(EmpresaID))


class ColumnMeta(str):
    """
    ColumnMeta is a string subclass that represents a column in a Polars DataFrame.
    It can also store an optional expression function and data type.
    The name is set automatically from the attribute name where it's assigned.
    """

    _expr_fn: Optional[Callable[[], pl.Expr]]
    _dtype: Optional[SchemaInitDataType]

    def __new__(
        cls,
        name: Optional[str] = None,
        expr_fn: Optional[Callable[[], pl.Expr]] = None,
        dtype: Optional[SchemaInitDataType] = None,
    ):
        # Name will be set later by metaclass
        if name is None:
            obj = str.__new__(cls, "")
        else:
            obj = str.__new__(cls, name)
        obj._expr_fn = expr_fn
        obj._dtype = dtype
        return obj

    def _set_name(self, name: str):
        # Called by metaclass to set the name after assignment
        self._name = name

    def __str__(self):
        if not hasattr(self, "_name"):
            raise ValueError("ColumnMeta name not set")
        return self._name

    @property
    def expr_fn(self) -> Optional[Callable[[], pl.Expr]]:
        return self._expr_fn

    @property
    def dtype(self) -> Optional[SchemaInitDataType]:
        return self._dtype

    @property
    def expr(self) -> pl.Expr:
        if self._expr_fn is None:
            return pl.col(str(self))
        return self._expr_fn()


class SchemaMetaType(type):
    def __new__(mcs, name, bases, namespace):
        for attr, value in namespace.items():
            if isinstance(value, ColumnMeta):
                value._set_name(attr)
        return super().__new__(mcs, name, bases, namespace)


class SchemaMeta(metaclass=SchemaMetaType):
    """Base schema: gather ColumnMeta from this class and all superclasses."""

    @classmethod
    def _collect_column_meta(cls) -> list[ColumnMeta]:
        cols: list[ColumnMeta] = []
        for base in reversed(cls.__mro__):
            for val in vars(base).values():
                if isinstance(val, ColumnMeta):
                    cols.append(val)
        return cols

    @classmethod
    def columns(cls) -> list[ColumnMeta]:
        return cls._collect_column_meta()

    @classmethod
    def to_mapping(cls) -> dict[str, SchemaInitDataType]:
        return {str(col): col.dtype for col in cls.columns() if col.dtype is not None}

    @classmethod
    def to_schema(cls) -> pl.Schema:
        return pl.Schema(cls.to_mapping())


@dataclass
class LazyFrameWithMeta:
    frame: pl.LazyFrame
    primary_keys: tuple[str, ...]
    date_name: Optional[str] = None
    emp_id_name: Optional[str] = None
    schema: Optional[type[SchemaMeta]] = None

    def __post_init__(self):
        # Ensure all primary_keys exist in the LazyFrame schema
        frame_columns = set(self.frame.collect_schema().names())
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
            schema=self.schema,
        )


# Entrada: dataframes disponibles para las reglas
@dataclass
class DataFramesInput:
    regalias: LazyFrameWithMeta
    calendario: LazyFrameWithMeta
    articulos: LazyFrameWithMeta
    ventas: LazyFrameWithMeta
    vendedores: LazyFrameWithMeta
    usuarios_sucursales: LazyFrameWithMeta
    # sucursales: LazyFrameWithMeta
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


@dataclass
class ProcConfig:
    connection_str: str
    fecha_inicio: dt.date
    fecha_fin: dt.date
    empresas_id: set[int]
    limit: int | None = None
    # Puedes agregar más salidas según sea necesario
