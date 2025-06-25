from collections.abc import Iterator
import datetime as dt
from typing import Any, Callable, ClassVar, Literal, TypeVar, get_args, Optional
from dataclasses import dataclass, fields
import polars as pl
from polars.schema import SchemaInitDataType

# Define los IDs válidos en el Literal
EmpresaID = Literal[1, 2, 3, 4, 5]

# Deriva la lista de IDs válidos a partir del Literal
EMPRESAS_ID: frozenset[EmpresaID] = frozenset(get_args(EmpresaID))

SchemaT = TypeVar("SchemaT", bound="SchemaBase")
ExprFn = Callable[[], pl.Expr]


class Column(str):
    _name: str | None
    _dtype: SchemaInitDataType | None
    _expr: pl.Expr | None

    def __new__(
        cls, name: str | None = None, dtype: Any = None, expr: pl.Expr | None = None
    ):
        obj = super().__new__(cls, name or "")
        obj._name = name
        obj._dtype = dtype
        obj._expr = expr
        return obj

    @property
    def name(self) -> str:
        if self._name is None:
            raise ValueError("Name is not set")
        return self._name

    @property
    def dtype(self) -> SchemaInitDataType:
        if self._dtype is None:
            raise ValueError("Dtype is not set")
        return self._dtype

    @property
    def expr(self) -> pl.Expr:
        if self._expr is None:
            return pl.col(self.name)
        return self._expr

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return f"Column(name={self.name!r}, dtype={self._dtype}, expr={self._expr})"


class SchemaBase:
    _columns: ClassVar[dict[str, Column]]

    def __init_subclass__(cls: type, **kwargs: Any) -> None:
        super().__init_subclass__(
            **kwargs
        )  # Es buena práctica llamar a la implementación de la clase base.

        # Este diccionario guardará las columnas procesadas para la subclase actual
        columns_for_this_subclass = {}

        # Iterar a través de los atributos de la subclase
        # Usamos vars(cls) para obtener solo los atributos definidos directamente en esta subclase
        # y dir(cls) para incluir atributos heredados o métodos, si fuera necesario
        for attr_name, attr_value in cls.__dict__.items():
            if isinstance(attr_value, Column):
                col: Column = attr_value
                # Si el nombre de la columna no está establecido (es vacío), usar el nombre del atributo
                if col._name is None or col._name == "":
                    # Crear una *nueva* instancia de Column con el nombre rellenado
                    # y los mismos dtype y expr.
                    # Esto es crucial porque los objetos Column son mutables a este nivel
                    # y queremos que cada Column tenga el nombre correcto.
                    new_col = Column(attr_name, dtype=col._dtype, expr=col._expr)

                    # Reemplazar el atributo en la clase con la nueva instancia de Column.
                    # Esto asegura que cuando accedas a MySchema.ID, obtengas la Column con el nombre "ID".
                    setattr(cls, attr_name, new_col)
                    columns_for_this_subclass[attr_name] = new_col
                else:
                    columns_for_this_subclass[attr_name] = col

        # Asignar las columnas procesadas al atributo _columns de la subclase
        cls._columns = columns_for_this_subclass

    @classmethod
    def columns(cls) -> list[Column]:
        return list(cls._columns.values())

    @classmethod
    def to_mapping(cls) -> dict[str, SchemaInitDataType]:
        return {
            col.name: col.dtype
            for col in cls._columns.values()
            if col._dtype is not None
        }

    @classmethod
    def to_schema(cls) -> pl.Schema:
        return pl.Schema(cls.to_mapping())


@dataclass
class LazyFrameWithMeta:
    frame: pl.LazyFrame
    primary_keys: tuple[str, ...]
    date_name: Optional[str] = None
    emp_id_name: Optional[str] = None
    schema: Optional[type[SchemaBase]] = None
    validar_llave_primaria: Optional[bool] = False
    columnas_iniciales: Optional[list[str]] = None

    def __post_init__(self):
        # Ensure all primary_keys exist in the LazyFrame schema
        frame_columns = self.frame.limit(1).collect(engine="streaming").columns
        self.columnas_iniciales = frame_columns
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

        if self.validar_llave_primaria:
            self.validate_primary_keys()

    def with_frame(
        self,
        lf: pl.LazyFrame,
        primary_keys: Optional[tuple[str, ...]] = None,
        validar_llave_primaria: Optional[bool] = None,
    ) -> "LazyFrameWithMeta":
        """
        Crea una nueva instancia de LazyFrameWithMeta usando el nuevo LazyFrame `lf`
        pero conservando primary_keys, date_name y emp_id_name de esta instancia,
        a menos que se especifique lo contrario.
        """
        primary_keys = primary_keys or self.primary_keys
        validar_llave_primaria = validar_llave_primaria or self.validar_llave_primaria

        return LazyFrameWithMeta(
            frame=lf,
            primary_keys=primary_keys,
            date_name=self.date_name,
            emp_id_name=self.emp_id_name,
            schema=self.schema,
            validar_llave_primaria=validar_llave_primaria,
        )

    def validate_primary_keys(self) -> "LazyFrameWithMeta":
        """
        Validates the primary keys of the LazyFrame to ensure no duplicates exist.

        If `validar_llave_primaria` is True, this method checks that the primary keys
        do not contain any duplicate values. If duplicates are found, a ValueError is raised.

        Returns:
            LazyFrameWithMeta: The current instance, allowing for method chaining.

        Raises:
            ValueError: If duplicate primary keys are detected when validation is enabled.
        """
        llaves_primarias = self.frame.select(self.primary_keys).collect(
            engine="streaming"
        )
        if llaves_primarias.is_duplicated().any():
            raise ValueError(
                f"Las llaves primarias de contienen duplicados. {llaves_primarias.filter(llaves_primarias.is_duplicated()).head(10)}"
            )

        df_vacios = llaves_primarias.null_count()
        if df_vacios.sum_horizontal().item() > 0:
            raise ValueError(
                f"Las llaves primarias están vacías en {df_vacios.columns}"
            )

        return self


# Entrada: dataframes disponibles para las reglas
@dataclass
class DataFramesInput:
    regalias: LazyFrameWithMeta
    calendario: LazyFrameWithMeta
    articulos: LazyFrameWithMeta
    ventas: LazyFrameWithMeta
    vendedores: LazyFrameWithMeta
    usuarios_sucursales: LazyFrameWithMeta
    roles: LazyFrameWithMeta
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


class DataFramesRegla(DataFramesInput):
    ventas_incentivo: LazyFrameWithMeta


# Salida: resultados procesados por la regla
@dataclass
class DataFramesOutput:
    regalias_incentivo: LazyFrameWithMeta
    detalle_incentivo: LazyFrameWithMeta


@dataclass
class ProcConfig:
    connection_str: str
    fecha_inicio: dt.date
    fecha_fin: dt.date
    empresas_id: set[int]
    limit: int | None = None
    # Puedes agregar más salidas según sea necesario
