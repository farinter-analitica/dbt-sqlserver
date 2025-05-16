from __future__ import annotations

import datetime as dt
from typing import Any

import polars as pl

from dagster_kielsa_gf.assets.control_incentivos.reglas_incentivos.base import (
    BaseReglaIncentivo,
)
from dagster_kielsa_gf.assets.control_incentivos.reglas_incentivos.honduras import (
    ReglaIncentivoHN2025,
)
from dagster_kielsa_gf.assets.control_incentivos.reglas_incentivos.default import (
    ReglaIncentivoDefault,
)


def get_reglas_incentivo() -> list[BaseReglaIncentivo]:
    """
    Devuelve la lista de reglas de incentivo.
    Puedes parametrizar aquí si necesitas reglas dinámicas.
    """
    reglas: list[BaseReglaIncentivo] = [
        ReglaIncentivoHN2025({}),
        # ...agrega aquí más reglas...
    ]

    return reglas


class ReglaIncentivoRegistry:
    """
    Registro y lógica de selección de reglas de incentivo.
    Permite validar solapamientos, seleccionar reglas y generar tabla de mapeo.
    """

    def __init__(
        self, reglas: list[BaseReglaIncentivo], regla_por_defecto: BaseReglaIncentivo
    ):
        self.lista_reglas = reglas
        self._regla_por_defecto = regla_por_defecto
        self._validar_no_solapamiento()
        self._map_reglas = self._build_mapping_dict()
        self._table_mapping = self._build_mapping_table()

    def _validar_no_solapamiento(self) -> None:
        reglas_concretas = [r for r in self.lista_reglas if not r.es_regla_por_defecto]
        for i, regla_a in enumerate(reglas_concretas):
            emp_ids_a = regla_a.emp_ids
            fechas_a = regla_a.rango_fechas
            for regla_b in reglas_concretas[i + 1 :]:
                emp_ids_b = regla_b.emp_ids
                fechas_b = regla_b.rango_fechas
                emp_ids_inter = emp_ids_a & emp_ids_b
                if not emp_ids_inter:
                    continue
                start = max(fechas_a[0], fechas_b[0])
                end = min(fechas_a[1], fechas_b[1])
                if start <= end:
                    raise ValueError(
                        f"Solapamiento entre {regla_a.nombre} y {regla_b.nombre} "
                        f"para emp_id(s)={emp_ids_inter} en fechas {start} a {end}"
                    )

    def _build_mapping_dict(self) -> dict[str, dict[str, Any]]:
        """
        Devuelve un diccionario con los rangos de cada regla, validando nombres únicos.
        """
        mapping: dict[str, dict[str, Any]] = {}
        for regla in self.lista_reglas:
            nombre = regla.nombre
            if nombre in mapping:
                raise ValueError(f"Nombre de regla duplicado: {nombre}")
            if regla.es_regla_por_defecto:
                continue  # no incluimos la regla por defecto
            emp_ids = regla.emp_ids
            fechas = regla.rango_fechas
            mapping[nombre] = {
                "Regla_Nombre": nombre,
                "Regla_Emp_Id": emp_ids,
                "Regla_Fecha_Desde": fechas[0],
                "Regla_Fecha_Hasta": fechas[1],
                "Regla_Obj": regla,
            }

        return mapping

    @property
    def map_reglas(self) -> dict[str, dict[str, Any]]:
        return self._map_reglas

    @property
    def regla_por_defecto(self) -> BaseReglaIncentivo:
        return self._regla_por_defecto

    def regla_por_nombre(self, regla_nombre: str) -> BaseReglaIncentivo:
        """
        Devuelve la regla correspondiente al nombre dado.
        """
        if regla_nombre == self.regla_por_defecto.nombre:
            return self.regla_por_defecto
        if regla_nombre not in self.map_reglas:
            raise ValueError(f"No existe una regla con el nombre {regla_nombre}")
        return self.map_reglas[regla_nombre]["Regla_Obj"]

    def _build_mapping_table(self) -> pl.LazyFrame:
        """
        Convierte el diccionario de mapeo a un LazyFrame de Polars.
        """
        mapping_dict = self.map_reglas
        mapping_list = [
            {k: v for k, v in data.items() if k != "Regla_Obj"}
            for data in mapping_dict.values()
        ]

        return pl.LazyFrame(
            mapping_list, schema_overrides={"Regla_Emp_Id": pl.List(pl.Int64)}
        ).with_columns(pl.col("Regla_Emp_Id").list.explode())

    def asignar_regla_a_dataframe(
        self,
        df: pl.LazyFrame,
        emp_id_col: str = "Emp_Id",
        fecha_col: str = "Fecha_Id",
    ) -> pl.LazyFrame:
        """
        Asigna la regla correspondiente a cada fila de un LazyFrame de canjes/regalías, etc.
        Devuelve el LazyFrame original con una columna adicional 'Regla_Nombre'.
        """
        mapping_table = self._table_mapping

        # Convertir tipos por si es necesario
        df = df.with_columns(regla_fecha_datos=pl.col(fecha_col).cast(pl.Date))
        mapping_table = mapping_table.with_columns(
            pl.col("Regla_Emp_Id").cast(df.schema[emp_id_col]),
            pl.col("Regla_Fecha_Desde").cast(pl.Date),
            pl.col("Regla_Fecha_Hasta").cast(pl.Date),
        )

        # Join con las reglas existentes
        join_reglas = (
            df.select(pl.col(emp_id_col), pl.col("regla_fecha_datos"))
            .unique()
            .join_where(
                mapping_table,
                pl.col(emp_id_col).eq(pl.col("Regla_Emp_Id")),
                pl.col("regla_fecha_datos") >= pl.col("Regla_Fecha_Desde"),
                pl.col("regla_fecha_datos") <= pl.col("Regla_Fecha_Hasta"),
            )
            .drop("Regla_Emp_Id")
        )

        print(join_reglas.head(10).collect())

        # Join por Emp_Id y filtrar por rango de fechas
        join_df = (
            df.join(
                join_reglas,
                left_on=[emp_id_col, "regla_fecha_datos"],
                right_on=["Emp_Id", "regla_fecha_datos"],
                how="left",
            )
            .drop(pl.selectors.ends_with("_right"))
            .with_columns(
                pl.col("Regla_Nombre").fill_null(self.regla_por_defecto.nombre),
                pl.col("Regla_Fecha_Desde").fill_null(
                    self.regla_por_defecto.fecha_desde
                ),
                pl.col("Regla_Fecha_Hasta").fill_null(
                    self.regla_por_defecto.fecha_hasta
                ),
            )
        ).drop("regla_fecha_datos")

        return join_df


def build_registry_incentivo() -> ReglaIncentivoRegistry:
    """
    Construye y valida el registro de reglas cuando se llama.
    """
    reglas = get_reglas_incentivo()
    return ReglaIncentivoRegistry(reglas, ReglaIncentivoDefault({}))


if __name__ == "__main__":
    # Construir el registro
    registry = build_registry_incentivo()

    # Supón que tienes un LazyFrame de canjes con columnas "Emp_Id" y "Fecha"
    df_canje = pl.LazyFrame(
        {
            "Emp_Id": [1, 2, 1, 3],
            "Fecha_Id": [
                dt.date(2023, 1, 1),
                dt.date(2025, 1, 1),
                dt.date(2021, 5, 1),
                dt.date(2022, 7, 1),
            ],
            # ... otras columnas ...
        }
    )

    # Asignar la regla correspondiente a cada fila (devuelve un LazyFrame)
    df_con_regla = registry.asignar_regla_a_dataframe(df_canje)
    print(registry._table_mapping.head(10).collect())
    print(df_con_regla.head(10).collect())
