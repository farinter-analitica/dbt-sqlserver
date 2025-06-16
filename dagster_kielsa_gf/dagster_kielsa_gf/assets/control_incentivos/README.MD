# Control de Incentivos — Kielsa Dagster

Este módulo implementa el procesamiento flexible de incentivos para el canje de regalias, usando reglas configurables y componibles. Está diseñado para ser extensible y desacoplado, facilitando la incorporación de nuevas reglas de negocio.

## Jerarquía de Archivos

```
control_incentivos/
├── procesamiento_incentivos.py        # Orquestador principal del flujo de incentivos
└── reglas_incentivos/
    ├── base.py                        # Clase base abstracta para reglas de incentivos
    ├── config.py                      # Tipos y configuración de entrada/salida y empresas
    ├── default.py                     # Regla por defecto (catch-all)
    └── honduras.py                    # Ejemplo de regla específica (HN 2025)
```

## Resumen de Funcionamiento

### 1. Orquestación (`procesamiento_incentivos.py`)
- **ProcesamientoIncentivos**: Clase principal que orquesta la obtención de datos (regalias, calendario, artículos), aplica reglas y produce los resultados.
- Obtiene los datos desde SQL Server usando Polars.
- Prepara los dataframes de entrada y los agrupa según la lógica de negocio.
- Asigna la regla de incentivo adecuada a cada grupo de datos según empresa y fecha.
- Ejecuta el método `procesar` de cada regla, consolidando los resultados.

### 2. Reglas de Incentivo (`reglas_incentivos/`)
- **base.py**: Define la interfaz abstracta `BaseReglaIncentivo` que toda regla debe implementar. Incluye:
  - Métodos y propiedades para definir a qué empresas y fechas aplica la regla.
  - Método abstracto `procesar` que recibe los dataframes y devuelve los resultados.
- **config.py**: Define tipos y estructuras para entradas y salidas de las reglas, y los IDs de empresas válidos.
- **default.py**: Implementa una regla por defecto que aplica si ninguna otra regla es válida para el caso.
- **honduras.py**: Ejemplo de una regla concreta para Honduras, con lógica de incentivos diferenciada por casa comercial.

## ¿Cómo se extiende?

1. **Crear una nueva regla**  
   - Crear un archivo nuevo en `reglas_incentivos/` (ej: `nicaragua.py`).
   - Heredar de `BaseReglaIncentivo`.
   - Implementar las propiedades `EMP_ID`, `VALID_FROM`, `VALID_UNTIL` y el método `procesar`.

2. **Registrar la regla**  
   - Modificar el registro de reglas (normalmente en `reglas_incentivos/__init__.py` o donde se construya el registry) para incluir la nueva clase.

3. **(Opcional) Agregar configuración**  
   - Si la regla requiere parámetros, puede recibirlos vía el argumento `config` en el constructor.

4. **El flujo la usará automáticamente**  
   - El orquestador asigna la regla correcta a cada grupo de datos según empresa y fecha.

## Ejemplo de nueva regla

```python:dagster_kielsa_gf/dagster_kielsa_gf/assets/control_incentivos/reglas_incentivos/nicaragua.py
import datetime as dt
from .base import BaseReglaIncentivo
from .config import EmpresaID, DataFramesInputDict, DataFramesOutputDict

class ReglaIncentivoNI2025(BaseReglaIncentivo):
    @property
    def EMP_ID(self) -> frozenset[EmpresaID]:
        return frozenset([2])  # ID de Nicaragua

    @property
    def VALID_FROM(self) -> dt.date:
        return dt.date(2025, 1, 1)

    @property
    def VALID_UNTIL(self) -> dt.date:
        return dt.date(9999, 12, 31)

    def procesar(self, dataframes: DataFramesInputDict) -> DataFramesOutputDict:
        # Implementar lógica específica aquí
        ...
```
**Contacto:** Equipo Analítica Farinter
