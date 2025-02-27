# dagster_sap_gf

## Primeros pasos

Primero, instala tu ubicación de código Dagster como un paquete de Python. Al usar la bandera --editable, pip instalará tu paquete de Python en ["modo editable"](https://pip.pypa.io/en/latest/topics/local-project-installs/#editable-installs) para que mientras desarrollas, los cambios locales en el código se apliquen automáticamente.


pip install -e ".[dev]"


Luego, inicia el servidor web de la interfaz de Dagster:


dagster dev


Abre http://localhost:3000 en tu navegador para ver el proyecto.

Puedes comenzar a escribir assets en `dagster_sap_gf/assets.py`. Los assets se cargan automáticamente en la ubicación del código Dagster mientras los defines.

## Desarrollo

### Agregar nuevas dependencias de Python

Puedes especificar nuevas dependencias de Python en `pyproject.toml`.

### Pruebas unitarias

Las pruebas están en el directorio `dagster_sap_gf_tests` y puedes ejecutar las pruebas usando `pytest`:


pytest dagster_sap_gf_tests


### Programaciones y sensores

Si deseas habilitar [Programaciones](https://docs.dagster.io/concepts/partitions-schedules-sensors/schedules) o [Sensores](https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors) de Dagster para tus trabajos, el proceso [Dagster Daemon](https://docs.dagster.io/deployment/dagster-daemon) debe estar ejecutándose. Esto se hace automáticamente cuando ejecutas `dagster dev`.

Una vez que tu Dagster Daemon esté ejecutándose, puedes comenzar a activar programaciones y sensores para tus trabajos.

## Desplegar en Dagster Cloud

La forma más fácil de desplegar tu proyecto Dagster es usar Dagster Cloud.

Consulta la [Documentación de Dagster Cloud](https://docs.dagster.cloud) para obtener más información.
