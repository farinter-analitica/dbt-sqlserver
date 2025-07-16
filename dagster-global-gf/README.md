# dagster_global_gf

## Descripción
Este proyecto contiene recursos globales y compartidos para otros proyectos Dagster en el repositorio.

## Primeros pasos

Instala el paquete en modo editable para desarrollo:

```bash
pip install -e ".[dev]"
```

Luego, inicia el servidor web de Dagster:

```bash
dagster dev
```

Abre http://localhost:3000 en tu navegador para ver el proyecto.

Puedes comenzar a escribir assets en `dagster_global_gf/assets.py`. Los assets se cargan automáticamente en la ubicación de código Dagster mientras los defines.

## Desarrollo

### Agregar nuevas dependencias de Python

Puedes especificar nuevas dependencias de Python en `pyproject.toml`.

### Pruebas unitarias

Las pruebas están en el directorio `dagster_global_gf_tests` y puedes ejecutar las pruebas usando `pytest`:

```bash
pytest dagster_global_gf_tests
```

### Programaciones y sensores

Si deseas habilitar [Programaciones](https://docs.dagster.io/concepts/partitions-schedules-sensors/schedules) o [Sensores](https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors) de Dagster para tus trabajos, el proceso [Dagster Daemon](https://docs.dagster.io/deployment/dagster-daemon) debe estar ejecutándose. Esto se hace automáticamente cuando ejecutas `dagster dev`.

Una vez que tu Dagster Daemon esté ejecutándose, puedes comenzar a activar programaciones y sensores para tus trabajos.
