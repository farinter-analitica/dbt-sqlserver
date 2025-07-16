
# Instrucciones Copilot para main-dagster

## Visión General del Proyecto
- **Monorepo** para pipelines de datos basados en Dagster, con subpaquetes: `dagster-global-gf`, `dagster-kielsa-gf`, `dagster-sap-gf`, `dagster-shared-gf` y el proyecto `dbt_dwh_farinter`.
- Uso de **Dev Containers** para entornos de desarrollo consistentes. Todas las dependencias se instalan automáticamente al abrir en VS Code con Dev Containers.
- Se requiere **Git Credential Manager** y llaves SSH para acceder a dependencias privadas.

## Flujos de Trabajo Clave
- **Build & Install**: No es necesario instalar manualmente; las dependencias se gestionan con el dev container y `uv`.
- **Pruebas**: Ejecuta todos los tests con `pytest` o `python scripts/run_all_tests.py`. Pre-commit ejecuta chequeos estáticos y formatea el código.
- **Despliegue**: Automático vía GitHub Actions. El tipo de despliegue se controla con el mensaje de commit (ejemplo: `github_actions:deployment_type=deploy-full`). Despliegues manuales con `bash scripts/deployment.sh <tipo>`.
- **DBT**: El directorio `dbt_dwh_farinter` contiene modelos y scripts DBT para operaciones de data warehouse.
- **Dagster Dev**: Inicia la UI de Dagster con `dagster dev -h "0.0.0.0"` y accede en http://localhost:3000.

## Estructura y Convenciones
- **Subpaquetes**: Cada carpeta `dagster-*` es un paquete Python con su propio `pyproject.toml`, código y tests.
- **Código Compartido**: Ubica la lógica compartida en `dagster-shared-gf`.
- **Dependencias Externas**: Gestionadas en `pyproject.toml` usando URLs SSH. Configura llaves SSH para repos privados.
- **Archivos de Configuración**: Usa `.env`, `.dlt/secrets.toml` o `.slingdata/env.yaml` para variables de entorno. Evita rutas de Windows.
- **Base de Datos**: El desarrollo local requiere PostgreSQL; configura la conexión en `dagster.yaml` usando variables de entorno.

## Patrones y Ejemplos
- **Gestión de Llaves SSH**: Usa `python scripts/deployment.py setup-deploy-key --repo=<repo> --org=<org>` para gestionar llaves de despliegue de dependencias privadas.
- **Pruebas**: Ubica los tests en carpetas `<paquete>_tests/`. Usa `pytest` para ejecutarlos.
- **Tipos de Despliegue**: Controla la granularidad del despliegue de acuerdo a los tipos de cambios con palabras clave en el commit (ver README para opciones).
- **Pre-commit**: Los chequeos estáticos y el formateo se ejecutan antes de cada commit.
- **Commit Messages**: Usa mensajes de commit segun formato `templates/commit-template.git.txt`.

## Referencias
- Consulta los `README.md` (raíz y subpaquetes) para detalles de setup, despliegue y troubleshooting.
- Scripts clave: `scripts/deployment.py`, `scripts/deployment.sh`, `scripts/run_all_tests.py`.
- Proyecto DBT: `dbt_dwh_farinter/`
- Configuración Dagster: `dagster.yaml`, `workspace.yaml`

---

**Si eres un agente AI, sigue estas convenciones y flujos de trabajo para asegurar compatibilidad con las prácticas del equipo.**
