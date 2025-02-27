## Instalación y Configuración ##

Para instalar los requisitos en un entorno virtual:

```bash
# Opción 1: Instalación manual
python3.12 -m venv .venv_main_dagster

pip install -e dagster_shared_gf --config-settings editable_mode=compat
pip install -e dagster_sap_gf --config-settings editable_mode=compat
pip install -e dagster_kielsa_gf --config-settings editable_mode=compat

# Opción 2: Usando el script de desarrollo
python scripts/deployment.py dev
```

## Configuración de GitHub ##
Se requiere la creación de un token para clonar o configurar el repositorio remoto:
```bash
git clone https://<MYTOKEN>@github.com/org-name/repo-name.git
git add origin https://<MYTOKEN>@github.com/org-name/repo-name.git
```

## Configuración Local ##
1. Crear los archivos de configuración basándose en los archivos .sample
2. Configurar las variables de entorno necesarias en el archivo .env

## Ejecución Local ##
La ejecución local requiere:
1. Base de datos PostgreSQL (desarrollo individual o compartida)
2. Configuración en dagster.yaml con las siguientes variables de entorno:

```yaml
storage:
 postgres:
   postgres_db:
     username: { env: DAGSTER_PG_USERNAME }
     password: { env: DAGSTER_PG_PASSWORD }
     hostname: { env: DAGSTER_PG_HOST }
     db_name: { env: DAGSTER_PG_DB }
     port: { env: DAGSTER_PG_PORT }
```

## Despliegue ##
El sistema cuenta con despliegue automático a través de GitHub Actions:

1. **Entornos**:
  - Desarrollo (dev): Al hacer push a la rama `dev`
  - Producción (prd): Al hacer push a la rama `main`

2. **Tipos de Despliegue**:
  - `deploy-full`: Regenera plantillas de servicio y actualiza todo (cuando hay cambios en scripts/generate_dagster_service.py)
  - `deploy-partial`: Actualiza Python y dependencias (cuando hay cambios en .python-version)
  - `deploy-fast`: Solo actualiza dependencias (cuando hay cambios en pyproject.toml)
  - `deploy-continuous`: Actualiza solo el código (para otros cambios) Siempre se requiere recargar en dagster manualmente.

3. **El proceso de despliegue**:
  - Detecta automáticamente el tipo de cambios
  - Actualiza el código desde el repositorio
  - Procesa archivos DBT
  - Ejecuta la estrategia de despliegue correspondiente
  - Verifica el estado de los servicios

## Pruebas y Commits ##

### Pre-Commit ###

Se utiliza pre-commit para ejecutar pruebas estaticas y formatear el código antes de cada commit. La primera
vez puede tardar un poco en instalar las dependencias.

### Publicacion ###


Antes de publicar commits, ejecutar manualmente las pruebas usando:

1. Herramienta del IDE (recomendado para mejor control)
2. Pytest directamente:
```bash
pytest
```
3. Script de pruebas rápido:
```bash
python run_all_tests.py
```
