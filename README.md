## Instalación y Configuración ##

Para instalar los requisitos en un entorno virtual:

```bash
# Opción 1: Usando el script de desarrollo
python scripts/deployment.py dev

# Opción 2: Instalación manual (esto no realiza todos los procedimientos para dev.)
python3.12 -m venv .venv_main_dagster

pip install -e dagster_shared_gf --config-settings editable_mode=compat
pip install -e dagster_sap_gf --config-settings editable_mode=compat
pip install -e dagster_kielsa_gf --config-settings editable_mode=compat
```

## Configuración de GitHub ##
Se requiere la creación de un token o una llave ssh para clonar o configurar el repositorio remoto:

```bash
git clone https://<MYTOKEN>@github.com/org-name/repo-name.git
git add origin https://<MYTOKEN>@github.com/org-name/repo-name.git
```

### Configuración de Llaves SSH para Repos Privados ###
Para acceder a repositorios privados como dependencias, es necesario configurar llaves SSH:

```bash
# Verificar llaves SSH existentes
python scripts/deployment.py check-deploy-key

# Generar una nueva llave SSH para un repositorio específico
python scripts/deployment.py setup-deploy-key --repo=algoritmos-gf --org=farinter-analitica

# Probar la conexión SSH a un repositorio
python scripts/deployment.py test-deploy-key --repo=algoritmos-gf --org=farinter-analitica
```

Después de generar la llave, agrégala como deploy key en la configuración del repositorio en GitHub:
https://github.com/farinter-analitica/algoritmos-gf/settings/keys

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
## Configuracion de github actions

Se utiliza github actions para el despliegue automatico. Para configurarlo:

1. Ir a la sección "Settings" del repositorio en GitHub
2. Seleccionar "Actions" en el menú lateral
3. Habilitar GitHub Actions si no está activo
4. Habilitar sel-runner: [Configuracion farinter-analitica](https://github.com/organizations/farinter-analitica/settings/actions/runners)
  a. Seguir las instrucciones
  b. Activar el servicio: [Docs Service](https://docs.github.com/en/actions/hosting-your-own-runners/managing-self-hosted-runners/configuring-the-self-hosted-runner-application-as-a-service)
5. Los workflows se activarán automáticamente al hacer push:
   - Push a rama dev -> despliegue en desarrollo
   - Push a rama main -> despliegue en producción
6. Verificar la ejecución en la pestaña "Actions" del repositorio

Para más detalles sobre los workflows disponibles, revisar los archivos en .github/workflows/
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

## Dependencias Externas ##
El proyecto utiliza dependencias externas de repositorios privados:

```toml
[project.optional-dependencies]
external = [
    "statstools_gf @ git+ssh://git@github.com-algoritmos-gf/farinter-analitica/algoritmos-gf.git@v0.9#subdirectory=py_statstools_gf",
]
```

Estas dependencias requieren llaves SSH configuradas correctamente. El script de despliegue intentará instalarlas, pero continuará el despliegue incluso si fallan.

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
