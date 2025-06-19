## Instalación y Configuración

Este repositorio utiliza **Dev Containers** (devcontainer) y **Git Credential Manager** para gestionar las credenciales y el entorno de desarrollo. **Al abrir el proyecto en VS Code y seleccionar "Reopen in Container", todas las dependencias y configuraciones se instalarán automáticamente.** No es necesario ejecutar scripts manualmente para instalar dependencias o configurar el entorno.

### Requisitos Previos

- [Docker](https://www.docker.com/products/docker-desktop) instalado y en ejecución.
   - Si se usa el WSL (mejor rendimiento), debe estar actualizado, si la configuracion de git no funciona usar token de acceso personal.
- [Git y GCM](https://git-scm.com/) instalado y configurado. [Git for Windows](https://gitforwindows.org/)
   - Para autenticación automática en el devcontainer instala el [Git Credential Manager](https://aka.ms/gcm) para tu sistema operativo.
   - Otras opcioones [sharing-git-credentials](https://code.visualstudio.com/remote/advancedcontainers/sharing-git-credentials)
   - De lo contrario utilizar un repositorio con token de acceso personal.
- [Visual Studio Code](https://code.visualstudio.com/) con la extensión [Dev Containers](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers).

### Clonar y Activar el Repositorio en un Dev Container

1. **Configurar Git Credential Manager**  
   Abre una terminal y ejecuta:
   ```bash
   git-credential-manager configure
   ```
   Sigue las instrucciones para autenticarte y asegurarte de que el modo de autenticación es por HTTPS o SSH según tu preferencia. Esto permitirá que las credenciales se compartan correctamente con el contenedor y puedas hacer `push` y `pull` sin problemas.

   [Más información sobre Git Credential Manager](https://aka.ms/gcm)

2. **Clonar el repositorio usando Git**  
   Puedes clonar el repositorio directamente con:
   ```bash
   git clone https://github.com/farinter-analitica/main-dagster.git
   cd main-dagster
   ```

   Opcion b: Clonar con vscode directamente en un volumen (mejor rendimiento pero inaccesible desde el sistema operativo) y saltarse paso 3.

3. **Abrir el proyecto en VS Code y activar el Dev Container**  
   - Abre la carpeta del repositorio en VS Code.
   - Si tienes la extensión Dev Containers instalada, VS Code te sugerirá automáticamente "Reabrir en Contenedor" (`Reopen in Container`).  
   - Si no aparece, abre la paleta de comandos (`Ctrl+Shift+P`), busca "Dev Containers: Reopen in Container" y selecciónalo.

4. **El entorno se instalará automáticamente**  
   Al abrir el proyecto en el Dev Container, todas las dependencias y configuraciones necesarias se instalarán automáticamente según la configuración del repositorio. No es necesario ejecutar scripts manuales.

   Con el uso de WSL2 podria conllevar la necesidad de utilizar la configuracion en .devcontainer/.wslconfig para poder acceder a los puertos expuestos desde el host local. %UserProfile%\.wslconfig, fuera de wsl.

### DAGSTER DEV
Debes correr dagster dev -h "0.0.0.0" si se cierra para poder acceder a la interfaz de dagster desde el host local.

### Variables de entorno
Asegurarse de no incluir rutas de windows en las variables de entorno.


### Configuración de Llaves SSH para Repos Privados

Para acceder a repositorios privados como dependencias, es necesario configurar llaves SSH:

```bash
# Verificar llaves SSH existentes
python scripts/deployment.py check-deploy-key

# Generar una nueva llave SSH para un repositorio específico
python scripts/deployment.py setup-deploy-key --repo=algoritmos-gf --org=farinter-analitica

# Forzar la creación de una nueva llave (sobreescribe la existente)
python scripts/deployment.py setup-deploy-key --repo=algoritmos-gf --org=farinter-analitica --force

# Probar la conexión SSH a un repositorio
python scripts/deployment.py test-deploy-key --repo=algoritmos-gf --org=farinter-analitica
```

Después de generar la llave, agrégala como deploy key en la configuración del repositorio en GitHub:  
https://github.com/farinter-analitica/algoritmos-gf/settings/keys

## Configuración Local

1. Crear los archivos de configuración basándose en los archivos .sample.
2. Configurar las variables de entorno necesarias en el archivo .env, o .dlt/secrets.toml o .slingdata/env.yaml

## Ejecución Local

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

## Configuración de GitHub Actions

Se utiliza GitHub Actions para el despliegue automático. Para configurarlo:

1. Ir a la sección "Settings" del repositorio en GitHub.
2. Seleccionar "Actions" en el menú lateral.
3. Habilitar GitHub Actions si no está activo.
4. Habilitar self-runner: [Configuración farinter-analitica](https://github.com/organizations/farinter-analitica/settings/actions/runners)
   - Seguir las instrucciones.
   - Activar el servicio: [Docs Service](https://docs.github.com/en/actions/hosting-your-own-runners/managing-self-hosted-runners/configuring-the-self-hosted-runner-application-as-a-service)
5. Los workflows se activarán automáticamente al hacer push:
   - Push a la rama dev → despliegue en desarrollo.
   - Push a la rama main → despliegue en producción.
6. Verificar la ejecución en la pestaña "Actions" del repositorio.

### Despliegue Basado en Mensajes de Commit

El sistema de despliegue ha sido actualizado para permitir forzar distintos tipos de despliegue mediante el mensaje del último commit. Para ello, basta con incluir en el mensaje de commit:

  github_actions:deployment_type=deploy-full

Puedes utilizar cualquiera de los siguientes valores:
- deploy-full: Regenera plantillas de servicio, actualiza la versión de Python y todas las dependencias.
- deploy-partial: Actualiza Python y dependencias.
- deploy-fast: Solo actualiza dependencias.
- deploy-continuous: Actualiza solo el código (cambios mínimos).

Si el mensaje del commit no incluye la clave "github_actions:deployment_type", se utilizará el valor predeterminado de deploy-continuous.

## Despliegue

El sistema cuenta con despliegue automático a través de GitHub Actions y el nuevo script deployment.py con soporte para uv:

1. **Entornos**:
   - Desarrollo (dev): Al hacer push a la rama `dev`.
   - Producción (prd): Al hacer push a la rama `main`.

2. **Tipos de Despliegue**:
   - `deploy-full`: Regenera plantillas de servicio, Python y actualiza todas las dependencias.
   - `deploy-partial`: Actualiza Python y dependencias.
   - `deploy-fast`: Solo actualiza dependencias.
   - `deploy-continuous`: Actualiza solo el código (cambios mínimos).

3. **Ejecución Manual de Despliegue**:

```bash
# Configurar variables de entorno
export ENV=dev
export DEPLOY_DIR=/path/to/deployment

# Ejecutar el tipo de despliegue deseado
python scripts/deployment.py deploy-full
python scripts/deployment.py deploy-partial
python scripts/deployment.py deploy-fast
python scripts/deployment.py deploy-continuous
```

El nuevo sistema utiliza `uv` para gestionar versiones de Python y dependencias, lo que proporciona instalaciones más rápidas y consistentes entre entornos.

## Dependencias Externas

El proyecto utiliza dependencias externas de repositorios privados:

```toml
[project.optional-dependencies]
external = [
    "statstools_gf @ git+ssh://git@github.com-algoritmos-gf/farinter-analitica/algoritmos-gf.git@v0.9#subdirectory=py_statstools_gf",
]
```

Estas dependencias requieren llaves SSH configuradas correctamente. El script de despliegue intentará instalarlas, pero continuará el despliegue incluso si fallan.

## Pruebas y Commits

### Pre-Commit

Se utiliza pre-commit para ejecutar pruebas estáticas y formatear el código antes de cada commit. La primera vez puede tardar un poco en instalar las dependencias.

### Publicación

Antes de publicar commits, ejecutar manualmente las pruebas usando:

1. Herramienta del IDE (recomendado para mejor control).
2. Pytest directamente:
```bash
pytest
```
3. Script de pruebas rápido:
```bash
python run_all_tests.py
```
