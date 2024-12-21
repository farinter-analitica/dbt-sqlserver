### Instalar requisitos en un venv ej: ###

```bash
python3.12 -m venv .venv_main_dagster

pip install -e dagster_shared_gf --config-settings editable_mode=compat
pip install -e dagster_sap_gf --config-settings editable_mode=compat
pip install -e dagster_kielsa_gf --config-settings editable_mode=compat

```
### Uso de github ###
Requiere la creacion de un token para usar para el clone o remote:
git clone https://<MYTOKEN>@github.com/org-name/repo-name.git
git add origin https://<MYTOKEN>@github.com/org-name/repo-name.git


### Configuración local ###


### Ejecucion local ###
La ejecucion local es posible teniendo una base de datos postgresql de desarrollo individual o compartida, o reconfigurando dagster.yaml eliminando las opciones de postgres. A continuacion datos requeridos de la conexión con postgresql en el archivo .env.

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
