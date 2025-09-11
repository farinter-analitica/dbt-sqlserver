# Guía de Testing

Este documento describe cómo ejecutar y mantener las pruebas en el monorepo Dagster con entornos Python por *code location* gestionados con **uv**.

## 1. Arquitectura de Entornos
Cada ubicación de código (code location) tiene ahora su propio entorno virtual aislado:
- `dagster-global-gf/.venv`
- `dagster-kielsa-gf/.venv`
- `dagster-sap-gf/.venv`

La librería compartida `dagster-shared-gf` también tiene su propio .venv ya que necesita multiples paquetes para las pruebas.

La resolución y sincronización se hace usando `uv sync` con la variable `UV_PROJECT_ENVIRONMENT` apuntando al directorio `.venv` de cada location para garantizar:
- Instalación determinista basada en `pyproject.toml` + `uv.lock` raíz.
- Eliminación (prune) automática de dependencias obsoletas.

## 2. Creación / Actualización de entornos
Ejecutar:
```bash
bash scripts/deployment.sh install-deps --local
```
Esto recorrerá las ubicaciones configuradas y llamará internamente a `uv sync --frozen` para cada una. Puedes re‑ejecutarlo de forma idempotente tras cambios en dependencias.

## 3. Ejecución de Tests por Location
Ejemplo rápido (desde la raíz del repo), UV_PROJECT_ENVIRONMENT no es necesario:
```bash
for loc in dagster-global-gf dagster-kielsa-gf dagster-sap-gf; do 
  echo "== PYTEST $loc ==";
  (cd $loc; UV_PROJECT_ENVIRONMENT="$(pwd)/.venv" uv run --frozen pytest -q);
done
```

Ejecutar sólo un subconjunto (patrón):
```bash
cd dagster-kielsa-gf
UV_PROJECT_ENVIRONMENT="$(pwd)/.venv" uv run --frozen pytest -k test_jobs -q
```

Ejecutar todos (acepta los args de pytest):
`scripts/run_all_tests.py`

Notas:
- Se usa `uv run --frozen` para respetar el lock file sin recalcular resolución.
- Si necesitas recalcular .lock tras editar `pyproject.toml`, ejecuta manualmente `uv sync` en el workdir correspondiente.

### Uso de uv run --project vs --directory
------------------------------------------
- **uv run --project <ubicacion>**: Usa para scripts dentro de un paquete que necesitan su propio venv pero trabajar con el espacio de trabajo raíz. Ejemplo: `uv run --project dagster-global-gf scripts/some_script.py`.
- **uv run --directory <ubicacion>**: Usa para tests que necesitan tanto el .venv como su propio contexto (funcionalidades de pytest). Ejemplo: `uv run --directory dagster-global-gf pytest`.
- **uv run**: Para scripts en la raíz que necesitan contexto del proyecto raíz. Ejemplo: `uv run scripts/run_all_tests.py`.

## 4. Manejo de Secretos en Tests

## 1. Arquitectura de Entornos
Cada ubicación de código (code location) tiene ahora su propio entorno virtual aislado:
- `dagster-global-gf/.venv`
- `dagster-kielsa-gf/.venv`
- `dagster-sap-gf/.venv`

La librería compartida `dagster-shared-gf` también tiene su propio .venv ya que necesita multiples paquetes para las pruebas.

La resolución y sincronización se hace usando `uv sync` con la variable `UV_PROJECT_ENVIRONMENT` apuntando al directorio `.venv` de cada location para garantizar:
- Instalación determinista basada en `pyproject.toml` + `uv.lock` raíz.
- Eliminación (prune) automática de dependencias obsoletas.

## 2. Creación / Actualización de entornos
Ejecutar:
```bash
bash scripts/deployment.sh install-deps --local
```
Esto recorrerá las ubicaciones configuradas y llamará internamente a `uv sync --frozen` para cada una. Puedes re‑ejecutarlo de forma idempotente tras cambios en dependencias.

## 3. Ejecución de Tests por Location
Ejemplo rápido (desde la raíz del repo), UV_PROJECT_ENVIRONMENT no es necesario:
```bash
for loc in dagster-global-gf dagster-kielsa-gf dagster-sap-gf; do 
  echo "== PYTEST $loc ==";
  (cd $loc; UV_PROJECT_ENVIRONMENT="$(pwd)/.venv" uv run --frozen pytest -q);
done
```

Ejecutar sólo un subconjunto (patrón):
```bash
cd dagster-kielsa-gf
UV_PROJECT_ENVIRONMENT="$(pwd)/.venv" uv run --frozen pytest -k test_jobs -q
```

Ejecutar todos (acepta los args de pytest):
`scripts/run_all_tests.py`

Notas:
- Se usa `uv run --frozen` para respetar el lock file sin recalcular resolución.
- Si necesitas recalcular .lock tras editar `pyproject.toml`, ejecuta manualmente `uv sync` en el workdir corrrespondiente.

## 4. Manejo de Secretos en Tests
Muchos módulos acceden a `dlt.secrets[...]` en tiempo de importación (pattern anti‑ideal en test). Para evitar fallos:
- Se añadieron `conftest.py` en las ubicaciones que cargan valores dummy a partir de los archivos de muestra:
  - `.dlt/secrets.toml.sample`
  - `.env.sample`
- Los valores sólo se aplican si la variable NO está ya definida (no sobreescribe secretos reales en CI).

### Cómo agregar nuevas claves
1. Añade la clave en el archivo de ejemplo correspondiente (`.dlt/secrets.toml.sample` o `.env.sample`).
2. Asegúrate de usar la jerarquía TOML adecuada para que se genere el nombre de variable correcto (las secciones se convierten a MAYÚSCULAS separadas por `__`).
3. Si la clave es sensible, NO la pongas en claro en `.env.sample` (usa `NOT-SET`) y provée el valor real sólo en entornos seguros.

### Validación manual de claves cargadas
Puedes inspeccionar qué variables dummy están disponibles:
```bash
uv run --no-sync env | grep MDB_CRM_HN || true
```

## 5. Añadir / Modificar Tests
- Ubica los tests dentro del paquete de cada location usando la convención `<paquete>_tests/` (ya existente).
- Evita que nuevos tests importen módulos que requieran secretos reales a nivel de módulo; si es inevitable, documenta la clave en los archivos sample.

## 6. Buenas Prácticas
- Mantén los *extras* de `dagster-shared-gf` minimalistas y sin duplicar dependencias ya listadas en `dependencies` core de ese paquete.
- Antes de un commit grande de dependencia, corre el loop de tests completo.
- Usa `pytest -q` para feedback rápido y `pytest -vv` sólo si necesitas detalle.
- Si ves advertencias de Pydantic/Dagster en masa, evalúa silenciarlas selectivamente con filtros en `pytest.ini` (pendiente de implementar si el ruido aumenta).

## 7. Troubleshooting
| Problema | Causa común | Solución |
|----------|-------------|----------|
| `ConfigFieldMissingException` vuelve a aparecer | Nueva clave no definida en samples | Añadir clave al sample o exportarla antes de correr tests |
| Paquetes antiguos permanecen | No se usó `uv sync` con `UV_PROJECT_ENVIRONMENT` | Reejecutar `bash scripts/deployment.sh install-deps --local` |
| Dependencias faltantes | Extras no incluidos en el `pyproject.toml` de la location | Agregar el extra requerido y volver a sincronizar |
| Advertencias masivas Pydantic | Cambios internos v2 | Añadir filtros en `pytest.ini` (opcional) |

## 8. Próximos Pasos (Opcionales)
- Añadir configuración pytests con filtros de warnings críticos.
- Script `scripts/run_location_tests.sh` para encapsular el bucle (si se desea).
- Fixture que falle si una variable crítica queda con valor placeholder en CI.

---
Última actualización: automatizada en migración a entornos multi‑location.
