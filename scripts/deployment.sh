#!/usr/bin/env bash

# =============================================================================
# Bash script para instalar uv y dependencias de Python
# (Simplificado para aprovechar la gestión automática de uv)
# =============================================================================

set -euo pipefail

# -------------------- Variables y constantes --------------------
if [ -f .env ]; then
    echo "Loading environment variables from .env file..."
    set -a
    source .env
    set +a
fi

IS_LOCAL="false"
CARGO_BIN_DIR="$HOME/.local/bin"
CARGO_UV_BIN="${CARGO_BIN_DIR}/uv"

# -------------------- Funciones auxiliares ----------------------

error_exit() {
    echo "Error: $1" >&2
    exit 1
}

check_vars() {
    if [[ -z "${ENV:-}" ]]; then
        error_exit "ENV must be explicitly provided (e.g., ENV=value)."
    fi
}

check_os() {
    if [[ "$(uname)" != "Linux" ]]; then
        error_exit "Este script solo soporta Linux para despliegue."
    fi
}

install_uv_standalone() {
    local reinstall="${1:-false}"

    if [[ "$reinstall" == "true" ]]; then
        if [ -f "$CARGO_UV_BIN" ]; then
            echo "Desinstalando uv..."
            rm -f "$CARGO_UV_BIN"
        fi
    elif command -v uv >/dev/null 2>&1; then
        echo "uv ya está instalado, omitiendo instalación."
        return 0
    fi

    echo "Instalando uv..."
    mkdir -p "$CARGO_BIN_DIR"

    if [[ "$(uname)" == "Linux" ]]; then
        bash -c "curl -LsSf https://astral.sh/uv/install.sh | sh"
        if ! echo "$PATH" | grep -q "$CARGO_BIN_DIR"; then
            export PATH="$CARGO_BIN_DIR:$PATH"
        fi
    else
        error_exit "Este script solo soporta Linux para despliegue."
    fi
}

install_deps_uv() {
    local is_local="${1:-false}"
    unset VIRTUAL_ENV
    echo "Instalando dependencias principales..."
    if [ "$is_local" != "true" ]; then
        uv sync --locked --no-dev --inexact
        # Configurar claves SSH de deploy para repos privados
        uv run --frozen ./scripts/deployment.py setup-deploy-key
        echo "Instalando dependencias externas..."
        uv sync --extra external --inexact --locked || \
            echo "⚠️ Falló la instalación de dependencias externas, el sistema continuará."
    else
        uv sync
        # Configurar claves SSH de deploy para repos privados sin preguntas
        uv run --frozen ./scripts/deployment.py setup-deploy-key --test --dev
        echo "Instalando dependencias externas..."
        uv sync --extra external --inexact || \
            echo "⚠️ Falló la instalación de dependencias externas, el sistema continuará."
    fi

    # Sincronizar entornos por code location (.venv individuales) reutilizando la misma lógica de uv sync
    echo "Creando/actualizando entornos por code location (mismo flujo que root)..."
    for loc in dagster-global-gf dagster-kielsa-gf dagster-sap-gf; do
        echo "[$loc] Instalando dependencias..."
        (
            cd "$loc";
            if [ "$is_local" != "true" ]; then
                UV_PROJECT_ENVIRONMENT="$(pwd)/.venv" uv sync --locked --no-dev --inexact
                UV_PROJECT_ENVIRONMENT="$(pwd)/.venv" uv sync --extra external --inexact --locked || echo "⚠️ Falló external en $loc (continuando)"
            else
                UV_PROJECT_ENVIRONMENT="$(pwd)/.venv" uv sync --dev
                UV_PROJECT_ENVIRONMENT="$(pwd)/.venv" uv sync --extra external --inexact || echo "⚠️ Falló external en $loc (continuando)"
            fi
            echo "[$loc] ✅ Sync completado"
        )
    done
    (
        cd dagster-shared-gf;
        if [ "$is_local" == "true" ]; then
            UV_PROJECT_ENVIRONMENT="$(pwd)/.venv" uv sync --dev --all-extras --no-extra external
            UV_PROJECT_ENVIRONMENT="$(pwd)/.venv" uv sync --extra external --inexact || echo "⚠️ Falló external en dagster-shared-gf (continuando)"
        fi
        echo "dagster-shared-gf ✅ Sync completado"
    )
    echo "✅ Entornos por location sincronizados"
    unset UV_PROJECT_ENVIRONMENT
}

manage_services() {
    local env="$1"
    local action="${2:-restart}"
    
    echo "Managing services: $action"
    uv run --frozen ./scripts/deployment.py manage-services --env "$env" --action "$action"
}

generate_service() {
    echo "Generando template de servicio..."
    CMD_PATH="$(realpath ./scripts/generate_dagster_service.py)"
    sudo "$CARGO_UV_BIN" run --frozen "$CMD_PATH"
}

run_dagster_instance_migrate() {
    echo "Ejecutando 'dagster instance migrate'..."
    uv run --frozen dagster instance migrate
}

reload_code_locations() {
    echo "Recargando code locations..."
    uv run --frozen ./scripts/deployment.py reload-code-locations
}

procesar_archivos_dbt() {
    echo "Procesando archivos dbt..."
    uv run --frozen ./dbt_dwh_farinter/autoload_dbt_deploy.py
}

# -------------------- Funciones de despliegue --------------------

deploy_continuous() {
    echo "Performing continuous deployment (code changes only)..."
    check_vars
    check_os
    install_uv_standalone
    procesar_archivos_dbt
    reload_code_locations
    echo "✅ Continuous deployment completed."
}

deploy_fast() {
    echo "Performing fast deployment (dependencies only)..."
    check_vars
    check_os
    install_uv_standalone
    install_deps_uv "$IS_LOCAL"
    run_dagster_instance_migrate
    procesar_archivos_dbt
    reload_code_locations
    echo "✅ Fast deployment completed."
}

deploy_partial() {
    echo "Performing partial deployment (Python + dependencies)..."
    check_vars
    check_os
    
    install_uv_standalone "true"  # reinstall=true
    manage_services "$ENV" "stop"
    install_deps_uv "$IS_LOCAL"
    run_dagster_instance_migrate
    procesar_archivos_dbt
    manage_services "$ENV" "restart"
    echo "✅ Partial deployment completed."
}

deploy_full() {
    echo "Performing full deployment (service template, Python, and dependencies)..."
    check_vars
    check_os
    
    install_uv_standalone "true"  # reinstall=true
    manage_services "$ENV" "stop"
    install_deps_uv "$IS_LOCAL"
    generate_service
    run_dagster_instance_migrate
    procesar_archivos_dbt
    manage_services "$ENV" "restart"
    echo "✅ Full deployment completed."
}

# -------------------- Lógica principal --------------------------

usage() {
    cat << EOF
Usage: $0 COMMAND [OPTIONS]

Commands:
    deploy-full         Perform full deployment (service template, Python, and dependencies)
    deploy-partial      Perform partial deployment (Python + dependencies)
    deploy-fast         Perform fast deployment (dependencies only)
    deploy-continuous   Perform continuous deployment (code changes only)
    check-deploy-key    Check if deploy key exists
    setup-deploy-key    Setup deploy key
    test-deploy-key     Test deploy key
    reload-code-locations  Reload Dagster code locations

Deploy key options:
    --repo REPO         Repository name for deploy keys
    --org ORG           GitHub organization for deploy keys
    --force             Force creation of new deploy key
    --test              Test the deploy key

Environment variables:
    ENV                 Environment (dev, prd)
EOF
}

main() {
    if [[ $# -eq 0 ]]; then
        usage
        exit 1
    fi

    # Detectar y eliminar --local de los argumentos
    for i in "$@"; do
        if [[ "$i" == "--local" ]]; then
            IS_LOCAL="true"
            # Elimina --local de los argumentos
            set -- "${@/--local/}"
            break
        fi
    done    
    
    local command="$1"
    shift

    case "$command" in
        deploy-full)
            deploy_full
            ;;
        deploy-partial)
            deploy_partial
            ;;
        deploy-fast)
            deploy_fast
            ;;
        deploy-continuous)
            deploy_continuous
            ;;
        check-deploy-key|setup-deploy-key|test-deploy-key|reload-code-locations)
            install_uv_standalone
            uv run --frozen ./scripts/deployment.py "$command" "$@"
            ;;
        install-deps)
            install_uv_standalone
            install_deps_uv "$IS_LOCAL"
            ;;
        --help|-h)
            usage
            ;;
        *)
            echo "Error: Unknown command '$command'"
            usage
            exit 1
            ;;
    esac
}

main "$@"
