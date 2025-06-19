#!/bin/bash

set -e

# Ajustar permisos para .ssh
if [ -d "/home/vscode/.ssh" ]; then
  echo "Ajustando permisos en .ssh..."
  sudo chown -R vscode:vscode /home/vscode/.ssh
  sudo chmod 700 /home/vscode/.ssh
  sudo find /home/vscode/.ssh -type f -name "id_*" -exec chmod 600 {} \;
  sudo find /home/vscode/.ssh -type f -name "*_key" -exec chmod 600 {} \;
  sudo find /home/vscode/.ssh -type f -name "*.pub" -exec chmod 644 {} \;
fi

# Ajustar permisos para /home/vscode/
if [ -d "/home/vscode/" ]; then
  echo "Ajustando permisos en /home/vscode/..."
  sudo chown -R vscode:vscode /home/vscode/
fi

if [ -z "$UV_PROJECT_ENVIRONMENT" ]; then
  echo "Error: UV_PROJECT_ENVIRONMENT no está definido. Por favor, especifícalo en tu devcontainer."
  exit 1
fi

# Ajustar permisos para /home/vscode/
if [ -d "$UV_PROJECT_ENVIRONMENT" ]; then
  echo "Ajustando permisos en $UV_PROJECT_ENVIRONMENT..."
  sudo chown -R vscode:vscode $UV_PROJECT_ENVIRONMENT
fi

echo "Permisos ajustados correctamente."


echo 'eval "$(uv generate-shell-completion bash)"' >> ~/.bashrc
echo 'eval "$(uvx --generate-shell-completion bash)"' >> ~/.bashrc

# ——————————————————————————————
# Inicio del setup de entorno de desarrollo
# ——————————————————————————————

# Asegurarse de que DAGSTER_HOME venga de .env
if [ -z "$DAGSTER_HOME" ]; then
  echo "Error: DAGSTER_HOME no está definido. Por favor, especifícalo en tu .env."
  exit 1
fi
if [ -z "$VIRTUAL_ENV" ]; then
  echo "Error: VIRTUAL_ENV no está definido. Por favor, especifícalo en tu .env o deploy."
  exit 1
fi

echo "🚀 Iniciando setup del entorno de desarrollo..."
echo "DAGSTER_HOME = $DAGSTER_HOME"
echo "VIRTUAL_ENV = $VIRTUAL_ENV"

# Instalar uv si falta
if ! command -v uv &> /dev/null; then
  echo "Instalando uv standalone..."
  mkdir -p ~/.local/bin
  curl -LsSf https://astral.sh/uv/install.sh | sh
  # Actualizar el PATH para la sesión actual
  export PATH="$HOME/.local/bin:$PATH"
else
  echo "uv ya está instalado, omitiendo."
fi

# Instalar dependencias core
echo "📥 Instalando dependencias core con uv..."
uv sync
echo "✅ Dependencias core instaladas."

# Actualizar path de binarios python
if [[ ":$PATH:" != *":$VIRTUAL_ENV/bin:"* ]]; then
  export PATH="$VIRTUAL_ENV/bin:$PATH"
fi

# Activar el virtualenv
source $VIRTUAL_ENV/bin/activate

# Configurar claves SSH de deploy para repos privados
uv run --frozen ./scripts/deployment.py setup-deploy-key --test

# Instalar dependencias externas
echo "📥 Instalando dependencias externas con uv..."
uv sync --extra external --inexact --locked
echo "✅ Dependencias externas instaladas."

# Configurar Git (commit template + pre-commit)
echo "⚙️ Configurando entorno Git y pre-commit..."
TEMPLATE_PATH="$DAGSTER_HOME/templates/commit-template.git.txt"
if [ -f "$TEMPLATE_PATH" ]; then
  git config commit.template "$TEMPLATE_PATH" || echo "⚠️ No se pudo setear commit.template"
  echo "✅ Commit template establecido."
else
  echo "⚠️ No se encontró $TEMPLATE_PATH, omitiendo."
fi

if ! uv pip show pre-commit &> /dev/null; then
  echo "Instalando pre-commit via uv..."
  uv run --frozen pre-commit install --install-hooks
fi

echo "🎉 Entorno de desarrollo preparado correctamente."