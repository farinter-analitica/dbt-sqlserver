# Usa la última imagen oficial de Python 3.12 como base
FROM mcr.microsoft.com/devcontainers/python:3.12 AS base
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Instala dependencias del sistema necesarias
# RUN apt-get update && \
#     apt-get install -y --no-install-recommends \
#         build-essential \
#         openssh-client \
#         && apt-get clean \
#         && rm -rf /var/lib/apt/lists/* 

# Crea el usuario vscode para desarrollo (como en devcontainer)
# RUN useradd -ms /bin/bash vscode

# Ensure workspace is owned by vscode
# RUN mkdir -p /workspace && chown -R vscode:vscode /workspace

# Establece el usuario por defecto
USER vscode

# Expone el puerto

EXPOSE 3000

# Crea el directorio de trabajo

# WORKDIR /workspace
# CMD ["dagster", "dev", "-h", "0.0.0.0"]