FROM mcr.microsoft.com/devcontainers/python:3.11-trixie

RUN rm -f /etc/apt/sources.list.d/yarn.list \
    /etc/apt/sources.list.d/yarn.sources \
    /usr/share/keyrings/yarnkey.gpg \
    /etc/apt/keyrings/yarn-archive-keyring.gpg

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
