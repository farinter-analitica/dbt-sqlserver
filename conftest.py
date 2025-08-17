"""conftest raíz mínimo.

Propósito: no inyectar otros entornos; cada code location debe ejecutar sus
tests con su propio intérprete/venv. Usa este archivo sólo para fixtures o
marcadores de integración opcionales.

Ejemplo (descomentar si necesitas marcar tests multi-location):

    def pytest_configure(config):
        config.addinivalue_line("markers", "integration: tests multi code location")
"""

# Placeholder intencional.
