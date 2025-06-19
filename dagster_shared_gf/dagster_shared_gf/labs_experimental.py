import polars as pl
import numpy as np


def compute_eta_squared(
    df: pl.DataFrame, variable: str, value_col: str, media_alias: str
) -> float | None:
    """
    Calcula el eta cuadrado (correlación ratio) para una variable categórica en un DataFrame de Polars.
    """
    n_observaciones = df.filter(pl.col(variable).is_not_null()).height
    n_categorias = df[variable].n_unique()
    if n_categorias <= 1 or n_observaciones == 0:
        return None

    # Agregamos por la variable categórica: media y conteo de la columna objetivo
    stats_por_categoria = df.group_by(variable).agg(
        [
            pl.mean(value_col).alias(media_alias),
            pl.count(value_col).alias("n_observaciones"),
        ]
    )

    varianza_total = df.select(pl.var(value_col)).item()
    if varianza_total is None or varianza_total == 0:
        return None

    media_global = df.select(pl.mean(value_col)).item()

    suma_cuadrados_entre = 0
    for row in stats_por_categoria.to_dicts():
        if row[media_alias] is not None and row["n_observaciones"] > 0:
            suma_cuadrados_entre += (
                row["n_observaciones"] * (row[media_alias] - media_global) ** 2
            )

    eta_squared = suma_cuadrados_entre / (varianza_total * n_observaciones)
    return eta_squared


def bootstrap_eta_squared(
    df: pl.DataFrame,
    variable: str,
    value_col: str,
    media_alias: str,
    n_bootstrap: int = 1000,
):
    """
    Realiza bootstrap para estimar la distribución del eta cuadrado (correlación ratio)
    a partir de muestras con reemplazo.
    """
    boot_estimates = []

    for _ in range(n_bootstrap):
        # Tomar una muestra bootstrap del mismo tamaño que el original
        df_boot = df.sample(n=df.height, with_replacement=True)
        eta = compute_eta_squared(df_boot, variable, value_col, media_alias)
        boot_estimates.append(eta)

    return boot_estimates


# Ejemplo de uso:
# Supongamos que tenemos un DataFrame con una variable categórica 'grupo' y una variable numérica 'valor'.
data = {
    "grupo": ["A", "A", "B", "B", "C", "C", "C", "D", "D", "D"],
    "valor": [1, 1, 5, 5, 3, 3, 3, 8, 8, 8],
}
df = pl.DataFrame(data)

# Calculamos el eta cuadrado para la muestra original
eta_original = compute_eta_squared(df, "grupo", "valor", "media_valor")
print("Eta cuadrado original:", eta_original)

# Realizamos bootstrap
n_iter = 1000
boot_estimates = bootstrap_eta_squared(
    df, "grupo", "valor", "media_valor", n_bootstrap=n_iter
)

# Convertimos a array de NumPy para calcular estadísticas
boot_array = np.array([est for est in boot_estimates if est is not None])
mean_eta = np.nanmean(boot_array)
ci_lower = np.nanpercentile(boot_array, 2.5)
ci_upper = np.nanpercentile(boot_array, 97.5)

print(f"Bootstrap (n={n_iter}):")
print("Media eta cuadrado:", mean_eta)
print("IC 95%:", ci_lower, "-", ci_upper)
