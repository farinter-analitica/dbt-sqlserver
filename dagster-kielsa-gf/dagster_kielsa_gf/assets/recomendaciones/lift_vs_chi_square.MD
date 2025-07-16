## Puntuación Lift (Lift Score)

### Qué mide
La fuerza y dirección de asociación entre elementos

### Fórmula
lift(A,B) = P(A,B) / (P(A) * P(B))

### Interpretación
- Lift = 1: Los elementos aparecen juntos exactamente como se esperaría por azar
- Lift > 1: Asociación positiva (los elementos aparecen juntos con más frecuencia de lo esperado)
- Lift < 1: Asociación negativa (los elementos aparecen juntos con menos frecuencia de lo esperado)

### Fortaleza
Directamente interpretable como "cuántas veces más probable que por puro azar"

## Estadístico Chi-Cuadrado

### Qué mide
La significancia estadística de la asociación

### Fórmula
χ² = Σ[(O-E)²/E] donde O es la frecuencia observada y E es la frecuencia esperada

### Interpretación
Valores más altos indican evidencia más fuerte contra la hipótesis nula (que los elementos son independientes)

### Fortaleza
Tiene en cuenta el tamaño de la muestra y proporciona una medida formal de significancia estadística

## Diferencias Clave

### Dirección vs. Significancia
Lift muestra dirección y magnitud; chi-cuadrado muestra si el patrón es estadísticamente significativo

### Sensibilidad al Tamaño de Muestra
Chi-cuadrado considera inherentemente el tamaño de la muestra, lo que hace menos probable sobrevalorar asociaciones con poco respaldo

### Marco Estadístico
Chi-cuadrado proporciona un marco formal de prueba de hipótesis, mientras que lift es más una medida descriptiva

### Uso Complementario
Usar ambas métricas te da la imagen completa - lift te dice cuán fuerte es la asociación, mientras que chi-cuadrado te dice cuán seguro puedes estar de que la asociación no se debe al azar