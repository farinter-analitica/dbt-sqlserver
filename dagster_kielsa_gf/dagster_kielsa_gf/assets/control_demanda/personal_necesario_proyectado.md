# Documentación: Cálculo de Personal Recomendado para Cajas en Retail

**Fecha:** 8 de Abril de 2025
**Contexto:** Optimización de personal en puntos de cobro (cajas) en negocios de retail usando Teoría de Colas.

## 1. Introducción

Equilibrar el costo de personal con la satisfacción del cliente en las cajas es un desafío clave en retail. Colas largas y tiempos de espera excesivos pueden llevar a clientes frustrados y ventas perdidas. Por otro lado, tener demasiado personal incrementa los costos operativos innecesariamente.

Este documento describe un enfoque basado en la **Teoría de Colas**, específicamente el modelo **M/G/c**, para determinar de forma analítica el número recomendado de cajeros (`c`) necesarios para cumplir con objetivos de nivel de servicio predefinidos (tiempo máximo de espera y longitud máxima de cola).

## 2. Conceptos Clave de Teoría de Colas Aplicados a Retail

* **Tasa de Llegada (`λ` - lambda_h):** Número promedio de clientes que llegan a la zona de cajas por unidad de tiempo (ej. clientes por hora). Varía significativamente durante el día/semana.
* **Tiempo de Servicio (`Ts` - tiempo_s):** Tiempo promedio que toma atender a un cliente en caja (desde que inicia el servicio hasta que termina). Se expresa en la misma unidad de tiempo que λ (ej. horas por cliente). `μ` (mu) es la tasa de servicio (`μ = 1 / Ts`).
* **Variabilidad del Servicio (`Cv²` - cv_sq):** Mide cuánto varía el tiempo de servicio entre diferentes clientes. Un `Cv² = 1` indica variabilidad exponencial (típica en modelos simples). Un `Cv² > 1` (común en retail por diferencias en cantidad de artículos, métodos de pago, etc.) indica mayor variabilidad. Un `Cv² < 1` indica menor variabilidad (más consistencia).
* **Carga Ofrecida (`A` - carga):** Medida de cuánto trabajo "llega" al sistema. Se calcula como `A = λ * Ts`. Representa el número promedio de servidores que estarían ocupados si hubiera capacidad infinita. Se mide en Erlangs.
* **Número de Servidores (`c`):** Número de cajas abiertas y operativas. Es la variable que buscamos optimizar (`personal_recomendado`).
* **Utilización (`ρ` - rho):** Proporción del tiempo que un servidor (cajero) está ocupado en promedio. Se calcula como `ρ = A / c`. Debe ser `< 1` (o < 100%) para que el sistema sea estable.
* **Tiempo de Espera Promedio en Cola (`Wq` - tiempo_espera_promedio):** Tiempo promedio que un cliente pasa en la fila *antes* de iniciar el servicio en caja. Usualmente se mide en segundos o minutos.
* **Longitud Promedio de la Cola (`Lq` - longitud_cola_promedio):** Número promedio de clientes esperando en la fila.

## 3. El Modelo M/G/c en Retail

Este modelo es adecuado para cajas de retail porque:

* **M (Llegadas Markovianas/Poisson):** Las llegadas de clientes a las cajas suelen ser razonablemente aleatorias e independientes en intervalos cortos, ajustándose bien a un proceso de Poisson (`λ`).
* **G (Tiempo de Servicio General):** El tiempo para atender a cada cliente varía mucho (cantidad de artículos, preguntas, pagos, etc.) y no sigue necesariamente una distribución exponencial. El modelo M/G/c permite esta variabilidad general usando `Ts` y `Cv²`.
* **c (Servidores Múltiples):** Representa las múltiples cajas (`c`) que pueden estar operativas simultáneamente.

## 4. Lógica del Cálculo del Personal Recomendado

El objetivo es encontrar el **menor número de cajeros (`c`)** que garantice que las métricas de rendimiento (Wq y Lq) se mantengan por debajo de los umbrales deseados (`TIEMPO_ESPERA_MAX` y `COLA_MAX`). El proceso conceptual es:

1.  **Recolectar Datos de Entrada:** Medir o estimar `λ`, `Ts` y `Cv²` para el período de interés (ej. hora pico).
2.  **Calcular Carga (`A`):** `A = λ * Ts`.
3.  **Establecer Objetivos de Nivel de Servicio:** Definir los umbrales aceptables:
    * `TIEMPO_ESPERA_MAX`: Máximo tiempo *promedio* de espera en cola tolerado (ej. 120 segundos).
    * `COLA_MAX`: Máxima longitud *promedio* de cola tolerada (ej. 1.5 clientes).
4.  **Calcular Personal Mínimo (`c_min`):** Determinar el mínimo absoluto de cajeros para estabilidad: `c_min = ceil(A)`.
5.  **Evaluar Rendimiento vs. Personal:** Comenzando desde `c = c_min`, calcular iterativamente:
    * Probabilidad de Espera (`Pc`): Usando la fórmula de **Erlang-C**.
    * Tiempo de Espera Promedio (`Wq`): Usando la **aproximación de Allen-Cunneen** para M/G/c: `Wq ≈ Pc * ((Cv² + 1) / 2) * Ts / (c * (1 - A/c))`.
    * Longitud de Cola Promedio (`Lq`): Usando la **Ley de Little**: `Lq = λ * Wq`.
6.  **Encontrar Personal por Criterio:**
    * `c_tiempo`: Incrementar `c` hasta que `Wq <= TIEMPO_ESPERA_MAX`.
    * `c_cola`: Incrementar `c` (independientemente) hasta que `Lq <= COLA_MAX`.
7.  **Determinar Personal Recomendado (`c_recomendado`):** Seleccionar el máximo entre los dos valores anteriores para cumplir ambas condiciones: `c_recomendado = max(c_tiempo, c_cola)`.
8.  **Calcular Métricas Finales:** Calcular `Wq`, `Lq` y `Utilización` (`ρ = A / c_recomendado`) con el personal recomendado.

## 5. Configuraciones y Objetivos Recomendados para Retail

No hay números mágicos universales, ya que dependen de la estrategia del negocio, tipo de tienda (supermercado, conveniencia, departamental), y expectativas del cliente. Sin embargo, algunas guías comunes son:

* **Tiempo de Espera Promedio (`TIEMPO_ESPERA_MAX`):**
    * **Objetivo Común:** Muchos retailers apuntan a mantener el `Wq` **por debajo de 2-3 minutos (120-180 segundos)** en condiciones normales.
    * **Entornos Rápidos:** Tiendas de conveniencia o cajas express pueden apuntar a **menos de 60-90 segundos**.
    * **Impacto:** Tiempos mayores suelen correlacionarse con menor satisfacción y mayor probabilidad de abandono de compra.
    * **Alternativa:** A veces se usan objetivos percentiles (ej. "80% de los clientes esperan menos de 90 segundos"), que requieren análisis más detallados o simulaciones.

* **Longitud de Cola Promedio (`COLA_MAX`):**
    * **Impacto Visual:** Los clientes reaccionan a la longitud *visual* de la cola. Una regla de oro común es **evitar ver más de 2-3 clientes esperando *por cada caja abierta***.
    * **Traducción a `Lq`:** Para lograr esa percepción visual, el `Lq` (promedio numérico) necesita ser bastante bajo, quizás en el rango de **0.5 a 1.5 clientes**. Un `Lq` bajo asegura que *la mayor parte del tiempo*, las colas visibles sean cortas. Depende mucho del layout físico.

* **Utilización (`ρ`):**
    * **No es un Objetivo Directo:** La utilización es una *consecuencia* de dimensionar para Wq y Lq.
    * **Rango Típico:** Para lograr buenos tiempos de espera, la utilización promedio por cajero raramente supera el **80-90%**, y a menudo es menor en horas no pico. Una utilización cercana al 100% garantiza colas largas.

* **Variabilidad (`Cv²`):**
    * **Mitigación:** Aunque es una entrada, estrategias como lectores de código de barras eficientes, entrenamiento de cajeros, y zonas de empaque separadas pueden ayudar a *reducir* la variabilidad (`Cv²`) y, por ende, las esperas.

## 6. Fuentes y Referencias Conceptuales

La metodología descrita se basa en principios establecidos de:

* **Teoría de Colas (Queueing Theory):** Disciplina matemática que estudia las líneas de espera. Textos clásicos y modernos de investigación de operaciones cubren estas fórmulas (Erlang C, Little's Law, aproximaciones M/G/c).
    * Autores de referencia incluyen a Leonard Kleinrock, Donald Gross, Carl M. Harris, Richard W. Hall.
* **Gestión de Operaciones (Operations Management):** Aplica estos principios a contextos de negocio.
* **Gestión de Retail (Retail Management):** Estudios y mejores prácticas sobre la experiencia del cliente en tienda, incluyendo la gestión de cajas.
    * Autores como Levy & Weitz cubren aspectos operativos del retail.
* **Estudios de Industria y Consultoría:** Reportes específicos del sector retail a menudo analizan benchmarks de satisfacción y eficiencia operativa (pueden requerir suscripción o compra).
* **Software de Simulación y Workforce Management (WFM):** Herramientas especializadas que a menudo incorporan estos modelos (o más complejos) para la planificación de personal.

## 7. Limitaciones del Modelo

* **Estado Estacionario:** Asume que las tasas (`λ`, `Ts`) son constantes durante el período analizado. No maneja bien cambios muy bruscos (inicio/fin de promociones instantáneas). Requiere análisis por bloques de tiempo (ej. cada hora).
* **No considera "Balking" (No entrar a la cola) ni "Reneging" (Abandonar la cola).**
* **Comportamiento del Cliente:** No modela explícitamente la elección entre cajas o el impacto de cajas de autopago.
* **Precisión de Datos:** La calidad de la predicción depende fuertemente de la precisión de las entradas (`λ`, `Ts`, `Cv²`).
* **Eventos Externos:** No considera interrupciones (fallos de sistema, falta de cambio, etc.).

## 8. Conclusión

Utilizar el modelo M/G/c y las fórmulas de teoría de colas proporciona una base **analítica y objetiva** para tomar decisiones sobre el número de cajeros necesarios en un entorno retail. Permite cuantificar el impacto del personal en la experiencia del cliente (tiempo de espera, longitud de cola) y tomar decisiones informadas para balancear costos y satisfacción, adaptando los objetivos (`TIEMPO_ESPERA_MAX`, `COLA_MAX`) a la estrategia específica del negocio. Es una herramienta poderosa que, complementada con monitoreo real y ajustes, puede mejorar significativamente la eficiencia operativa.