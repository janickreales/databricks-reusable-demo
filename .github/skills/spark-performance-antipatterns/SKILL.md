---
name: spark-performance-antipatterns
description: Usar al revisar código PySpark en notebooks o módulos que hace joins, agregaciones o transformaciones de DataFrames. Detecta uso incorrecto de broadcast joins, acciones redundantes de Spark, y particionado inseguro de resultados pequeños.
---

# Rendimiento de Spark: anti-patrones a detectar

## Broadcast joins
- El lado que se broadcastea debe ser el DataFrame pequeño, nunca la tabla de hechos grande. Señala cualquier `broadcast()` aplicado al lado que tiene más filas en el join.
- Antes de confiar en que un DataFrame es "pequeño" por su conteo de filas, verifica que también se podaron las columnas innecesarias — el tamaño en bytes importa más que el conteo de filas para el umbral de broadcast (por defecto ~10MB).
- Si el tamaño del lado a broadcastear depende de un filtro previo (no es una tabla de dimensión fija), sugiere verificar el tamaño real antes de forzar el hint, en vez de asumirlo.

## Acciones redundantes
- Señala múltiples `.count()`, `.collect()` o `.show()` separados que escanean la misma fuente de datos. Sugiere calcular las métricas en una sola agregación condicional, o cachear explícitamente si se necesitan varias pasadas.
- Señala `.count()` o `.collect()` usados solo para depuración que quedaron commiteados en código de producción.

## UDFs vs. funciones nativas
- Señala cualquier UDF de Python que reemplace una función nativa de `pyspark.sql.functions` ya existente para la misma operación.

## Particionado de tablas Delta
- Señala `partitionBy()` sobre un resultado con pocas filas totales o muchas combinaciones distintas de baja cardinalidad — riesgo de archivos diminutos.
- Regla general: particionar solo aporta valor cuando cada partición individual pesa al menos varias decenas de MB.