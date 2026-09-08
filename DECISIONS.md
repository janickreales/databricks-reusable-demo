# Decisiones de diseño

## Orden de joins para el cálculo de margen de rentabilidad (nación/año, partes 'green')

**Contexto:** 6 tablas TPC-H a SF5 (`lineitem` ~30M, `orders` 7.5M, `partsupp` 4M, `part` 1M,
`supplier` 50K, `nation` 25). Objetivo: minimizar el volumen de shuffle sobre `lineitem`,
la tabla más grande, al calcular el margen por nación y año filtrando `part.p_name LIKE '%green%'`.

**Decisión:** encadenar los joins en este orden:

```
part (filtrado 'green', broadcast)
   ⋈ partsupp (broadcast)
   ⋈ supplier (broadcast)
   ⋈ nation (broadcast)
        │
        ▼  (broadcast join: lineitem no se shufflea, solo se escanea)
   ⋈ lineitem
        │
        ▼
   ⋈ orders (broadcast si el tamaño lo permite, si no shuffle sobre el resultado ya reducido)
        │
        ▼
   groupBy(nation, year) → shuffle final, pero sobre datos ya agregados/reducidos
```

**Razonamiento:**

1. **Filtrar `part` primero.** `p_name LIKE '%green%'` es muy selectivo (~2-3% de 1M filas),
   reduce `part` a ~20-30K filas antes de tocar cualquier otra tabla.
2. **Encadenar las dimensiones pequeñas entre sí antes de `lineitem`.** `part` filtrado ⋈
   `partsupp` (broadcast, porque el lado chico es diminuto) ⋈ `supplier` (50K, broadcast trivial)
   ⋈ `nation` (25 filas, broadcast trivial). El resultado acumulado sigue siendo pequeño
   (~80-120K filas).
3. **Unir ese resultado compacto contra `lineitem`, no al revés.** Si el lado derecho cabe en
   el umbral de broadcast (ajustar `spark.sql.autoBroadcastJoinThreshold` o usar `broadcast()`
   explícito), `lineitem` nunca se shufflea: cada partición se escanea una sola vez contra la
   tabla hash broadcastada. Esto es lo que realmente minimiza el shuffle sobre la tabla de 30M
   filas — la clave es que el join con el lado grande sea *broadcast* y no *sort-merge*.
4. **Unir con `orders` después, sobre el `lineitem` ya reducido.** Tras el paso 3 el dataset
   ya no tiene 30M filas, solo las líneas de partes "green". Unir con `orders` (para obtener
   `o_orderdate`/año) es más barato porque el volumen a mover (broadcast o shuffle) es sobre
   el dataset reducido, no sobre `lineitem` original.
5. **La agregación final (`groupBy(nation, year)`) requiere shuffle**, pero ya es sobre datos
   reducidos — es inevitable y no es lo que se buscaba minimizar.

**Notas prácticas:**
- Proyectar solo las columnas necesarias de cada tabla antes de cada join (poda de columnas).
- Usar `broadcast()` explícito en vez de confiar en el optimizador, ya que a SF5 el tamaño
  filtrado de `part`/`partsupp` puede estar cerca del umbral por defecto (10MB).
- Activar AQE (`spark.sql.adaptive.enabled=true`) para reoptimizar el plan con estadísticas
  reales y manejar *skew* en `l_suppkey`/`l_partkey`.
- El orden en el código DataFrame no dicta necesariamente el plan físico (Catalyst puede
  reordenar joins con CBO), pero encadenar explícitamente en este orden hace el comportamiento
  predecible.

## Qué tablas forzar con `broadcast()` en Databricks Serverless (AQE habilitado)

**Contexto:** en Serverless no se puede ajustar memoria de ejecutores manualmente, y AQE está
habilitado por defecto. Un hint de `broadcast()` equivocado sobre una tabla más grande de lo
esperado puede causar OOM sin posibilidad de recuperarlo subiendo memoria del clúster.

**Decisión:**

| Tabla | ¿Forzar `broadcast()`? | Razón |
|---|---|---|
| `nation` (25 filas) | Sí | Tamaño trivial y estable, cero riesgo. |
| `supplier` (50K filas) | Sí | Muy por debajo de cualquier umbral razonable, riesgo nulo. |
| `part` filtrado por `'green'` (~20-30K filas) | Sí, sobre el DataFrame ya filtrado, nunca sobre `part` completo (1M filas) | El filtro es determinístico y lo controla el código, no depende de estadísticas de la tabla. |
| `partsupp` sin filtrar (4M filas) | No | Demasiado grande para forzar de forma segura entre scale factors; dejar que AQE convierta a broadcast el resultado ya reducido (`part` filtrado ⋈ `partsupp`, ~80-120K filas). |
| `orders` (7.5M filas) | No | Demasiado grande para un hint estático seguro; dejar a criterio de AQE según el tamaño real tras la reducción previa. |
| `lineitem` (30M filas) | Nunca | Es la tabla objetivo del shuffle a evitar, siempre debe ser el lado grande del broadcast join, no el lado que se broadcastea. |

**Razonamiento:**

1. AQE solo convierte un sort-merge join a broadcast *después* de ejecutar un shuffle y medir
   el tamaño real; para el primer intento de plan, sin hint, ese shuffle puede ejecutarse igual.
   Un hint explícito en tablas realmente pequeñas evita ese shuffle desde el plan inicial.
2. Las estadísticas de `samples.tpch` pueden no estar actualizadas (`ANALYZE TABLE` no
   necesariamente corrido), por lo que el optimizador basado en costos puede estimar mal el
   tamaño de tablas cuyo valor real ya se conoce de antemano (`nation`, `supplier`).
3. `broadcast()` es un hint fuerte que bypassea `spark.sql.autoBroadcastJoinThreshold`; en
   Serverless, sin control de memoria de ejecutores, es más seguro reservarlo solo para tablas
   con tamaño garantizado por lógica de negocio y dejar que AQE decida dinámicamente sobre
   tablas cuyo tamaño post-filtro depende de los datos (`partsupp` unido, `orders`).

## Dónde aplicar el filtro `p_name LIKE '%green%'` sobre `part`

**Contexto:** todos los joins de la pipeline son `INNER JOIN`, y Catalyst empuja predicados a
través de joins internos (`PushPredicateThroughJoin`) antes de la planificación física, así que
escribir `.filter()` antes o después del join produce el mismo plan físico en el caso normal.

**Decisión:** aplicar el filtro explícitamente con `.filter()` justo después de `read_part()`,
antes de cualquier join, en vez de confiar en el pushdown automático.

**Razonamiento:** el pushdown automático no es fiable en varios casos que sí pueden ocurrir en
este proyecto:

1. **Joins externos.** Si `part` pasa a ser el lado "productor de nulls" de un
   `LEFT`/`RIGHT`/`FULL OUTER JOIN` (por un cambio futuro), Catalyst no puede empujar el filtro
   sin alterar la semántica del resultado. Filtrar antes del join es la única forma de garantizar
   el resultado esperado, no solo una optimización.
2. **UDFs o expresiones no determinísticas.** Si el filtro se envuelve en un UDF de Python o usa
   funciones como `rand()`/`current_date()`, Catalyst lo trata como caja negra y no lo empuja a
   través de joins ni hacia el scan.
3. **`.cache()`/`.persist()` entre la lectura y el join.** Si se cachea `part` sin filtrar, el
   cache materializa las 1M de filas completas; filtrar antes de cachear reduce el cache a
   ~20-30K filas.
4. **Cross joins accidentales** (join sin condición de igualdad reconocible). Spark puede
   materializar el producto cartesiano completo antes de aplicar un filtro posterior, en vez de
   convertirlo en un hash join.

Aplicar el filtro explícito temprano no cambia el rendimiento en el caso feliz, pero documenta
la intención, es inmune a estos casos y hace el comportamiento predecible independientemente de
cambios futuros en el tipo de join.

## Particionado de la tabla Delta de resultado (margen por nación/año)

**Contexto:** el resultado final tiene ~25 naciones × unos pocos años, es decir ~100-150 filas
en total (una fila por combinación nación/año tras el `groupBy`).

**Decisión:** no particionar la tabla de salida por `(nation, year)`. Escribirla como una única
tabla Delta sin particionar (por ejemplo forzando `coalesce(1)` antes de escribir).

**Razonamiento:** particionar por columnas con ~150 combinaciones distintas sobre una tabla de
~150 filas produce una explosión de archivos pequeños, no un beneficio de rendimiento:

1. Cada partición terminaría con literalmente 1 fila, es decir un archivo Parquet de pocos
   bytes/KB por partición; el overhead de abrir cada archivo (footer, schema, estadísticas
   min/max) supera el tamaño real del dato que contiene.
2. La escritura reparte el DataFrame según `spark.sql.shuffle.partitions`, así que particionar
   por columnas puede generar muchos más archivos diminutos de los 150 esperados (varios por
   partición en vez de uno).
3. Cada archivo, sin importar su tamaño, genera una entrada en `_delta_log`; con cientos de
   archivos minúsculos el log crece desproporcionadamente respecto al tamaño real de los datos,
   ralentizando lecturas, *time travel* y `OPTIMIZE`/`VACUUM`.
4. No hay beneficio de *partition pruning* que lo compense: la tabla completa ya pesa unos pocos
   KB, un *full scan* sin particionar es instantáneo, y el *data skipping* automático de Delta
   (estadísticas min/max por archivo) logra el mismo filtrado sin carpetas de partición.

Regla general: particionar solo aporta valor cuando cada partición individual pesa al menos
decenas de MB a GB; muy por debajo de eso, el overhead de metadata y archivos pequeños supera
cualquier beneficio.
