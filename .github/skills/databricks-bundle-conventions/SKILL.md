---
name: databricks-bundle-conventions
description: Usar al revisar o escribir databricks.yml u otras definiciones de recursos de Databricks Asset Bundles. Detecta zona horaria faltante en schedules, valores hardcodeados específicos de un ambiente, y falta de overrides por target necesarios para portabilidad entre dev, staging y prod.
---

# Convenciones para Databricks Asset Bundles

## Zona horaria explícita
Todo Job con `schedule` (trigger programado) debe declarar `timezone_id` explícitamente. Databricks corre en UTC por defecto si se omite — señala cualquier bloque `schedule` sin `timezone_id`, porque es una causa común de jobs que corren a la hora equivocada sin que nadie lo note hasta que ya causó un problema.

## Portabilidad entre ambientes (dev/staging/prod)
- Señala nombres de catálogo, esquema, o rutas de workspace **hardcodeados** dentro de `resources:` — deben venir de `variables:` del bundle, sustituidos por `targets:`.
- Cada bloque bajo `targets:` (dev/staging/prod) debe sobrescribir, como mínimo: el catálogo/esquema de destino, y el modo de compute.
- Señala rutas de notebook (`notebook_path`) que no sean relativas a la raíz del bundle.
- Si dos personas pueden desplegar su propio bundle de desarrollo simultáneamente, sugiere usar `${bundle.target}` en la nomenclatura de recursos.