---
name: notebook-documentation-standards
description: Usar al revisar o escribir notebooks de Databricks (formato Source .py o .ipynb). Verifica que cada notebook tenga un encabezado con propósito y responsable, y que las secciones principales estén documentadas con celdas markdown.
---

# Estándar de documentación de notebooks

## Celda de encabezado obligatoria
La primera celda de todo notebook debe ser markdown, e incluir:
- **Título** del notebook.
- **Propósito**: 1-2 frases de qué hace y por qué existe.
- **Owner**: persona o equipo responsable.
- **Inputs**: tablas o catálogos que lee (`catalog.schema.table`).
- **Outputs**: tablas que escribe, si aplica.
- **Job relacionado**: si este notebook se orquesta desde un Job, el nombre del Job.

Si falta esta celda, o le falta alguno de estos campos, señálalo.

## Documentación por sección
Cada bloque lógico distinto (carga de datos, validación, transformación, agregación, escritura) debe tener una celda markdown inmediatamente antes explicando qué hace esa sección y por qué. Señala 3 o más celdas de código consecutivas sin ninguna celda markdown intermedia.

## Consistencia entre documentación y código
Señala cuando el texto de una celda markdown describe algo distinto de lo que el código real hace inmediatamente después.