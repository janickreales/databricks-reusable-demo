# Convenciones del proyecto

- Los notebooks van en formato Source (.py con `# COMMAND ----------`), no `.ipynb`.
- La lógica de transformación reutilizable vive en `src/`, no directamente en notebooks.
- Usar type hints en todas las funciones de `src/`.
- Docstrings estilo Google.
- Preferir archivos `.sql` independientes sobre SQL embebido en strings de Python cuando sea posible.
- Todo módulo en `src/` debe tener su prueba correspondiente en `tests/` (pytest).