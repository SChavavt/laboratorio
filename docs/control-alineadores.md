# Control de alineadores

La app `alineadores_pg.py` administra la pestaña `ALINEADORES (nuevo)` del archivo
Google Sheets **Control ALINEADORES**. Es independiente de `lab_pg.py`, por lo que
la app actual de Control de aparatos conserva su configuración y comportamiento.

## Despliegue en Streamlit

1. Crear una app nueva desde este mismo repositorio y seleccionar
   `alineadores_pg.py` como archivo principal.
2. Copiar los mismos secretos de autenticación y la misma cuenta de servicio que
   utiliza `lab_pg.py`.
3. Compartir **Control ALINEADORES** con el `client_email` incluido en
   `google_credentials`, con permiso de editor.
4. Si se quiere reemplazar el archivo predeterminado, agregar una clave separada:

   ```toml
   [gsheets]
   alineadores_sheet_id = "1wNKD4bl__w1qMG182xFfu-1NlbcWTZdFEpa-h8ZLtik"
   ```

La clave existente `gsheets.sheet_id` sigue reservada para Control de aparatos y
la app de alineadores no la reutiliza.

## Funcionamiento

- `PROCESOS POR PRODUCTO` es la fuente de verdad para las etapas y plazos.
- `BORRADOR TITAN`, `SOLICITUD DE CAMBIOS` e `IMPRESIÓN EN PAUSA` son pausas
  opcionales; nunca forman parte obligatoria del flujo.
- Al salir de una pausa se puede elegir la etapa normal correcta para reanudar.
- `CANCELADO` está disponible en cualquier etapa activa. Los pedidos cancelados y
  enviados permanecen en la hoja, pero se ocultan de la mesa de trabajo.
- La app crea `TIEMPOS_ALINEADORES` en el primer arranque si todavía no existe y
  registra usuario, etapa, inicio, límite, fin, duración y comentario de cada cambio.
- Los pedidos existentes sin bitácora se muestran en gris. Su medición se inicia
  explícitamente desde la ficha del pedido para evitar inventar tiempos históricos.
- Los semáforos usan horas hábiles de lunes a viernes: verde debajo de 80 %, amarillo
  entre 80 % y 100 %, rojo al vencer y morado durante una pausa.
- No se asignan responsables por etapa; todos los usuarios pueden trabajar cualquier
  producto.
- En Seguimiento, sólo **No. Orden** y **Semáforo** quedan fijos y bloqueados. La
  casilla **Abrir** sigue siendo únicamente un control de selección. Las demás
  columnas provenientes de `ALINEADORES (nuevo)` son editables, incluidas las que
  se llenan automáticamente; los cálculos exclusivos de la vista permanecen
  automáticos.

## Pruebas

```bash
python -m pytest -q tests/test_alineadores_pg.py
```
