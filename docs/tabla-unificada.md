# Mesa de trabajo de laboratorio

`lab_pg.py` mantiene el flujo de procesos y muestra una sola tabla operativa.
Usa las mismas credenciales de Sheets/S3 y la misma entrada de Streamlit.

## Operación

1. Busca por folio, doctor, paciente o aparato. Puedes filtrar por responsable,
   semáforo, aparato, etapa, vendedor y pago, u ordenar por urgencia.
2. Edita las celdas habilitadas. **Guardar cambios** valida todos los pedidos
   modificados antes de comenzar las escrituras. Cada usuario conserva sus
   permisos de transición; Admin puede corregir los datos generales.
3. La columna **Abrir** selecciona pedidos sin guardarlos ni modificar Sheets.
   Con un pedido seleccionado se muestran archivos, historial y las acciones
   de pago o diseño correspondientes. Con pedidos de impresión seleccionados,
   Admin/Lesly pueden marcarlos o avanzar por lote en la misma pantalla.
4. Nuevo pedido, Forms y consulta de procesos se despliegan dentro de la página.
5. **Actualizar datos** vuelve a leer los pedidos y calcular el semáforo.
   La hora de consulta es visible. Los filtros y las acciones quedan bloqueados
   mientras hay celdas pendientes, para no perder cambios sin guardarlos.

## Semáforo

| Color | Condición |
| --- | --- |
| Verde | Menos del 80% del plazo de la etapa |
| Amarillo | Desde 80% y antes de alcanzar el plazo |
| Rojo | Plazo agotado o alerta especial de pago atrasada |
| Gris | Sin inicio, sin plazo válido, folio duplicado o registro de tiempo inconsistente |

El detalle muestra el motivo del color. Se conservan las alertas especiales de
pagos para Tiger, Leone y Distalizador; se muestra la más urgente entre la alerta
de etapa y la alerta especial. Los plazos cuentan lunes a viernes, 24 horas por
día hábil, como el modelo existente; no representan un turno laboral de 8 horas.

El flujo y sus plazos siguen definidos en `PROCESS_CONFIG`. La tabla usa la
duración guardada en el registro activo de `TIEMPOS_APARATOS`. No se inventan
fechas iniciales para pedidos antiguos ni se escribe una columna de semáforo
en Google Sheets. Los registros nuevos respetan el orden real de sus encabezados.

## Pedidos visibles y guardado

- Se ocultan `ENVIADO` histórico, `ENVÍO DE ENCUESTA` y `CANCELO`.
- `PRODUCTO ENVIADO` sigue visible porque aún falta cerrar su encuesta.
- Se omiten folios reservados que no tienen datos de un pedido.
- Todos ven la tabla activa; sus permisos existentes controlan las escrituras.
- Los pagos y la impresión se capturan con sus acciones, no editando sus fechas
  directamente en la cuadrícula. Se conservan autorización de anticipo,
  comprobantes, fechas automáticas, archivos S3 y bitácora de transiciones.
- Se vuelven a leer los datos antes de guardar. Los conflictos detectados se
  rechazan, y se actualizan sólo las celdas modificadas, identificadas por folio.
- Sheets no ofrece una transacción entre las dos hojas. Una falla de red puede
  dejar parte de un lote guardado o el status guardado sin su bitácora; la app
  informa el fallo y recarga para revisar antes de reintentar.

## Validación

```sh
python -m pip install -r requirements.txt pytest
python -m pytest -q
python -m streamlit run lab_pg.py
```

Las pruebas usan datos ficticios, sin conexión a Sheets ni S3. Incluyen colores,
fines de semana, estados históricos, encabezados, permisos, pagos, impresión,
concurrencia y renderizado/guardado del editor para los cuatro usuarios.
La revisión visual en el despliegue real debe comprobar el desplazamiento,
las columnas fijas y los adjuntos con las credenciales de ese entorno.
