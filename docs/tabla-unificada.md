# Mesa de trabajo de laboratorio

`lab_pg.py` mantiene el flujo de procesos y muestra una sola tabla operativa en
**Seguimiento**, seguida de **Nuevo pedido**, **Respuestas de Forms** y
**Procesos y plazos**, en ese orden. Se respetan los permisos de cada usuario:
Admin tiene las cuatro pestañas, Jime las primeras tres y Estefano/Lesly/Vero Seguimiento.
Usa las mismas credenciales de Sheets/S3 y la misma entrada de Streamlit.
Las contraseñas configuradas en `secrets` se respetan; los accesos heredados ya
no aparecen en texto legible dentro del repositorio público.

## Operación

1. Busca por folio, doctor, paciente o aparato. Puedes filtrar por responsable,
   semáforo, aparato, etapa, vendedor y pago, u ordenar por urgencia.
2. Edita las celdas habilitadas. **Guardar cambios** valida todos los pedidos
   modificados antes de comenzar las escrituras. Cada usuario conserva sus
   permisos de transición; Admin puede corregir los datos generales.
   El selector de etapa se calcula por pedido: incluye la etapa actual y sólo
   los destinos inmediatos admitidos por ese aparato y ese usuario. Las fechas
   editables abren calendario y, cuando corresponde, hora; **Ahora** registra
   el momento de Ciudad de México y los valores antiguos se conservan al cancelar.
3. La columna **Abrir** selecciona pedidos sin guardarlos ni modificar Sheets.
   Con un pedido seleccionado se muestran archivos, historial y las acciones
   de pago o diseño correspondientes. Con pedidos de impresión seleccionados,
   Admin/Lesly pueden marcarlos o avanzar por lote en la misma pantalla.
4. Nuevo pedido, Forms y consulta de procesos tienen sus propias pestañas.
   Las etapas de los pedidos permanecen juntas en Seguimiento.
5. **Actualizar datos** vuelve a leer los pedidos y calcular el semáforo.
   La hora de consulta es visible. Los filtros y las acciones quedan bloqueados
   mientras hay celdas pendientes, para no perder cambios sin guardarlos.

La interfaz usa acentos morados y turquesa, contadores con los colores del
semáforo y las etiquetas originales con emojis en las etapas y desplegables.
Los emojis son sólo presentación: Sheets recibe siempre el valor canónico.
Las fichas de pedido también muestran la etapa con su color configurado.
El menú de etapa pinta cada opción con el mismo color de su celda. Los encabezados
identifican columnas editables, de solo lectura y automáticas; estas dos últimas
categorías se pueden ocultar de forma independiente.

Las columnas no fijas se pueden arrastrar desde su encabezado. **Guardar orden**
se activa después de mover una columna y persiste el acomodo por usuario en
`PREFERENCIAS APP`, dentro del mismo archivo de Google Sheets configurado para
la app, por lo que se recupera en otra sesión o equipo. El historial de cada
pedido ya muestra `USUARIO`, que se registra en `TIEMPOS_APARATOS` al cambiar de
etapa.

Todos los usuarios autenticados pueden corregir los campos manuales (doctor,
paciente, comentarios, vendedor, servicio, archivos y fecha de recepción). El
folio, el aparato y los campos calculados o autollenados permanecen protegidos.

La edición se ejecuta en un fragmento de Streamlit y conserva la misma clave,
posición y tamaño del editor. La barra de cambios ocupa siempre 40 px; el mensaje
de pendientes ya no inserta un bloque encima de la tabla. Las confirmaciones de
guardado se muestran como avisos flotantes y los efectos visuales son de color
y sombra al pasar el cursor, sin animaciones que desplacen las filas.
La cuadrícula conserva su posición al editar y la navegación por filtros sólo se
bloquea cuando hay cambios reales. El botón Guardar deshabilitado mantiene texto
oscuro sobre morado claro para que su estado siga siendo legible.

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
concurrencia y renderizado/guardado del editor para los cinco usuarios.
La revisión visual en el despliegue real debe comprobar el desplazamiento,
las columnas fijas y los adjuntos con las credenciales de ese entorno.
