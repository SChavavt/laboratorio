# Mesa de trabajo de laboratorio

`lab_pg.py` mantiene el flujo de procesos y muestra una sola tabla operativa en
**Seguimiento**, seguida de **Nuevo pedido** y **Recibidos de Forms**, en ese
orden. Se respetan los permisos de cada usuario: Admin, Jime y Lesly tienen las
tres pestañas y Vero, Seguimiento y **Confección y Calidad**.
Jime también atiende las etapas de planeación y diseño que antes correspondían a
Estefano; Estefano ya no aparece como usuario de acceso.
Usa las mismas credenciales de Sheets/S3 y la misma entrada de Streamlit.
Las credenciales de acceso se leen exclusivamente desde Streamlit Secrets; no
se publica ninguna contraseña ni hash en el repositorio.

## Configuración de acceso en Streamlit Cloud

Antes de desplegar esta versión, abre la app en Streamlit Cloud y entra a
**Settings → Secrets**. Conserva las secciones existentes de Google Sheets y
AWS, y agrega los cuatro hashes con esta estructura:

```toml
[auth.passwords]
Admin = "HASH_PBKDF2_DE_ADMIN"
Jime = "HASH_PBKDF2_DE_JIME"
Lesly = "HASH_PBKDF2_DE_LESLY"
Vero = "HASH_PBKDF2_DE_VERO"
```

Los valores deben ser los hashes PBKDF2 completos que ya usaba la aplicación.
Si falta alguno, el acceso muestra exactamente qué usuario falta configurar.
`Estefano` no debe agregarse porque dejó de ser usuario de la app.

## Operación

1. Busca por folio, doctor, paciente o aparato. Puedes filtrar por responsable,
   semáforo, aparato, etapa, vendedor y pago, u ordenar por urgencia.
2. Edita las celdas habilitadas. **Guardar cambios** valida todos los pedidos
   modificados antes de comenzar las escrituras. Para todos los usuarios, el
   selector muestra la etapa actual, la siguiente y las alternativas previstas
   para esa etapa en el flujo del aparato. También ofrece cancelar o pausar
   cuando el usuario tiene permiso. Se rechazan saltos a otras etapas sin haberlas
   elegido mediante la opción de cambio manual.
   `ESCANEO MAL (EN REPETICIÓN)` y `SOLICITUD DE CAMBIOS` son alternativas
   opcionales: se ofrecen junto con la siguiente etapa normal de la hoja. Así,
   si Revisión de archivos va seguida de Escaneo mal y Pago planeación, se puede
   elegir repetición o avanzar directamente a Pago planeación.
   Admin, Jime y Lesly también pueden pasar de **Revisión de archivos** a
   **En planeación** cuando esa etapa existe en el flujo del aparato, para
   iniciar la planeación de casos nuevos sin esperar las etapas de solicitud o pago.
   Para correcciones, esos tres usuarios disponen de **Ver todas las etapas…**
   dentro de **STATUS**, tanto en la tabla como en la ficha. En la tabla abre
   el segundo nivel del mismo menú y permite volver a las etapas sugeridas.
   En la ficha abre **Etapa del flujo** justo debajo de STATUS. Muestra el flujo
   completo del aparato, incluidas las etapas que no aparecen en la lista habitual;
   al cambiar el aparato, también cambian esas opciones.
   Abrir la lista no modifica el pedido. Se guarda la etapa elegida, junto con los
   demás campos editados, con **Guardar cambios** en la tabla o **Guardar este pedido**
   en la ficha. Se validan datos recientes, permisos y reglas de impresión, y los
   cambios manuales se distinguen en la bitácora. La ficha se bloquea mientras
   haya cambios pendientes en las tablas de Enviados y Pausados.
   Admin, Jime y Lesly conservan acceso a todas las áreas y su excepción de pagos;
   Vero conserva sus permisos limitados a sus propias transiciones. Reactivar
   pedidos pausados o enviados sigue siendo una acción del histórico. Las fechas
   editables abren calendario y, cuando corresponde, hora; **Ahora** registra
   el momento de Ciudad de México y los valores antiguos se conservan al cancelar.
   En **FECHA/HORA ENVÍO STEFANO** sólo se elige el día: la app registra la hora
   de Ciudad de México al guardar, sin pedirla manualmente.
3. La columna **Abrir** selecciona pedidos sin guardarlos ni modificar Sheets.
   Con un pedido seleccionado se muestran archivos, historial y las acciones
   de pago o diseño correspondientes. Con pedidos de impresión seleccionados,
   Admin/Lesly pueden marcarlos o avanzar por lote en la misma pantalla.
   Con un solo pedido, la etapa se cambia desde **STATUS** en su ficha y se
   confirma con **Guardar este pedido**. **Registrar impresión** aparece en esa
   ficha sólo para pedidos en `LISTO P/SINTERIZADO` sin impresión registrada y
   usuarios con permiso. **Impresión y avance por lote** aparece cerrado sólo al
   seleccionar varios pedidos de producción.
4. Nuevo pedido y Recibidos de Forms tienen sus propias pestañas.
   Las etapas de los pedidos permanecen juntas en Seguimiento.
5. **Actualizar datos** vuelve a leer los pedidos y calcular el semáforo.
   La hora de consulta es visible. Los filtros y las acciones quedan bloqueados
   mientras hay celdas pendientes, para no perder cambios sin guardarlos.

La interfaz usa acentos morados y turquesa, contadores con los colores del
semáforo y las etiquetas originales con emojis en las etapas y desplegables.
Los emojis son sólo presentación: Sheets recibe siempre el valor canónico.
La ficha del pedido reúne en una sola línea el doctor, la etapa (con su color
configurado) y el semáforo; el motivo del semáforo sólo aparece cuando agrega
algo, así que no se repite «En tiempo». Cada selector de la ficha se pinta con el
color de su opción, sin repetir el valor debajo, y lo que el usuario puede hacer
con la etapa se consulta en el ícono (?) de STATUS.
El menú de etapa pinta cada opción con el mismo color de su celda. Los encabezados
distinguen las columnas manuales de las que la app llena o calcula
automáticamente. Sólo **Folio** y **Semáforo** son columnas fijas de consulta (la
casilla **Abrir** es únicamente un control); todas las demás columnas provenientes
de Sheets se pueden corregir. Las columnas automáticas se pueden ocultar en grupo.

Las columnas no fijas se pueden arrastrar desde su encabezado. **Guardar orden**
se activa después de mover una columna y persiste el acomodo por usuario en
`PREFERENCIAS APP`, dentro del mismo archivo de Google Sheets configurado para
la app, por lo que se recupera en otra sesión o equipo. El historial de cada
pedido ya muestra `USUARIO`, que se registra en `TIEMPOS_APARATOS` al cambiar de
etapa.

Todos los usuarios autenticados pueden corregir las columnas provenientes de
`ESTATUS APARATOS`, incluidas doctor, paciente, aparato y las fechas que la app
autollena. Folio, semáforo y los cálculos exclusivos de la vista permanecen
protegidos. El filtro Responsable
elige inicialmente al usuario activo cuando su nombre existe en los datos; en
caso contrario comienza en Todos y siempre permite cambiar la selección.

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

El flujo y sus plazos se leen de `PROCESOS POR APARATO` (misma hoja horizontal
Fases/Tiempo por aparato que usa `PROCESOS POR PRODUCTO` en alineadores) y se
combinan sobre `PROCESS_CONFIG`: un aparato o tiempo que ya está programado en
el código sigue funcionando si Sheets falla o todavía no lo tiene ahí; la hoja
sólo agrega aparatos nuevos o actualiza sus tiempos, nunca elimina lo
programado. Un aparato con columna propia (p. ej. Hyrax o Trampa Lingual) usa
esa columna; sólo si no la tiene toma el flujo de `PIEZA SINTERIZADA`. Se relee cada hora (no en cada actualización de 30 s de los
pedidos) o al pulsar **Actualizar datos**, para no afectar el rendimiento. La
tabla usa la duración guardada en el registro activo de `TIEMPOS_APARATOS`. No
se inventan fechas iniciales para pedidos antiguos ni se escribe una columna de
semáforo en Google Sheets. Los registros nuevos respetan el orden real de sus
encabezados.

## Pedidos visibles y guardado

- Se ocultan `ENVIADO` histórico, `PRODUCTO ENVIADO`, `ENVÍO DE ENCUESTA` y `CANCELO`.
- Los pedidos se archivan desde `PRODUCTO ENVIADO`; la encuesta se registra desde el histórico.
- Debajo de **🚫 Confección en pausa** está el desplegable
  **🚚 Enviados · N pedido(s)**, histórico de los pedidos en `ENVIADO`, `PRODUCTO ENVIADO` y
  `ENVÍO DE ENCUESTA` (los `CANCELO` permanecen archivados en la hoja). Tiene
  buscador (folio, doctor, paciente o aparato) y filtro por aparato. Sólo se
  edita la etapa: se ofrece `ENVÍO DE ENCUESTA` y las etapas de trabajo del flujo.
  Al pulsar **Guardar etapas**, los pedidos en `PRODUCTO ENVIADO` o
  `ENVÍO DE ENCUESTA` permanecen archivados. Si se elige una etapa de trabajo,
  el pedido vuelve a la tabla principal. `TIEMPOS_APARATOS` registra el cambio
  como actualización del histórico o reactivación, según corresponda. Admin,
  Jime y Lesly pueden cambiar las etapas; Vero sólo consulta el histórico.
- Mientras haya etapas elegidas sin guardar en Enviados no se puede actualizar
  datos, filtrar ni guardar la tabla principal; lo mismo al revés.
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
