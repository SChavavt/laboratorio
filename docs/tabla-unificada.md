# Mesa de trabajo de laboratorio

`lab_pg.py` mantiene el flujo de procesos y muestra una sola tabla operativa en
**Seguimiento**, seguida de **Buscar casos** y **Recibidos de Forms**. **Nuevo pedido**
se abre desde Seguimiento. Admin, Jime, Lesly y Vero pueden consultar Buscar casos;
Vero conserva también su pestaña **Confección y Calidad**.
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
   de Ciudad de México al guardar, sin pedirla manualmente. Normalmente no hace
   falta capturarla: se registra sola al cambiar de etapa (ver
   [Envío y entrega de Stefano](#envío-y-entrega-de-stefano)).
3. La columna **Abrir** selecciona pedidos sin guardarlos ni modificar Sheets.
   Con un pedido seleccionado se muestran archivos, historial y las acciones
   de pago o diseño correspondientes. Con pedidos de impresión seleccionados,
   Admin/Lesly pueden marcarlos o avanzar por lote en la misma pantalla.
   Con un solo pedido, la etapa se cambia desde **STATUS** en su ficha y se
   confirma con **Guardar este pedido**. **Registrar impresión** aparece en esa
   ficha sólo para pedidos en `LISTO P/SINTERIZADO` sin impresión registrada y
   usuarios con permiso. **Impresión y avance por lote** aparece cerrado sólo al
   seleccionar varios pedidos de producción.
4. Buscar casos y Recibidos de Forms tienen sus propias pestañas.
   Las etapas de los pedidos permanecen juntas en Seguimiento.
5. **Actualizar datos** vuelve a leer los pedidos y calcular el semáforo.
   La hora de consulta es visible. Los filtros y las acciones quedan bloqueados
   mientras hay celdas pendientes, para no perder cambios sin guardarlos.

## Facturas PDF y buscador de casos

La ficha incluye **Adjuntar facturas PDF** dentro de **📋 Servicio y archivos**.
Permite elegir varios PDFs y pulsar **Guardar facturas**, así como añadir más
facturas después. Las selecciones de campos de la ficha se conservan mientras
se guardan los archivos. Dos facturas con el mismo nombre se almacenan por
separado; reintentar una carga parcialmente fallida conserva un solo objeto por
archivo de esa selección. Se valida la extensión y la cabecera PDF de todo el
lote, y se comprueba que la orden siga existiendo con un folio único antes de subir.

**🔎 Buscar casos** encuentra órdenes por folio, paciente, doctor o aparato,
sin distinguir mayúsculas ni acentos. Incluye pedidos activos, enviados,
cancelados y pausados. Al elegir una orden muestra el resumen y sus facturas,
con **Abrir PDF**, **Descargar PDF** y **Actualizar facturas**. También permite
adjuntar facturas desde ese buscador para órdenes archivadas. Los folios duplicados
requieren corregir la hoja antes de consultar o añadir sus facturas.

Las facturas se guardan en el bucket S3 existente bajo `facturas/aparatos/<folio>/`.
Cada carga tiene su propia clave de archivo; los nombres se codifican para
conservar espacios y caracteres especiales. **No hay que agregar columnas a
Google Sheets.** El vínculo se conserva por vista y folio, independientemente
de la etapa del pedido. Los enlaces para abrir y descargar son temporales
(30 minutos); **Actualizar facturas** genera enlaces nuevos.

Se utilizan las claves AWS existentes, en la raíz de Streamlit Secrets o en
`[aws]`: `aws_access_key_id`, `aws_secret_access_key`, `aws_region` y
`s3_bucket_name`. La cuenta debe poder listar objetos (`s3:ListBucket`) y
leer/escribir archivos (`s3:GetObject`, `s3:PutObject`) bajo el prefijo `facturas/`.
No se habilitan permisos públicos al subir las facturas. Estos permisos y la
configuración real del bucket se comprueban en el entorno desplegado.

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
protegidos. **Fecha para entrega** también es sólo de lectura (en la tabla y en
la ficha): la calcula una fórmula de la hoja (`FECHA/HORA ENVÍO STEFANO` + 6
días hábiles) y escribir en su rango la rompería, así que la app nunca la
escribe, ni al guardar pagos ni al crear pedidos. El filtro Responsable
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
| Gris | Sin inicio, sin plazo válido, esperando al doctor, folio duplicado o registro de tiempo inconsistente |

El detalle muestra el motivo del color. Se conservan las alertas especiales de
pagos para Tiger, Leone y Distalizador; se muestra la más urgente entre la alerta
de etapa y la alerta especial, salvo mientras se espera al doctor
(`REVISIÓN PLAN DOCTOR`, `REVISIÓN DISEÑO DOCTOR`, `VOBO/ACEPTACIÓN PLANEACIÓN`,
`ESPERANDO STL PSM DOCTOR`, `ESCANEO MAL (EN REPETICIÓN)`): entonces el pedido
queda gris aunque la hoja les asigne un tiempo, y la alerta especial sólo
aparece en el motivo. En las etapas de Stefano (`EN PLANEACIÓN`,
`SOLICITUD DE CAMBIOS`, `EN DISEÑO`) el plazo cuenta desde
`FECHA/HORA ENVÍO STEFANO`, aunque sea anterior al inicio de la etapa (un envío
capturado con otro día); una fecha sin hora del mismo día en que empezó la etapa
cuenta desde ese inicio. Se ignora, y se cuenta desde el inicio del registro
activo, si no se entiende, es futura o es de una ronda anterior: anterior al
cierre del último registro de una etapa de Stefano del pedido en
`TIEMPOS_APARATOS`, con 5 minutos de tolerancia. Semáforo, horas, plazo y límite
de la fila usan ese mismo inicio. Los plazos cuentan lunes a viernes, 24 horas por
día hábil, como el modelo existente; no representan un turno laboral de 8 horas.

El flujo y sus plazos se leen de `PROCESOS POR APARATO` (misma hoja horizontal
Fases/Tiempo por aparato que usa `PROCESOS POR PRODUCTO` en alineadores) y se
combinan sobre `PROCESS_CONFIG`: un aparato que ya está programado en el código
sigue funcionando si Sheets falla o todavía no lo tiene ahí. Un aparato con
columna en la hoja usa la lista completa de esa columna (nombres, orden y
tiempos), que reemplaza la programada. Un aparato con columna propia (p. ej. Hyrax o Trampa Lingual) usa
esa columna; sólo si no la tiene toma el flujo de `PIEZA SINTERIZADA`. Se relee cada hora (no en cada actualización de 30 s de los
pedidos) o al pulsar **Actualizar datos**, para no afectar el rendimiento. La
tabla usa la duración guardada en el registro activo de `TIEMPOS_APARATOS`. No
se inventan fechas iniciales para pedidos antiguos ni se escribe una columna de
semáforo en Google Sheets. Los registros nuevos respetan el orden real de sus
encabezados.

## Envío y entrega de Stefano

Las dos fechas de Stefano se registran solas en cada cambio de etapa (tabla,
ficha, cambio manual, reactivación desde Enviados o pausa), con la hora de
Ciudad de México y para cualquier usuario:

- Entrar a `EN PLANEACIÓN`, `SOLICITUD DE CAMBIOS` o `EN DISEÑO` desde otra
  etapa registra **FECHA/HORA ENVÍO STEFANO**: estar en esas etapas significa
  que el trabajo ya se envió a Stefano, y desde ahí se cuenta su regreso.
- Salir de esas etapas a cualquier otra registra **FECHA/HORA ENTREGA
  STEFANO** (Stefano entregó), salvo al pausar o cancelar, que no son una
  entrega. Pasar de una etapa de Stefano a otra registra ambas: regresó y se
  volvió a enviar.
- Cada ronda reemplaza la fecha anterior, así que **Fecha para entrega** (la
  fórmula de la hoja) sigue al último envío. Entrar a `STL PSM ENVIADO` ya no
  registra la entrega de Stefano.
- Si en el mismo guardado se captura alguna de las dos, manda lo capturado: del
  envío se toma el día elegido con la hora del guardado (para registrar un envío
  hecho antes) y la entrega se guarda tal cual. Un día capturado en un guardado
  anterior al cambio de etapa se reemplaza; después del cambio se puede corregir
  sin que se pierda. El envío no puede ser una fecha futura.

La confirmación del cambio de etapa dice lo registrado, p. ej. «Envío a Stefano
registrado: mié 07/10 16:00 · regresa lun 12/10 16:00» (el regreso suma el
tiempo de la nueva etapa) o «Entrega de Stefano registrada: …»; en Seguimiento
aparece como un solo aviso flotante aunque se cambien varios pedidos a la vez.
Si el día elegido para el envío es de una ronda anterior (p. ej. al pasar de
`EN PLANEACIÓN` a `SOLICITUD DE CAMBIOS` con un día pasado), la confirmación lo
dice y da el regreso que mostrará la tabla, contado desde el cambio de etapa.
Los encabezados de la tabla y la ayuda (?) de la ficha lo recuerdan.

## Agenda del pedido

Tres columnas calculadas (sólo lectura, se ocultan con las automáticas) van
justo después de **Límite etapa**, también para quien ya guardó un orden de
columnas antes de que existieran. Se recalculan en cada lectura con el flujo del
aparato en `PROCESOS POR APARATO`; no se escriben en Sheets.

| Columna | Qué muestra |
| --- | --- |
| Siguiente etapa | La siguiente etapa obligatoria del flujo (sin las opcionales `ESCANEO MAL` y `SOLICITUD DE CAMBIOS`). |
| Próxima fecha | Etapas de Stefano: «Regresa Stefano: vie 09/10 13:56», contado desde el envío registrado. Espera del doctor: «Esperando doctor desde lun 05/10 (2 días hábiles)». Pagos: «Esperando pago desde …». Otras etapas con tiempo: «Vence: …». |
| Entrega estimada | Día en que el pedido entraría a `PRODUCTO ENVIADO`: parte del vencimiento de la etapa actual (o de ahora, si ya pasó o la etapa no tiene tiempo) y suma el tiempo de cada etapa obligatoria pendiente. |

El pago nunca detiene la entrega estimada (las etapas de pago suman cero, porque
hay doctores que trabajan antes de pagar). Las esperas del doctor también suman
cero; como la columna se recalcula, la fecha se recorre sola mientras el doctor
no responde y lo indica con «(+ revisión doctor)». La ficha del pedido muestra
la próxima fecha y la entrega estimada junto al semáforo.

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
