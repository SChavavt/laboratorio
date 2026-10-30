# Control de laboratorio unificado

La única app que debe desplegarse en Streamlit es `lab_pg.py`. Desde su recuadro
principal se cambia entre **Aparatos**, **Alineadores** y **Guías** sin abrir otro
enlace ni volver a iniciar sesión.

`alineadores_pg.py` se conserva como módulo interno porque contiene la lógica,
la tabla y los flujos de alineadores; `lab_pg.py` lo carga sólo cuando esa vista
está activa. Así no se hacen lecturas innecesarias de ambos archivos de Google
Sheets en cada interacción.

## Configuración de Streamlit Secrets

La app unificada utiliza una sola cuenta de servicio y dos IDs independientes:

```toml
[gsheets]
google_credentials = """{ ... JSON de la cuenta de servicio ... }"""
sheet_id = "<ID_CONTROL_APARATOS>"
alineadores_sheet_id = "<ID_CONTROL_ALINEADORES>"
# Opcional: por defecto se usa el mismo Excel de pedidos que app_v.
ventas_sheet_id = "<ID_EXCEL_PEDIDOS_VENTAS>"
# Opcional: sólo si ventas debe leerse con otra cuenta de servicio.
# ventas_google_credentials = """{ ... }"""

[auth.passwords]
Admin = "..."
Jime = "..."
Lesly = "..."
Vero = "..."
```

La clave `gsheets.sheet_id` corresponde a **CONTROL APARATOS** y
`gsheets.alineadores_sheet_id` a **Control ALINEADORES**. Ambos archivos deben
estar compartidos como editores con el `client_email` incluido en
`google_credentials`.

La vista **Guías** abre el Excel de pedidos de ventas (el mismo `GOOGLE_SHEET_ID`
de app_v, o `gsheets.ventas_sheet_id` si se configura). Ese archivo también debe
compartirse como editor con el mismo `client_email`, o bien configurar
`gsheets.ventas_google_credentials` con la cuenta de servicio de ventas. Para
abrir las guías en vista previa se usan las credenciales AWS de `[ventas_aws]`
o, si no existen, las del laboratorio; sin ellas el enlace abre la URL directa.

## Navegación y sesión

- El selector **Aparatos / Alineadores** vive dentro del encabezado principal.
- La vista actual aparece escrita en el mismo encabezado.
- La elección se conserva en el URL como `vista=aparatos`,
  `vista=alineadores` o `vista=guias`.
- Cada vista tiene su color: morado (aparatos), verde (alineadores) y azul
  (guías).
- El inicio de sesión es único para ambas vistas.
- El usuario recordado en el enlace lleva una firma; cambiar manualmente
  `usuario=...` no concede permisos de otro usuario.
- Al cerrar sesión se conserva la vista elegida.

## Datos de alineadores

Las tablas abren con **Última edición primero**: al guardar una orden nueva,
corregir campos de la tabla o ficha (incluidos los envíos), o cambiar de etapa,
el caso aparece arriba. El orden se conserva al recargar y se aplica también a
Polanco, AERIS, Seguimiento General, Pausados y Enviados. El selector **Orden**
ofrece además el orden de la hoja y la prioridad por urgencia. Los pedidos
anteriores también se ordenan desde la primera lectura usando su última
actividad registrada en la bitácora, sin tener que editarlos. Los casos sin
actividad registrada quedan al final, por fecha de recepción; si tampoco tienen
esa fecha, conservan entre ellos el orden de la hoja.
Los detalles de persistencia y el alcance de los guardados se describen en
[Orden de los casos por última edición](tabla-unificada.md#orden-de-los-casos-por-última-edición).

- `ALINEADORES (nuevo)` contiene los pedidos.
- `PROCESOS POR PRODUCTO` es la fuente de verdad para etapas y plazos.
- `TIEMPOS_ALINEADORES` registra cada cambio y se crea al primer arranque si
  todavía no existe.
- Las pausas opcionales son `BORRADOR TITAN`, `SOLICITUD DE CAMBIOS`,
  `IMPRESIÓN EN PAUSA`, `FALTA DE PAGO DE PLANEACIÓN` y
  `FALTA DE PAGO DE IMPRESIÓN`. Sólo se ofrecen cuando están incluidas en
  la columna del producto en `PROCESOS POR PRODUCTO`; las celdas de Tiempo
  correspondientes deben quedar vacías. No son pasos obligatorios ni tienen
  plazo de producción. El semáforo las muestra como **En pausa**, se registra
  su duración y, al resolverlas, se puede elegir la etapa donde reanudar.
- Para habilitar las dos pausas de pago, primero desplegar el código que las
  reconoce y después agregarlas a `PROCESOS POR PRODUCTO` para los productos
  correspondientes. Así se evita tratarlas como etapas normales en una versión
  anterior de la app.
- La pausa `FALTA DE PAGO DE PLANEACIÓN` corresponde a Convencional, Graphy,
  Setup, Guarda y Guía. Retenedores y Modelos sólo llevan impresión, por lo que
  no deben ofrecer esa pausa. `FALTA DE PAGO DE IMPRESIÓN` corresponde a los
  siete productos, incluidos Retenedores y Modelos.
- **🚫 Caso en pausa** está disponible en STATUS para todos los productos, sin
  agregarlo a `PROCESOS POR PRODUCTO`. Al guardar `CASO EN PAUSA`, el pedido sale
  de la lista principal, de los contadores activos y del tablero para pantalla.
  Permanece en su hoja y se consulta debajo de Seguimiento en el desplegable
  **🚫 Caso en pausa · N pedido(s)**, con buscador y filtro por producto. También
  aparece en Polanco, AERIS y Seguimiento General, separado por origen.
  Las pausas operativas (Titan, cambios, impresión y pagos) siguen visibles en
  la lista principal: sólo el nuevo estatus archiva temporalmente el caso.
  Para devolverlo, elige una etapa del producto y pulsa **Guardar y reactivar**;
  en este archivo sólo se edita STATUS. La bitácora conserva la etapa anterior,
  registra la pausa sin plazo de producción y abre la medición de la etapa
  elegida al reanudar. Se conservan los permisos de edición existentes.
- **STATUS** ofrece la etapa actual, la siguiente, las pausas y Cancelado. Para
  correcciones, Admin, Jime y Lesly tienen además **Ver todas las etapas…**,
  igual que en Aparatos: en la tabla abre el segundo nivel del mismo menú (con
  «← Volver a etapas sugeridas») y en la ficha abre **Etapa del flujo** debajo de
  STATUS. Muestra todo el flujo del producto, sus pausas y Cancelado. Abrir la
  lista no modifica el pedido; la etapa elegida se guarda con **Guardar cambios**
  o **Guardar este pedido**, y la bitácora la registra como "Cambio manual de
  etapa desde STATUS (todas las etapas).". Un salto que no se eligió desde esa
  lista se sigue rechazando, y los pedidos enviados se reactivan desde el
  histórico de Enviados.
- Los pedidos enviados o cancelados permanecen en Sheets y se ocultan de la mesa
  activa.
- Debajo de la tabla de Seguimiento (y de Seguimiento Polanco) está el desplegable
  **🚚 Enviados · N pedido(s)**, histórico de los pedidos en `ENVIADO`. Tiene buscador
  (orden, doctor, paciente o producto) y filtro por producto. Sólo se edita la
  etapa: se ofrece cualquier etapa normal o pausa del producto, excepto Enviado y
  Cancelado. Al pulsar **Guardar y reactivar** el pedido vuelve a la tabla de
  pedidos activos y la bitácora registra `ENVIADO → nueva etapa` con el comentario
  "Reactivado desde el histórico de Enviados.".
- Mientras haya etapas elegidas sin guardar en Enviados o Caso en pausa no se
  puede refrescar, cambiar de pestaña ni guardar la tabla principal; lo mismo
  al revés. En Seguimiento General también se protegen las otras hojas.

## Ficha del pedido

Al marcar un pedido se abre su ficha. Arriba, en una sola línea, están el doctor,
la etapa y el semáforo con su avance (`3.20 de 8 h hábiles consumidas`). Debajo
quedan los campos principales y el resto en desplegables juntos (**Servicio y
archivos**, **Fechas y entrega**…). Cada selector se pinta con el color de su
opción; la etapa usa el mismo color que en la tabla.

En **📋 Servicio y archivos**, **Adjuntar facturas PDF** permite subir varios PDFs
a la orden mediante **Guardar facturas**, y agregar más en cargas posteriores.
El guardado conserva los campos pendientes de la ficha. Las facturas se guardan
en el S3 del laboratorio, por separado para Alineadores, Polanco y AERIS:
`facturas/alineadores/<folio>/`, `facturas/polanco/<folio>/` y
`facturas/aeris/<folio>/`. Compartir un folio entre hojas no mezcla sus archivos.
No hay que agregar columnas a Sheets.

La pestaña **🔎 Buscar casos** consulta Alineadores, Polanco y AERIS, incluidas
órdenes enviadas, canceladas y pausadas, por paciente, doctor, folio o producto.
El resultado identifica la hoja de origen y muestra el resumen de la orden,
sus facturas y los enlaces existentes de sus columnas de archivos.
**Abrir PDF** y **Descargar PDF** usan enlaces temporales; **Actualizar facturas**
renueva la lista y esos enlaces. Desde el mismo resultado se pueden añadir
facturas a órdenes archivadas. Los folios duplicados dentro de una hoja requieren
corregir la hoja antes de consultar o adjuntar sus facturas.

La configuración AWS y los permisos del prefijo `facturas/` se describen en
[Facturas PDF y buscador de casos](tabla-unificada.md#facturas-pdf-y-buscador-de-casos).

## Envíos y alineadores (ficha del pedido)

Las columnas del plan de tratamiento (`TEMPLATE SUP` … `Total`, R:V) y de los
ocho envíos (`TEMP. SUP`, `TEMP. INF`, `NO. ALIN SUP`, `NO. ALIN INF`, `Total`,
`FECHA PAGO IMPRESIÓN`, `FECHA ENVÍO`, de W a BZ) no aparecen en la tabla. Se
capturan en la ficha del pedido, dentro del desplegable
**🚚 Envíos y alineadores**, y se guardan con el mismo **💾 Guardar este pedido**.

- **Plan de tratamiento:** templates y alineadores de todo el caso. El total se
  calcula solo.
- **Un envío a la vez:** si el pedido no tiene envíos, sólo se captura el
  Envío 1. Si ya tiene alguno, se muestra el último registrado (para corregirlo)
  y el botón **➕ Registrar Envío N**. Al pulsarlo, los campos quedan vacíos y
  debajo de cada uno aparece lo del envío anterior (`↳ Envío 1: 1-7`), para
  escribir lo nuevo o lo mismo. **📋 Usar las mismas cantidades** copia las
  cantidades del envío anterior y **✖️ Cancelar nuevo envío** descarta sólo ese
  envío.
- **Cantidades:** templates con número; alineadores con número (`7`) o rango
  (`1-7`, cuenta 7 piezas), como ya se usa en la hoja. Otro texto no se guarda.
- **Total:** se calcula al cambiar las cantidades del bloque. Si la celda de la
  hoja tiene fórmula, se conserva la fórmula.
- **Pago impresión:** fecha, 🎁 Cortesía (`CORTESIA`) o ⏳ Pendiente
  (`PENDIENTE`). **Fecha envío:** calendario.
- Arriba del envío se ve el avance: `Enviado 14 de 28 piezas del plan · faltan 14`.
- En Polanco, las mismas columnas salen de los campos sueltos de la ficha y se
  capturan en este desplegable; su tabla no cambia.
- Los pedidos en `ENVIADO` se reactivan desde el histórico de Enviados para
  registrar su siguiente envío.

## Tablero para pantalla (📺 Tablero)

Pestaña de sólo lectura pensada para dejarse en una TV. Junta los pedidos activos
de `ALINEADORES (nuevo)`, `POLANCO` y `AERIS` (con sus bitácoras
`TIEMPOS_ALINEADORES`, `TIEMPOS_POLANCO` y `TIEMPOS_AERIS`) sin cambiar la hoja
activa de Seguimiento.

- **Encabezado:** pedidos activos por hoja, semáforo de la etapa actual (el mismo
  de Seguimiento), el pedido más antiguo y lo recibido/enviado esta semana y hoy.
  Recibidos sale de `FECHA DE RECEPCIÓN`; enviados, de los cambios a `ENVIADO`
  registrados por la app en la bitácora.
- **Flujo por etapa:** una columna por etapa normal, en el orden común de
  `PROCESOS POR PRODUCTO`, y aparte las pausas. Cada tarjeta muestra paciente,
  antigüedad, folio, tiempo consumido del plazo de la etapa y, si viene de
  Polanco o AERIS, la marca `POL` o `AER`. Las tarjetas van de la más antigua a la más reciente.
  Una etapa que no está en `PROCESOS POR PRODUCTO` aparece en “Otras etapas”.
- **Prioridad por antigüedad:** los 10 pedidos activos más antiguos (`#1` … `#10`,
  la misma marca aparece en su tarjeta).
- **Antigüedad:** días hábiles (lunes a viernes) desde `FECHA DE RECEPCIÓN`; si la
  celda está vacía se usa el primer registro del pedido en la bitácora.
- Se actualiza solo cada 60 s (las lecturas de Sheets se comparten 30 s entre
  todas las sesiones). Si Sheets falla, conserva la última lectura y lo avisa.
  **Actualizar** fuerza una lectura nueva. El selector permite ver ambas hojas o
  una sola.
- **Modo pantalla** oculta menús, encabezado y pestañas, pinta el tablero a
  pantalla completa y ajusta cuántas tarjetas caben por columna. Queda guardado
  en el enlace como `pantalla=1`: abrir
  `…?vista=alineadores&pantalla=1` (con el usuario recordado) entra directo al
  tablero en modo pantalla. Para salir, el interruptor de la esquina inferior
  derecha. Con F11 el navegador quita también su barra.

## Recibidos de Forms (alineadores)

Las subpestañas Prescripción Alineadores TD, Prescripción Marca Blanca, Otros
Productos y AERIS leen las respuestas de cada Google Sheet de respuestas y, además, la
estructura actual de su Google Form con Google Forms API.

- La tabla, la ficha y el PDF de cada respuesta siguen las secciones y el orden
  de preguntas del formulario actual. Si se agrega, mueve o renombra una
  pregunta en Forms, la app lo refleja sola en menos de 2 minutos; no hay que
  tocar código. Google pone cada pregunta nueva al final del Sheet, por eso el
  orden de columnas no sirve para esto.
- Las respuestas a preguntas que ya no están en el formulario salen al final,
  en “Otras respuestas”, para no perder datos de respuestas viejas.
- Requisitos: Google Forms API habilitada en el proyecto de la cuenta de
  servicio y cada formulario compartido como editor con su `client_email`. Si
  falta alguno, la pestaña lo avisa y el PDF usa el orden de columnas del Sheet.
- Los IDs de los formularios vienen por defecto en `ALIGNERS_FORMS`; se pueden
  cambiar con `[google_forms_alineadores.<td|marca_blanca|otros_productos|aeris>]`
  `form_id = "..."` (igual que `sheet_id` y `worksheet`).

## Avisos de respuestas nuevas de Forms

Con la app abierta, cada usuario recibe un aviso cuando llega una respuesta
nueva a un formulario que puede abrir: Admin, Jime, Lesly, Juan y Paula los 5
(Aparatos sinterizados y los 4 de alineadores, incluido AERIS); Vero los 4 de alineadores.

- La app revisa los Sheets de respuestas cada minuto (`forms_notices.py`, con
  su propio caché de 55 s que no se vacía al guardar). Cada respuesta nueva sale
  en un aviso flotante que se desvanece solo y en el botón **🔔 N respuestas
  nuevas de Forms** debajo del encabezado. La pestaña del navegador muestra
  `(N)` para notarlo aunque la app esté en segundo plano.
- **Ver** abre la vista, la pestaña 📥 Recibidos de Forms, la subpestaña del
  formulario y deja seleccionada esa respuesta. Si hay cambios sin guardar, avisa
  y no cambia de vista, igual que el selector de vista.
- Una respuesta deja de ser nueva al abrirla (desde el aviso o eligiéndola en la
  pestaña de Forms, que lista arriba del selector las nuevas con 🆕, aunque no
  estén entre las 25 más recientes) o con **Marcar todas como vistas**, que sólo
  marca las que la lista mostraba. Abrirla en la pestaña baja el número de la
  campana en su siguiente ciclo.
- Lo visto se guarda por usuario en la hoja `AVISOS FORMS` de CONTROL APARATOS
  (se crea sola). Sirve en cualquier dispositivo. La primera vez que un usuario
  entra, lo que ya existía cuenta como visto: sólo avisa lo que llega después.
- Las respuestas se identifican por su “Marca temporal”, no por el número de
  fila, para que borrar filas del Sheet no oculte avisos. Dos respuestas del
  mismo formulario en el mismo segundo cuentan como un solo aviso.
- En el modo pantalla del Tablero no se muestra la campana ni se leen los Forms
  (se revisa en cada ciclo, también al activarlo con el interruptor). Fuera de la app no
  llegan avisos (correo, celular): eso requeriría un Apps Script en cada
  formulario.

## Pruebas

```bash
python -m pytest -q tests/test_workbench.py tests/test_alineadores_pg.py tests/test_polanco.py tests/test_board.py tests/test_shipments.py tests/test_forms_notices.py
```


### Altas y seguimientos Polanco y AERIS

- Aparatos: “Nuevo pedido” se encuentra en un desplegable cerrado dentro de
  Seguimiento, conservando los permisos de alta existentes. Ya no ocupa una pestaña.
- Alineadores: Seguimiento, Seguimiento Polanco y AERIS tienen un desplegable “Nueva orden”.
  El formulario se carga sólo al abrirlo, incluso si no hay pedidos activos.
- Polanco lee y escribe la pestaña `POLANCO` del mismo archivo Control ALINEADORES.
  Conserva los campos adicionales, incluido `ADEUDO`, y las columnas repetidas de envíos.
- AERIS lee y escribe la pestaña `AERIS` de ese archivo, con encabezados en la fila 2.
  Su alta sólo ofrece el producto AERIS y usa los campos existentes de esa hoja;
  no requiere `TIPO` ni `ETAPA SOLICITUD`. Las listas nativas se respetan cuando
  existen; los campos opcionales sin catálogo se capturan como texto.
  El producto AERIS puede seguir usándose en Polanco: las órdenes de cada hoja
  son independientes y no se copian ni se sincronizan automáticamente.
- Los procesos de las tres pestañas provienen de `PROCESOS POR PRODUCTO`. Los tiempos
  de Polanco se registran en `TIEMPOS_POLANCO`, creada automáticamente al entrar
  por primera vez. No necesita secrets nuevos ni mezcla órdenes con igual folio.
- AERIS tiene su propia bitácora `TIEMPOS_AERIS`, creada automáticamente al entrar.
  Su flujo empieza en `LISTO STLs PARA IMPRESIÓN`, continúa por
  `EN IMPRESIÓN Y CONFECCIÓN` y `LISTO P/ENVÍO`, y termina en `ENVIADO`.
  `IMPRESIÓN EN PAUSA` y `CANCELADO` conservan su tratamiento opcional.
- Las nuevas órdenes requieren producto, doctor y paciente. El folio se genera
  automáticamente al guardar con formato DDMMAAAA-NNN, igual que en Aparatos.
  La recepción es la fecha actual y la etapa inicial es la primera etapa normal
  configurada para el producto.
- El formulario usa tres columnas y selectores con emojis de color. Las opciones
  se leen de las tablas nativas de cada hoja, incluidas las opciones aún no usadas.
  Los colores individuales de chips no están expuestos por la API de Sheets:
  los indicadores visuales son de la app y no alteran los valores guardados.
- El catálogo se lee al abrir el formulario por primera vez y se conserva durante
  la sesión. Actualizar datos vuelve a consultar las opciones de Sheets.
- Cada seguimiento conserva su fotografía de datos y su editor por separado. Los
  cambios pendientes deben guardarse o descartarse antes de cambiar de pestaña.
- Si la orden se guarda pero falla la bitácora, la app lo informa y permite reparar
  la medición; no solicita volver a crear la orden.

### Formulario AERIS

En **Recibidos de Forms → AERIS** se leen las respuestas del archivo conectado,
pestaña `Respuestas de formulario 1`. Los identificadores del archivo y del
formulario se configuran únicamente en Streamlit Secrets; no se publican en el
repositorio. Agrega esta sección en **Settings → Secrets**, con los IDs reales:

```toml
[google_forms_alineadores.aeris]
sheet_id = "ID_DEL_ARCHIVO_DE_RESPUESTAS"
worksheet = "Respuestas de formulario 1"
form_id = "ID_INTERNO_DEL_FORMULARIO"
```

`form_id` es el ID del enlace de edición, no el ID público `1FAIpQL...` del enlace
que usan los pacientes para responder. Sin `sheet_id`, la subpestaña indica que
falta conectar la hoja y la campana no intenta leerla. Si falta acceso a la
estructura del formulario, la ficha/PDF usa el orden de columnas de la hoja.
La cuenta de servicio existente necesita acceso al archivo de respuestas y al
formulario, igual que con los otros formularios. Las respuestas son de consulta;
el alta de órdenes se hace con **Nueva orden** en el seguimiento que corresponda.

## Solicitudes de guía

En la vista **📋 Guías** todo son solicitudes de guía (no pedidos de venta).
Usa el mismo registro que ARTTD JIMENA en app_v, para que almacén las vea igual:

- **📋 Solicitar guía**: quien solicita queda fijo como `ARTTD JIMENA` y la
  fecha de solicitud es la de hoy. Se captura folio de factura (opcional),
  indicaciones para la guía y la dirección de envío DHL (datos obligatorios y
  opcionales). Al registrar se agrega un renglón en `data_pedidos` con las mismas
  columnas y valores que app_v (`Tipo_Envio = 📋 Solicitudes de Guía`,
  `id_vendedor = ARTTDJIM01`, `Estado = 🟡 Pendiente`, `Fecha_Entrega` = hoy,
  destinatario en `Cliente` y dirección en `Direccion_Guia_Retorno`). Si faltan
  las columnas que app_v agrega (`TD_Leal_Etapa`, `Tipo_Venta`, crédito y
  `Direccion_Guia_Retorno`) se crean al registrar. Reenviar la misma solicitud
  tras un error de conexión reutiliza su ID, así que no se duplica.
- **⚡ Llenado rápido de la dirección** (arriba del formulario). Todo llena los
  campos del formulario para revisarlos antes de registrar; nada se registra solo:
  - **🔎 Cliente anterior**: se escribe el nombre y se elige. Salen los
    destinatarios con dirección en `Direccion_Guia_Retorno` de `data_pedidos` y
    `datos_pedidos`: solicitudes de guía (de esta vista y de app_v) y ventas
    foráneas con guía, de cualquier vendedor. Hasta 3 direcciones distintas por
    destinatario, la más reciente primero, con la fecha del último envío. No hay
    hoja aparte: cada solicitud registrada aparece aquí la siguiente vez. El
    nombre se muestra sin acentos para encontrarlo escribiendo «martinez».
  - **📋 Pegar datos de envío**: el mensaje de WhatsApp o el correo tal como lo
    mandó el cliente. Reconoce datos con etiqueta (`CALLE:`, `NO. EXTERIOR:`,
    `NO. INTERIOR:`, `COLONIA:`, `C.P.:`, `Municipio / Alcaldía:`, `Tel`, `Cel`,
    `Correo`, `Referencias`, `Entre calles`…) y direcciones escritas de corrido.
    Si el texto trae los bloques `REMITENTE` y `DESTINATARIO` (formato de los
    correos de guía) sólo se usa el destinatario.
  - **📎 Factura, nota de venta o captura**: de la factura TD&S se toman el
    cliente, el folio y el recuadro «Consignado a» (no el domicilio fiscal), igual
    que la 🪄 de app_v; de la nota de venta («Orden de surtido»), el cliente y su
    domicilio. También se leen capturas JPG/PNG/WebP con OCR y PDFs con texto
    (por ejemplo, un correo guardado con «Convertir a PDF»). Una foto de la
    pantalla tomada con el celular casi no se puede leer: mejor captura o pegar el
    texto.
  - Si los datos son de otro destinatario que el ya escrito, se reemplaza toda la
    dirección para no mezclar dos clientes; si son del mismo, sólo se completan los
    campos que vienen. Debajo de los botones se ve qué se llenó y qué falta.
  - El OCR usa `tesseract` en el servidor (`packages.txt`); la lectura de PDFs,
    `pdfplumber`. Nada se manda a servicios externos.
- **📦 Guías cargadas**: sólo solicitudes de guía (`Tipo_Envio = 📋 Solicitudes de
  Guía`) de ARTTD JIMENA y SCHAVA a las que almacén ya les cargó la guía, del
  último mes, en `data_pedidos` (En curso) y `datos_pedidos` (Histórico). Tiene
  filtros por quién solicitó, últimos 7 días, fecha o rango, y el botón para
  abrir la guía.
- Arriba de las pestañas aparece el aviso de solicitudes con guía cargada en las
  últimas 12 h para `ARTTDJIM01`.
