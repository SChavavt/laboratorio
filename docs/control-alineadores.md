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

- `ALINEADORES (nuevo)` contiene los pedidos.
- `PROCESOS POR PRODUCTO` es la fuente de verdad para etapas y plazos.
- `TIEMPOS_ALINEADORES` registra cada cambio y se crea al primer arranque si
  todavía no existe.
- Las pausas opcionales siguen siendo `BORRADOR TITAN`,
  `SOLICITUD DE CAMBIOS` e `IMPRESIÓN EN PAUSA`.
- Los pedidos enviados o cancelados permanecen en Sheets y se ocultan de la mesa
  activa.
- Debajo de la tabla de Seguimiento (y de Seguimiento Polanco) está el desplegable
  **🚚 Enviados · N pedido(s)**, histórico de los pedidos en `ENVIADO`. Tiene buscador
  (orden, doctor, paciente o producto) y filtro por producto. Sólo se edita la
  etapa: se ofrece cualquier etapa normal o pausa del producto, excepto Enviado y
  Cancelado. Al pulsar **Guardar y reactivar** el pedido vuelve a la tabla de
  pedidos activos y la bitácora registra `ENVIADO → nueva etapa` con el comentario
  "Reactivado desde el histórico de Enviados.".
- Mientras haya etapas elegidas sin guardar en Enviados no se puede refrescar,
  cambiar de pestaña ni guardar la tabla principal; lo mismo al revés.

## Tablero para pantalla (📺 Tablero)

Pestaña de sólo lectura pensada para dejarse en una TV. Junta los pedidos activos
de `ALINEADORES (nuevo)` y `POLANCO` (con sus bitácoras `TIEMPOS_ALINEADORES` y
`TIEMPOS_POLANCO`) sin cambiar la hoja activa de Seguimiento.

- **Encabezado:** pedidos activos por hoja, semáforo de la etapa actual (el mismo
  de Seguimiento), el pedido más antiguo y lo recibido/enviado esta semana y hoy.
  Recibidos sale de `FECHA DE RECEPCIÓN`; enviados, de los cambios a `ENVIADO`
  registrados por la app en la bitácora.
- **Flujo por etapa:** una columna por etapa normal, en el orden común de
  `PROCESOS POR PRODUCTO`, y aparte las pausas. Cada tarjeta muestra paciente,
  antigüedad, folio, tiempo consumido del plazo de la etapa y, si viene de
  Polanco, la marca `POL`. Las tarjetas van de la más antigua a la más reciente.
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

## Pruebas

```bash
python -m pytest -q tests/test_workbench.py tests/test_alineadores_pg.py tests/test_polanco.py tests/test_board.py
```


### Altas y seguimiento Polanco

- Aparatos: “Nuevo pedido” se encuentra en un desplegable cerrado dentro de
  Seguimiento, conservando los permisos de alta existentes. Ya no ocupa una pestaña.
- Alineadores: Seguimiento y Seguimiento Polanco tienen un desplegable “Nueva orden”.
  El formulario se carga sólo al abrirlo, incluso si no hay pedidos activos.
- Polanco lee y escribe la pestaña `POLANCO` del mismo archivo Control ALINEADORES.
  Conserva los campos adicionales, incluido `ADEUDO`, y las columnas repetidas de envíos.
- Los procesos de ambas pestañas provienen de `PROCESOS POR PRODUCTO`. Los tiempos
  de Polanco se registran en `TIEMPOS_POLANCO`, creada automáticamente al entrar
  por primera vez. No necesita secrets nuevos ni mezcla órdenes con igual folio.
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

## Solicitudes de guía

La vista **📋 Guías** replica la sesión de ARTTD JIMENA en app_v:

- **📋 Solicitud Guía**: formulario con Tipo de Envío fijo en
  `📋 Solicitudes de Guía`, vendedor bloqueado en `ARTTD JIMENA`, folio de
  factura opcional, comentario y la dirección DHL (campos obligatorios y
  opcionales). Al registrar se agrega un renglón en `data_pedidos` con las mismas
  columnas y valores que app_v (`id_vendedor = ARTTDJIM01`, `Estado = 🟡 Pendiente`,
  `Fecha_Entrega` = hoy, dirección en `Direccion_Guia_Retorno`). Si faltan las
  columnas que app_v agrega (`TD_Leal_Etapa`, `Tipo_Venta`, crédito y
  `Direccion_Guia_Retorno`) se crean al registrar. Reenviar la misma solicitud
  tras un error de conexión reutiliza su `ID_Pedido`, así que no se duplica.
- **📦 Guías Cargadas**: guías que almacén subió en `data_pedidos`,
  `datos_pedidos` y `casos_especiales` del último mes, sólo de ARTTD JIMENA y
  SCHAVA, con los mismos filtros (vendedor, últimos 7 días, fecha o rango) y el
  enlace a la última guía.
- Arriba de las pestañas aparece el aviso de guías cargadas en las últimas 12 h
  para `ARTTDJIM01`, igual que en app_v.
