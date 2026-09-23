# Control de laboratorio unificado

La única app que debe desplegarse en Streamlit es `lab_pg.py`. Desde su recuadro
principal se cambia entre **Aparatos** y **Alineadores** sin abrir otro enlace ni
volver a iniciar sesión.

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

## Navegación y sesión

- El selector **Aparatos / Alineadores** vive dentro del encabezado principal.
- La vista actual aparece escrita en el mismo encabezado.
- La elección se conserva en el URL como `vista=aparatos` o
  `vista=alineadores`.
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

## Pruebas

```bash
python -m pytest -q tests/test_workbench.py tests/test_alineadores_pg.py
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
- Las nuevas órdenes requieren producto, doctor y paciente. El número de orden
  es opcional; vacío genera un folio único. La recepción es la fecha actual y la
  etapa inicial es la primera etapa normal configurada para el producto.
- Cada seguimiento conserva su fotografía de datos y su editor por separado. Los
  cambios pendientes deben guardarse o descartarse antes de cambiar de pestaña.
- Si la orden se guarda pero falla la bitácora, la app lo informa y permite reparar
  la medición; no solicita volver a crear la orden.
