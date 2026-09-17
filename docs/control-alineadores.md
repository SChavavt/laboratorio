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
sheet_id = "1CI4MxQmOqiSFZiO3h4YR5mvAQWFJrI1emAiBCTU5Xeg"
alineadores_sheet_id = "1wNKD4bl__w1qMG182xFfu-1NlbcWTZdFEpa-h8ZLtik"

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
