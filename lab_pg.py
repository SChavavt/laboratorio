import streamlit as st
import pandas as pd
from datetime import datetime
import gspread
import json
import colorsys
import html
from google.oauth2.service_account import Credentials

# ==============================
# 🔧 CONFIGURACIÓN
# ==============================
SCOPE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

COLUMNS = [
    "No_orden",
    "Nombre_paciente",
    "Nombre_doctor",
    "Status",
    "Status_NEMO",
    "Tipo_alineador",
    "Fecha_recepcion",
    "Dias_entrega",
    "Comentarios",
    "Notas",
    "Ultima_Modificacion",
]

DEFAULT_EMOJI_PALETTE = [
    "🔴",
    "🟠",
    "🟡",
    "🟢",
    "🔵",
    "🟣",
    "🟤",
    "⚫",
    "⚪",
    "🟥",
    "🟧",
    "🟨",
    "🟩",
    "🟦",
    "🟪",
    "🟫",
    "🔶",
    "🔷",
    "🔸",
    "🔹",
]


def _compute_color_pair(index: int, total: int, saturation: float = 0.65, value: float = 0.92):
    if total <= 0:
        return "#A0AEC0", "rgba(160, 174, 192, 0.18)"
    hue = (index % total) / total
    r, g, b = colorsys.hsv_to_rgb(hue, saturation, value)
    r_i, g_i, b_i = (int(r * 255), int(g * 255), int(b * 255))
    return (
        "#{:02X}{:02X}{:02X}".format(r_i, g_i, b_i),
        "rgba({:d}, {:d}, {:d}, 0.18)".format(r_i, g_i, b_i),
    )


def _build_option_structs(items, emoji_palette=None):
    palette = emoji_palette or DEFAULT_EMOJI_PALETTE
    total = len(items)
    options = []
    for idx, item in enumerate(items):
        if isinstance(item, (tuple, list)):
            label, emoji = item[0], item[1]
        else:
            label = item
            emoji = palette[idx % len(palette)]
        color, bg_color = _compute_color_pair(idx, total)
        display = f"{emoji} {label}"
        options.append(
            {
                "value": label,
                "emoji": emoji,
                "color": color,
                "bg_color": bg_color,
                "display": display,
            }
        )
    return options


STATUS_OPTIONS = _build_option_structs(
    [
        "1. Revisión de scan",
        "2. Por hacer Setup",
        "2.1 Scan con falla",
        "3. RETENEDOR",
        "3.1 RETENEDOR en proceso",
        "3.2 SUD en proceso",
        "3.3 SUD proceso modificación",
        "4. Para revisión de SUD 1",
        "4.1 Para revisión de SUD 2",
        "5. Revisado – hay que modificar",
        "5.1 Revisión alineación y nivelación",
        "5.2 Revisión attachments y secuencia",
        "5.3 Revisado VOBO y cotizar",
        "6. Para cotización",
        "7. Cotización lista",
        "7.1 Listo para envío al Dr.",
        "8. Enviado al Dr.",
        "9. Solicitud cambios Dr.",
        "10. Exportar modelos e imprimir",
        "11. Imprimir modelo",
        "12.1 Enviar biomodelos",
        "12.3 Biomodelos enviados",
        "13. Solo alineador",
        "13.1 Para impresión",
        "13.2 Reimpresión sin costo",
        "14. Enviado a impresión",
        "14.1 En impresión",
        "15. Impresión perfecta",
        "16. Error de impresión",
        "17. En calidad",
        "Revisados",
        "Falta Pago",
        "Esperando respuesta del Dr.",
        "REFINAMIENTO",
        "16. Enviado / empaquetado",
    ]
)

STATUS_NEMO_OPTIONS = _build_option_structs(
    [
        ("Nuevo", "🆕"),
        ("Carpeta específica", "🗂️"),
        ("Duplicado", "🧬"),
        ("Seguimiento", "📈"),
        ("Solo impresión", "🖨️"),
    ]
)


def _build_lookup_maps(options):
    by_value = {opt["value"]: opt for opt in options}
    by_display = {opt["display"]: opt for opt in options}
    return by_value, by_display


STATUS_BY_VALUE, STATUS_BY_DISPLAY = _build_lookup_maps(STATUS_OPTIONS)
STATUS_NEMO_BY_VALUE, STATUS_NEMO_BY_DISPLAY = _build_lookup_maps(STATUS_NEMO_OPTIONS)


def _normalize_option(value, by_value, by_display):
    if value is None:
        return None
    value_str = str(value).strip()
    if not value_str:
        return None
    option = by_display.get(value_str)
    if option:
        return option
    option = by_value.get(value_str)
    if option:
        return option
    for opt in by_value.values():
        if value_str.endswith(opt["value"]):
            return opt
    return None


def _display_value(value, by_value, by_display):
    option = _normalize_option(value, by_value, by_display)
    if option:
        return option["display"]
    return value


def _badge_html(option):
    label = html.escape(option["display"])
    return (
        f"<span class='status-badge' style='--status-color:{option['color']}; --status-bg:{option['bg_color']}'>"
        f"<span class='status-dot'></span>"
        f"<span class='status-text'>{label}</span>"
        "</span>"
    )


def _badge_value(value, by_value, by_display):
    option = _normalize_option(value, by_value, by_display)
    if option:
        return _badge_html(option)
    return html.escape(str(value)) if value is not None else ""

# ==============================
# 🔐 CLIENTE GOOGLE SHEETS
# ==============================
def _get_gs_client():
    creds_str = st.secrets["gsheets"]["google_credentials"]  # 👈 viene como string
    creds_info = json.loads(creds_str)  # 👈 lo convertimos en dict
    credentials = Credentials.from_service_account_info(creds_info, scopes=SCOPE)
    return gspread.authorize(credentials)
    
@st.cache_resource
def get_worksheet():
    client = _get_gs_client()
    sheet_id = st.secrets["gsheets"]["sheet_id"]
    ws_name = "Procesos_Graphy"

    ss = client.open_by_key(sheet_id)
    try:
        ws = ss.worksheet(ws_name)
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(title=ws_name, rows="1000", cols=str(len(COLUMNS)))
        ws.update("A1", [COLUMNS])
    else:
        # Mantener sincronizada la fila de encabezados con los campos definidos.
        current_headers = ws.row_values(1)
        if current_headers[: len(COLUMNS)] != COLUMNS:
            ws.update("A1", [COLUMNS])
    return ws

@st.cache_data(ttl=30)
def fetch_df():
    ws = get_worksheet()
    values = ws.get_all_values()
    if not values:
        return pd.DataFrame(columns=COLUMNS)
    df = pd.DataFrame(values[1:], columns=values[0])
    for c in COLUMNS:
        if c not in df.columns:
            df[c] = ""
    return df[COLUMNS]

def append_row(row):
    ws = get_worksheet()
    values = [str(row.get(c, "")) for c in COLUMNS]
    ws.append_row(values, value_input_option="USER_ENTERED")

# ==============================
# 🚀 APP STREAMLIT
# ==============================
st.set_page_config(page_title="Procesos – ARTTDLAB", layout="wide")
st.title("🧪 Plataforma de Procesos – ARTTDLAB")
st.markdown(
    """
<style>
.status-badge {
    display: inline-flex;
    align-items: center;
    gap: 0.45rem;
    padding: 0.15rem 0.7rem;
    border-radius: 999px;
    border: 1px solid var(--status-color, #CBD5F5);
    background-color: var(--status-bg, rgba(203, 213, 225, 0.25));
    font-weight: 600;
    color: #1f2933;
    font-size: 0.85rem;
}
.status-badge .status-dot {
    width: 0.65rem;
    height: 0.65rem;
    border-radius: 50%;
    background: var(--status-color, #64748b);
    display: inline-block;
}
.status-badge .status-text {
    white-space: nowrap;
}
.status-table {
    max-height: 600px;
    overflow: auto;
    border: 1px solid rgba(15, 23, 42, 0.12);
    border-radius: 0.75rem;
    margin-top: 0.75rem;
}
.status-table table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.9rem;
}
.status-table thead th {
    position: sticky;
    top: 0;
    background: rgba(248, 250, 252, 0.95);
    backdrop-filter: blur(4px);
    z-index: 1;
}
.status-table th,
.status-table td {
    padding: 0.55rem 0.75rem;
    border-bottom: 1px solid rgba(15, 23, 42, 0.06);
    text-align: left;
    color: #1f2933;
}
.status-table tbody tr:last-child td {
    border-bottom: none;
}
</style>
""",
    unsafe_allow_html=True,
)

tab1, tab2 = st.tabs(["➕ Nuevo Proceso", "📋 Consulta"])

# ➕ NUEVO PROCESO
with tab1:
    st.subheader("🆕 Registrar nuevo proceso")

    with st.form("form_nuevo"):
        in_orden = st.text_input("🧾 No. orden *")
        in_paciente = st.text_input("🧑‍🦱 Nombre del paciente *")
        in_doctor = st.text_input("🧑‍⚕️ Nombre del doctor *")
        in_status = st.selectbox(
            "📌 Status *",
            STATUS_OPTIONS,
            format_func=lambda option: option["display"],
        )
        in_status_nemo = st.selectbox(
            "🌐 Status en NEMO *",
            STATUS_NEMO_OPTIONS,
            format_func=lambda option: option["display"],
        )
        in_tipo_alineador = st.selectbox(
            "🦷 Tipo de alineador *", ["Graphy", "Convencional"]
        )
        in_fecha_recepcion = st.date_input("📅 Fecha de recepción *", datetime.today())
        in_dias_entrega = st.number_input(
            "⏳ Días de entrega *", min_value=1, value=1, step=1
        )
        in_comentarios = st.text_area("💬 Comentarios")
        in_notas = st.text_area("📝 Notas")
        enviado = st.form_submit_button("💾 Guardar")

    if (
        enviado
        and in_orden
        and in_paciente
        and in_doctor
        and in_status
        and in_status_nemo
        and in_tipo_alineador
        and in_fecha_recepcion
        and in_dias_entrega
    ):
        row = {
            "No_orden": in_orden,
            "Nombre_paciente": in_paciente,
            "Nombre_doctor": in_doctor,
            "Status": in_status["display"],
            "Status_NEMO": in_status_nemo["display"],
            "Tipo_alineador": in_tipo_alineador,
            "Fecha_recepcion": in_fecha_recepcion.strftime("%Y-%m-%d"),
            "Dias_entrega": str(int(in_dias_entrega)),
            "Comentarios": in_comentarios,
            "Notas": in_notas,
            "Ultima_Modificacion": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        append_row(row)
        st.success("🎉 Proceso registrado correctamente.")
        st.cache_data.clear()
    elif enviado:
        st.error("⚠️ Por favor completa los campos obligatorios (*).")

# 📋 CONSULTA
with tab2:
    st.subheader("Listado de procesos")
    df = fetch_df()
    if not df.empty:
        df_clean = df.fillna("")
        df_clean["Status"] = df_clean["Status"].apply(
            lambda value: _display_value(value, STATUS_BY_VALUE, STATUS_BY_DISPLAY)
        )
        df_clean["Status_NEMO"] = df_clean["Status_NEMO"].apply(
            lambda value: _display_value(
                value, STATUS_NEMO_BY_VALUE, STATUS_NEMO_BY_DISPLAY
            )
        )
        df_sanitized = df_clean.applymap(lambda val: html.escape(str(val)))
        df_sanitized["Status"] = df_clean["Status"].apply(
            lambda value: _badge_value(value, STATUS_BY_VALUE, STATUS_BY_DISPLAY)
        )
        df_sanitized["Status_NEMO"] = df_clean["Status_NEMO"].apply(
            lambda value: _badge_value(value, STATUS_NEMO_BY_VALUE, STATUS_NEMO_BY_DISPLAY)
        )
        st.markdown(
            f"<div class='status-table'>{df_sanitized.to_html(escape=False, index=False)}</div>",
            unsafe_allow_html=True,
        )
    else:
        st.info("No hay procesos registrados aún.")
