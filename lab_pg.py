import streamlit as st
import pandas as pd
from datetime import datetime
import gspread
import json
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

STATUS_OPTIONS = [
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

STATUS_NEMO_OPTIONS = [
    "Nuevo",
    "Carpeta específica",
    "Duplicado",
    "Seguimiento",
    "Solo impresión",
]

STATUS_COLOR_PREFIXES = [
    ("1.", "#FFE0B2"),
    ("2.", "#FFF59D"),
    ("3.", "#D1C4E9"),
    ("4.", "#BBDEFB"),
    ("5.", "#C8E6C9"),
    ("6.", "#DCEDC8"),
    ("7.", "#E6EE9C"),
    ("8.", "#FFF9C4"),
    ("9.", "#FFE082"),
    ("10.", "#FFCC80"),
    ("11.", "#FFAB91"),
    ("12.", "#F8BBD0"),
    ("13.", "#F48FB1"),
    ("14.", "#CE93D8"),
    ("15.", "#B39DDB"),
    ("16.", "#9FA8DA"),
    ("17.", "#90CAF9"),
]

STATUS_COLOR_EXACT = {
    "Revisados": "#A5D6A7",
    "Falta Pago": "#EF9A9A",
    "Esperando respuesta del Dr.": "#FFE082",
    "REFINAMIENTO": "#80CBC4",
}

STATUS_NEMO_BADGE_COLORS = {
    "Nuevo": "#90CAF9",
    "Carpeta específica": "#CE93D8",
    "Duplicado": "#EF9A9A",
    "Seguimiento": "#FFE082",
    "Solo impresión": "#A5D6A7",
}

STATUS_BADGE_DEFAULT_COLOR = "#CFD8DC"

DISPLAY_COLUMN_MAP = {
    "Nombre_paciente": "Paciente",
    "Nombre_doctor": "Doctor",
    "Status_NEMO": "Status en NEMO",
    "Tipo_alineador": "Tipo de alineador",
    "Fecha_recepcion": "Fecha de recepción",
    "Dias_entrega": "Días de entrega",
    "Ultima_Modificacion": "Última modificación",
}

DISPLAY_COLUMNS_ORDER = [
    "Paciente",
    "Doctor",
    "Status",
    "Status en NEMO",
    "Tipo de alineador",
    "Fecha de recepción",
    "Días de entrega",
    "Comentarios",
    "Notas",
    "Última modificación",
]


def _is_light_color(hex_color: str) -> bool:
    if not hex_color:
        return True
    value = hex_color.lstrip("#")
    if len(value) != 6:
        return True
    try:
        r, g, b = (int(value[i : i + 2], 16) for i in (0, 2, 4))
    except ValueError:
        return True
    brightness = 0.299 * r + 0.587 * g + 0.114 * b
    return brightness > 180


def _badge_style_for_color(color: str) -> str:
    text_color = "#000000" if _is_light_color(color) else "#FFFFFF"
    return (
        f"background-color: {color};"
        "border-radius: 999px;"
        "padding: 0.15rem 0.6rem;"
        "font-weight: 600;"
        "text-align: center;"
        f"color: {text_color};"
    )


def _status_badge_style(value) -> str:
    if pd.isna(value):
        return ""
    status_text = str(value).strip()
    if not status_text:
        return ""
    color = next(
        (color for prefix, color in STATUS_COLOR_PREFIXES if status_text.startswith(prefix)),
        STATUS_COLOR_EXACT.get(status_text, STATUS_BADGE_DEFAULT_COLOR),
    )
    return _badge_style_for_color(color)


def _status_nemo_badge_style(value) -> str:
    if pd.isna(value):
        return ""
    status_text = str(value).strip()
    if not status_text:
        return ""
    color = STATUS_NEMO_BADGE_COLORS.get(status_text, STATUS_BADGE_DEFAULT_COLOR)
    return _badge_style_for_color(color)

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

tab1, tab2 = st.tabs(["➕ Nuevo Proceso", "📋 Consulta"])

# ➕ NUEVO PROCESO
with tab1:
    st.subheader("Registrar nuevo proceso")

    with st.form("form_nuevo"):
        in_orden = st.text_input("No. orden *")
        in_paciente = st.text_input("Nombre del paciente *")
        in_doctor = st.text_input("Nombre del doctor *")
        in_status = st.selectbox("Status *", STATUS_OPTIONS)
        in_status_nemo = st.selectbox("Status en NEMO *", STATUS_NEMO_OPTIONS)
        in_tipo_alineador = st.selectbox("Tipo de alineador *", ["Graphy", "Convencional"])
        in_fecha_recepcion = st.date_input("Fecha de recepción *", datetime.today())
        in_dias_entrega = st.number_input(
            "Días de entrega *", min_value=1, value=1, step=1
        )
        in_comentarios = st.text_area("Comentarios")
        in_notas = st.text_area("Notas")
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
            "Status": in_status,
            "Status_NEMO": in_status_nemo,
            "Tipo_alineador": in_tipo_alineador,
            "Fecha_recepcion": in_fecha_recepcion.strftime("%Y-%m-%d"),
            "Dias_entrega": str(int(in_dias_entrega)),
            "Comentarios": in_comentarios,
            "Notas": in_notas,
            "Ultima_Modificacion": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        append_row(row)
        st.success("✅ Proceso registrado correctamente.")
        st.cache_data.clear()
    elif enviado:
        st.error("❌ Por favor completa los campos obligatorios (*).")

# 📋 CONSULTA
with tab2:
    st.subheader("Listado de procesos")
    df = fetch_df()
    if not df.empty:
        display_df = df.drop(columns=["No_orden"], errors="ignore").copy()
        display_df = display_df.rename(columns=DISPLAY_COLUMN_MAP)
        if "Fecha de recepción" in display_df.columns:
            fechas_originales = display_df["Fecha de recepción"]
            fechas_convertidas = pd.to_datetime(
                fechas_originales, errors="coerce"
            )
            fechas_formateadas = fechas_convertidas.dt.strftime("%d/%m/%Y")
            display_df["Fecha de recepción"] = fechas_formateadas.where(
                ~fechas_convertidas.isna(), fechas_originales
            )
        display_df = display_df.fillna("")
        ordered_columns = [
            column for column in DISPLAY_COLUMNS_ORDER if column in display_df.columns
        ]
        if ordered_columns:
            display_df = display_df.loc[:, ordered_columns]
        styler = display_df.style
        if "Status" in display_df.columns:
            styler = styler.applymap(_status_badge_style, subset=["Status"])
        if "Status en NEMO" in display_df.columns:
            styler = styler.applymap(
                _status_nemo_badge_style, subset=["Status en NEMO"]
            )
        st.dataframe(styler, use_container_width=True, height=600, hide_index=True)
    else:
        st.info("No hay procesos registrados aún.")
