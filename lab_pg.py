import colorsys
import hashlib
import json
import re
import unicodedata
from datetime import datetime

import gspread
import pandas as pd
import streamlit as st
from gspread.utils import rowcol_to_a1
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
    "Status_Color",
    "Status_NEMO",
    "Status_NEMO_Color",
    "Tipo_alineador",
    "Fecha_recepcion",
    "Dias_entrega",
    "Comentarios",
    "Notas",
    "Responsable_SUD",
    "Fecha_inicio_SUD",
    "Hora_inicio_SUD",
    "Plantilla_superior",
    "Plantilla_inferior",
    "IPR",
    "No_alineadores_superior",
    "No_alineadores_inferior",
    "Total_alineadores",
    "Fecha_solicitud_envio",
    "Ultima_Modificacion",
]

RESPONSABLE_SUD_OPTIONS = [
    "Selecciona",
    "Arq Brenda",
    "Karen",
    "Melissa",
    "Georgina",
    "Carolina",
    "Daniela",
]

_STATUS_LABELS = [
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

_STATUS_NEMO_LABELS = [
    "Nuevo",
    "Carpeta específica",
    "Duplicado",
    "Seguimiento",
    "Solo impresión",
]


def _slugify_label(label: str) -> str:
    normalized = unicodedata.normalize("NFKD", label)
    ascii_label = "".join(c for c in normalized if not unicodedata.combining(c))
    ascii_label = ascii_label.lower()
    ascii_label = re.sub(r"[^a-z0-9]+", "_", ascii_label)
    ascii_label = ascii_label.strip("_")
    return ascii_label or "estado"


def _generate_color(value: str, *, saturation: float = 0.65, lightness: float = 0.55) -> str:
    digest = hashlib.sha1(value.encode("utf-8")).hexdigest()
    hue = int(digest[:6], 16) / 0xFFFFFF
    r, g, b = colorsys.hls_to_rgb(hue, lightness, saturation)
    return "#{:02X}{:02X}{:02X}".format(int(r * 255), int(g * 255), int(b * 255))


def _build_options(labels: list[str], prefix: str) -> list[dict[str, str]]:
    options: list[dict[str, str]] = []
    seen_values: set[str] = set()
    for label in labels:
        base_value = _slugify_label(label)
        value = base_value
        suffix = 1
        while value in seen_values:
            suffix += 1
            value = f"{base_value}_{suffix}"
        seen_values.add(value)
        color = _generate_color(f"{prefix}_{value}")
        options.append({"value": value, "label": label, "color": color})
    return options


def _build_mappings(options: list[dict[str, str]]) -> tuple[dict[str, dict[str, str]], dict[str, dict[str, str]]]:
    by_value = {opt["value"]: opt for opt in options}
    by_label = {opt["label"]: opt for opt in options}
    return by_value, by_label


def _build_select_css(options: list[dict[str, str]]) -> str:
    css_rules: list[str] = []
    base_selector = 'div[data-baseweb="select"]'
    for option in options:
        label = option["label"]
        color = option["color"]
        safe_label = label.replace('"', '\\"')
        selectors = [
            f'{base_selector} [role="option"][aria-label="{safe_label}"]',
            f'{base_selector} [role="option"][aria-label="{safe_label}"][aria-selected="true"]',
            f'{base_selector} [aria-live="polite"] span[title="{safe_label}"]',
            f'{base_selector} [aria-live="polite"] div[title="{safe_label}"]',
            f'{base_selector} div[data-baseweb="tag"][title="{safe_label}"]',
        ]
        selector_block = ",\n".join(selectors)
        css_rules.append(
            f"{selector_block} {{ color: {color} !important; }}"
        )
    return "\n".join(css_rules)


STATUS_OPTIONS = _build_options(_STATUS_LABELS, prefix="status")
STATUS_OPTIONS_BY_VALUE, STATUS_OPTIONS_BY_LABEL = _build_mappings(STATUS_OPTIONS)
STATUS_VALUES = [opt["value"] for opt in STATUS_OPTIONS]

STATUS_NEMO_OPTIONS = _build_options(_STATUS_NEMO_LABELS, prefix="status_nemo")
STATUS_NEMO_BY_VALUE, STATUS_NEMO_BY_LABEL = _build_mappings(STATUS_NEMO_OPTIONS)
STATUS_NEMO_VALUES = [opt["value"] for opt in STATUS_NEMO_OPTIONS]

STATUS_SELECTBOX_CSS = _build_select_css(STATUS_OPTIONS)
STATUS_NEMO_SELECTBOX_CSS = _build_select_css(STATUS_NEMO_OPTIONS)


def format_status_option(value: str | None) -> str:
    return _format_colored_option(
        value,
        STATUS_OPTIONS_BY_VALUE,
    )


def format_status_nemo_option(value: str | None) -> str:
    return _format_colored_option(
        value,
        STATUS_NEMO_BY_VALUE,
    )


def _format_colored_option(
    value: str | None,
    mapping: dict[str, dict[str, str]],
) -> str:
    if value is None:
        return ""
    option = mapping.get(value)
    if option is None:
        return str(value)
    return option["label"]


def _normalize_cell(value: str | None) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        text = value.strip()
    else:
        text = str(value)
    return "" if text.lower() == "nan" else text


def _resolve_option_data(
    raw_value: str | None,
    stored_color: str | None,
    *,
    by_value: dict[str, dict[str, str]],
    by_label: dict[str, dict[str, str]],
) -> tuple[str, str, str]:
    cleaned_value = _normalize_cell(raw_value)
    color = _normalize_cell(stored_color)
    option = by_value.get(cleaned_value)
    if option is None:
        option = by_label.get(cleaned_value)
    if option is None:
        return cleaned_value, cleaned_value, color
    resolved_color = color or option["color"]
    return option["value"], option["label"], resolved_color

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


def update_process(identifier, data_dict):
    df = fetch_df()
    if df.empty:
        return False

    row_idx = None
    current_no_orden = None

    if isinstance(identifier, tuple):
        if len(identifier) >= 2:
            current_no_orden = identifier[0]
            row_idx = identifier[1]
        elif identifier:
            current_no_orden = identifier[0]
    elif isinstance(identifier, dict):
        current_no_orden = identifier.get("No_orden")
        row_idx = identifier.get("row_index")
    else:
        current_no_orden = identifier

    if row_idx is not None:
        try:
            row_idx = int(row_idx)
        except (TypeError, ValueError):
            return False
        if row_idx not in df.index:
            return False
        row_series = df.loc[row_idx]
    else:
        procesos = df[df["No_orden"].astype(str).str.strip() != ""]
        if procesos.empty:
            return False

        current_no_orden = str(current_no_orden)
        matching = procesos[procesos["No_orden"] == current_no_orden]
        if matching.empty:
            return False
        row_idx = matching.index[0]
        row_series = matching.iloc[0]

    row_number = row_idx + 2  # encabezado +1

    start_col = COLUMNS.index("Responsable_SUD") + 1
    end_col = COLUMNS.index("Fecha_solicitud_envio") + 1

    row_values = [row_series.get(col, "") for col in COLUMNS]
    for col, value in data_dict.items():
        if col in COLUMNS:
            row_values[COLUMNS.index(col)] = str(value)

    ws = get_worksheet()
    start_cell = rowcol_to_a1(row_number, start_col)
    end_cell = rowcol_to_a1(row_number, end_col)
    ws.update(
        f"{start_cell}:{end_cell}",
        [row_values[start_col - 1 : end_col]],
    )
    ws.update(
        rowcol_to_a1(row_number, COLUMNS.index("Ultima_Modificacion") + 1),
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )
    return True

# ==============================
# 🚀 APP STREAMLIT
# ==============================
st.set_page_config(page_title="Procesos – ARTTDLAB", layout="wide")
st.title("🧪 Plataforma de Procesos – ARTTDLAB")

tab1, tab_sud, tab2 = st.tabs(["➕ Nuevo Proceso", "SUD", "📋 Consulta"])

# ➕ NUEVO PROCESO
with tab1:
    st.subheader("🆕 Registrar nuevo proceso")

    st.markdown(
        "<style>\n"
        f"{STATUS_SELECTBOX_CSS}\n"
        f"{STATUS_NEMO_SELECTBOX_CSS}\n"
        "</style>",
        unsafe_allow_html=True,
    )

    with st.form("form_nuevo"):
        in_paciente = st.text_input("🧑‍🦱 Nombre del paciente *")
        in_doctor = st.text_input("🧑‍⚕️ Nombre del doctor *")
        in_status = st.selectbox(
            "📌 Status *",
            STATUS_VALUES,
            format_func=format_status_option,
        )
        in_status_nemo = st.selectbox(
            "🌐 Status en NEMO *",
            STATUS_NEMO_VALUES,
            format_func=format_status_nemo_option,
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
        and in_paciente
        and in_doctor
        and in_status
        and in_status_nemo
        and in_tipo_alineador
        and in_fecha_recepcion
        and in_dias_entrega
    ):
        status_option = STATUS_OPTIONS_BY_VALUE.get(in_status)
        status_color = status_option["color"] if status_option else ""
        status_nemo_option = STATUS_NEMO_BY_VALUE.get(in_status_nemo)
        status_nemo_color = status_nemo_option["color"] if status_nemo_option else ""

        row = {
            "No_orden": "",
            "Nombre_paciente": in_paciente,
            "Nombre_doctor": in_doctor,
            "Status": in_status,
            "Status_Color": status_color,
            "Status_NEMO": in_status_nemo,
            "Status_NEMO_Color": status_nemo_color,
            "Tipo_alineador": in_tipo_alineador,
            "Fecha_recepcion": in_fecha_recepcion.strftime("%Y-%m-%d"),
            "Dias_entrega": str(int(in_dias_entrega)),
            "Comentarios": in_comentarios,
            "Notas": in_notas,
            "Responsable_SUD": "",
            "Fecha_inicio_SUD": "",
            "Hora_inicio_SUD": "",
            "Plantilla_superior": "",
            "Plantilla_inferior": "",
            "IPR": "",
            "No_alineadores_superior": "",
            "No_alineadores_inferior": "",
            "Total_alineadores": "",
            "Fecha_solicitud_envio": "",
            "Ultima_Modificacion": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        append_row(row)
        st.success("🎉 Proceso registrado correctamente.")
        st.cache_data.clear()
    elif enviado:
        st.error("⚠️ Por favor completa los campos obligatorios (*).")

# 🧩 SUD
with tab_sud:
    st.subheader("Gestión de SUD")
    df_sud = fetch_df()

    if df_sud.empty:
        st.info("No hay procesos disponibles para actualizar.")
    else:
        procesos = df_sud.copy()

        status_nemo_emojis = {
            "Nuevo": "🆕",
            "Carpeta específica": "📁",
            "Duplicado": "🧬",
            "Seguimiento": "📌",
            "Solo impresión": "🖨️",
        }

        def _parse_date(value):
            try:
                return datetime.strptime(value, "%Y-%m-%d").date()
            except Exception:
                return None

        def _parse_time(value):
            for fmt in ("%H:%M", "%H:%M:%S"):
                try:
                    return datetime.strptime(value, fmt).time()
                except Exception:
                    continue
            return None

        def _parse_int(value, default=0):
            if value is None:
                return default
            if isinstance(value, str):
                cleaned = value.strip()
                if not cleaned or cleaned.lower() == "nan":
                    return default
                value = cleaned
            try:
                return int(float(value))
            except (TypeError, ValueError):
                return default

        for idx, row in procesos.iterrows():
            no_orden = str(row.get("No_orden", "") or "").strip()
            if no_orden.lower() == "nan":
                no_orden = ""
            paciente = str(row.get("Nombre_paciente", "") or "").strip() or "Sin nombre"
            doctor = str(row.get("Nombre_doctor", "") or "").strip() or "Sin doctor"

            status_nemo_value, status_nemo_label, _ = _resolve_option_data(
                row.get("Status_NEMO"),
                row.get("Status_NEMO_Color"),
                by_value=STATUS_NEMO_BY_VALUE,
                by_label=STATUS_NEMO_BY_LABEL,
            )
            status_nemo_label = status_nemo_label or "Sin status NEMO"
            status_nemo_emoji = status_nemo_emojis.get(status_nemo_label, "❓")
            expander_title = f"{status_nemo_emoji} {paciente} – {doctor} • {status_nemo_label}"

            identifier = (no_orden if no_orden else None, idx)

            fecha_inicio_val = _parse_date(row.get("Fecha_inicio_SUD", ""))
            fecha_solicitud_val = _parse_date(row.get("Fecha_solicitud_envio", ""))
            hora_inicio_val = _parse_time(row.get("Hora_inicio_SUD", ""))

            responsable_default = row.get("Responsable_SUD", "")
            responsable_options = RESPONSABLE_SUD_OPTIONS.copy()
            if (
                responsable_default
                and responsable_default not in responsable_options
            ):
                responsable_options.append(responsable_default)
            responsable_index = (
                responsable_options.index(responsable_default)
                if responsable_default in responsable_options
                else 0
            )

            form_key = f"form_sud_{idx}"
            with st.form(form_key):
                with st.expander(expander_title, expanded=False):
                    st.caption(
                        f"No. orden: {no_orden if no_orden else 'Sin número de orden'} | "
                        f"Status NEMO ID: {status_nemo_value or 'N/D'}"
                    )

                    responsable = st.selectbox(
                        "Responsable hacer SUD *",
                        responsable_options,
                        index=responsable_index,
                        key=f"{form_key}_responsable",
                    )
                    fecha_inicio = st.date_input(
                        "Fecha inicio SUD *",
                        value=fecha_inicio_val or datetime.today().date(),
                        key=f"{form_key}_fecha_inicio",
                    )
                    hora_inicio = st.time_input(
                        "Hora de inicio *",
                        value=(
                            hora_inicio_val
                            or datetime.now()
                            .time()
                            .replace(second=0, microsecond=0)
                        ),
                        key=f"{form_key}_hora_inicio",
                    )
                    plantilla_superior = st.text_input(
                        "Plantilla superior *",
                        value=row.get("Plantilla_superior", ""),
                        key=f"{form_key}_plantilla_sup",
                    )
                    plantilla_inferior = st.text_input(
                        "Plantilla inferior *",
                        value=row.get("Plantilla_inferior", ""),
                        key=f"{form_key}_plantilla_inf",
                    )
                    ipr_default = row.get("IPR", "-") if row.get("IPR") else "-"
                    ipr = st.radio(
                        "IPR *",
                        options=["x", "-"],
                        index=0 if ipr_default == "x" else 1,
                        key=f"{form_key}_ipr",
                    )
                    no_sup = st.number_input(
                        "No. alineadores superior *",
                        min_value=0,
                        value=_parse_int(row.get("No_alineadores_superior", "0")),
                        step=1,
                        key=f"{form_key}_no_sup",
                    )
                    no_inf = st.number_input(
                        "No. alineadores inferior *",
                        min_value=0,
                        value=_parse_int(row.get("No_alineadores_inferior", "0")),
                        step=1,
                        key=f"{form_key}_no_inf",
                    )
                    total_alineadores = int(no_sup + no_inf)
                    st.number_input(
                        "Total alineadores",
                        min_value=0,
                        value=total_alineadores,
                        step=1,
                        disabled=True,
                        key=f"{form_key}_total",
                    )
                    fecha_solicitud = st.date_input(
                        "Fecha solicitud de envío *",
                        value=fecha_solicitud_val or datetime.today().date(),
                        key=f"{form_key}_fecha_solicitud",
                    )

                    guardar_sud = st.form_submit_button("Actualizar SUD")

                if guardar_sud:
                    errores = []
                    if responsable in ("", "Selecciona"):
                        errores.append("Selecciona un responsable.")
                    if not plantilla_superior.strip():
                        errores.append("Ingresa la plantilla superior.")
                    if not plantilla_inferior.strip():
                        errores.append("Ingresa la plantilla inferior.")

                    if errores:
                        st.error("\n".join(errores))
                    else:
                        data = {
                            "Responsable_SUD": responsable,
                            "Fecha_inicio_SUD": fecha_inicio.isoformat(),
                            "Hora_inicio_SUD": hora_inicio.strftime("%H:%M"),
                            "Plantilla_superior": plantilla_superior.strip(),
                            "Plantilla_inferior": plantilla_inferior.strip(),
                            "IPR": ipr,
                            "No_alineadores_superior": str(int(no_sup)),
                            "No_alineadores_inferior": str(int(no_inf)),
                            "Total_alineadores": str(total_alineadores),
                            "Fecha_solicitud_envio": fecha_solicitud.isoformat(),
                        }

                        if update_process(identifier, data):
                            st.success("Información SUD actualizada correctamente.")
                            st.cache_data.clear()
                        else:
                            st.error(
                                "No se encontró el proceso o no fue posible actualizar los datos."
                            )

# 📋 CONSULTA
with tab2:
    st.subheader("Listado de procesos")
    df = fetch_df()
    if df.empty:
        st.info("No hay procesos registrados aún.")
    else:
        procesos_labels = {}
        for idx, row in df.iterrows():
            no_orden = str(row.get("No_orden", "") or "").strip()
            if no_orden.lower() == "nan":
                no_orden = ""
            paciente = str(row.get("Nombre_paciente", "") or "").strip()
            paciente = paciente or "Sin nombre"
            if no_orden:
                label = f"{no_orden} – {paciente}"
            else:
                label = f"Sin No. orden – {paciente} (fila {idx + 2})"
            procesos_labels[idx] = label

        opciones = list(df.index)
        if not opciones:
            st.info("No hay procesos registrados aún.")
        else:
            default_selection = st.session_state.get("tab2_selected_idx", opciones[0])
            if default_selection not in opciones:
                default_selection = opciones[0]

            form_key = "form_editar_no_orden"
            input_key = "tab2_no_orden_input"

            with st.form(form_key):
                selected_idx = st.selectbox(
                    "Selecciona el proceso a editar",
                    opciones,
                    format_func=lambda x: procesos_labels.get(x, f"Fila {x + 2}"),
                    index=opciones.index(default_selection),
                )

                current_value = str(df.at[selected_idx, "No_orden"] or "").strip()
                if current_value.lower() == "nan":
                    current_value = ""

                stored_selection = st.session_state.get("tab2_selected_idx")
                if stored_selection != selected_idx or input_key not in st.session_state:
                    st.session_state[input_key] = current_value

                st.session_state["tab2_selected_idx"] = selected_idx

                nuevo_no_orden = st.text_input(
                    "No. orden",
                    key=input_key,
                )

                guardar_orden = st.form_submit_button("Guardar número de orden")

            if guardar_orden:
                ws = get_worksheet()
                row_number = selected_idx + 2
                col_number = COLUMNS.index("No_orden") + 1
                cell = rowcol_to_a1(row_number, col_number)
                valor = (nuevo_no_orden or "").strip()
                ws.update(cell, valor)
                ws.update(
                    rowcol_to_a1(row_number, COLUMNS.index("Ultima_Modificacion") + 1),
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                )
                st.success("Número de orden actualizado correctamente.")
                st.session_state[input_key] = valor
                st.cache_data.clear()

        df_display = df.copy()
        if not df_display.empty:
            status_info = df_display.apply(
                lambda row: _resolve_option_data(
                    row.get("Status"),
                    row.get("Status_Color"),
                    by_value=STATUS_OPTIONS_BY_VALUE,
                    by_label=STATUS_OPTIONS_BY_LABEL,
                ),
                axis=1,
                result_type="expand",
            )
            status_info.columns = ["Status_ID", "Status_Label", "Status_Color_Value"]
            status_idx = df_display.columns.get_loc("Status")
            df_display.insert(status_idx, "Status_ID", status_info["Status_ID"])
            df_display["Status"] = status_info["Status_Label"]
            df_display["Status_Color"] = status_info["Status_Color_Value"]

            nemo_info = df_display.apply(
                lambda row: _resolve_option_data(
                    row.get("Status_NEMO"),
                    row.get("Status_NEMO_Color"),
                    by_value=STATUS_NEMO_BY_VALUE,
                    by_label=STATUS_NEMO_BY_LABEL,
                ),
                axis=1,
                result_type="expand",
            )
            nemo_info.columns = [
                "Status_NEMO_ID",
                "Status_NEMO_Label",
                "Status_NEMO_Color_Value",
            ]
            nemo_idx = df_display.columns.get_loc("Status_NEMO")
            df_display.insert(nemo_idx, "Status_NEMO_ID", nemo_info["Status_NEMO_ID"])
            df_display["Status_NEMO"] = nemo_info["Status_NEMO_Label"]
            df_display["Status_NEMO_Color"] = nemo_info["Status_NEMO_Color_Value"]

        st.dataframe(df_display, use_container_width=True, height=600)
