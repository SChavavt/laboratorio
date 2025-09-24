import colorsys
import hashlib
import json
import re
import unicodedata
from datetime import date, datetime, time
from functools import partial
from io import BytesIO

import gspread
import pandas as pd
import streamlit as st
from google.oauth2.service_account import Credentials
from gspread.cell import Cell

# ==============================
# 🔧 CONFIGURACIÓN
# ==============================
SCOPE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

DEFAULT_COLUMNS = [
    "No_orden",
    "Nombre_paciente",
    "Nombre_doctor",
    "Status",
    "Status_NEMO",
    "Tipo_alineador",
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

TAB_LABELS = ["➕ Nuevo Proceso", "SUD", "📋 Consulta"]
SUD_EXPANDERS_STATE_KEY = "sud_expanders_state"

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


def _ensure_unique_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Garantiza que todas las columnas del DataFrame tengan nombres únicos."""

    if df.empty or df.columns.is_unique:
        return df

    new_columns: list[str] = []
    counts: dict[str, int] = {}

    for original_name in df.columns:
        # Convertimos a string para evitar problemas con nombres no textuales.
        base_name = str(original_name).strip()
        if not base_name:
            base_name = "Columna"

        occurrence = counts.get(base_name, 0)
        if occurrence == 0 and base_name not in new_columns:
            new_name = base_name
        else:
            # Generamos sufijos incrementales hasta encontrar un nombre libre.
            suffix = occurrence + 1
            new_name = f"{base_name}_{suffix}"
            while new_name in counts or new_name in new_columns:
                suffix += 1
                new_name = f"{base_name}_{suffix}"
            counts[new_name] = 0

        counts[base_name] = occurrence + 1
        new_columns.append(new_name)

    df = df.copy()
    df.columns = new_columns
    return df


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
    *,
    by_value: dict[str, dict[str, str]],
    by_label: dict[str, dict[str, str]],
) -> tuple[str, str]:
    cleaned_value = _normalize_cell(raw_value)
    option = by_value.get(cleaned_value)
    if option is None:
        option = by_label.get(cleaned_value)
    if option is None:
        return cleaned_value, cleaned_value
    return option["value"], option["label"]


def _ensure_ui_state_defaults() -> None:
    if SUD_EXPANDERS_STATE_KEY not in st.session_state:
        st.session_state[SUD_EXPANDERS_STATE_KEY] = {}


def _normalize_expander_key(row_index) -> str | None:
    if row_index is None:
        return None
    try:
        return str(int(row_index))
    except (TypeError, ValueError):
        try:
            return str(row_index)
        except Exception:
            return None


def _focus_sud_tab(row_index=None) -> None:
    key = _normalize_expander_key(row_index)
    if key is None:
        return
    expanders_state = st.session_state.setdefault(SUD_EXPANDERS_STATE_KEY, {})
    for existing_key in list(expanders_state.keys()):
        expanders_state[existing_key] = existing_key == key
    expanders_state[key] = True


def _is_expander_marked_open(row_index) -> bool:
    key = _normalize_expander_key(row_index)
    if key is None:
        return False
    expanders_state = st.session_state.get(SUD_EXPANDERS_STATE_KEY, {})
    return bool(expanders_state.get(key))

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
        ws = ss.add_worksheet(title=ws_name, rows="1000", cols=str(len(DEFAULT_COLUMNS)))
        ws.update("A1", [DEFAULT_COLUMNS])
    return ws


@st.cache_data(ttl=30)
def fetch_columns():
    ws = get_worksheet()
    headers = ws.row_values(1)
    if headers:
        return headers
    ws.update("A1", [DEFAULT_COLUMNS])
    return DEFAULT_COLUMNS.copy()

@st.cache_data(ttl=30)
def fetch_df():
    ws = get_worksheet()
    columns = fetch_columns()
    values = ws.get_all_values()
    if not values:
        return pd.DataFrame(columns=columns)
    df = pd.DataFrame(values[1:], columns=values[0])
    required_columns = set(DEFAULT_COLUMNS)
    required_columns.add("Fecha_recepcion")
    for c in required_columns:
        if c not in df.columns:
            df[c] = ""
    ordered_columns = columns + [
        col for col in DEFAULT_COLUMNS if col not in columns
    ]
    extra_columns = [col for col in df.columns if col not in ordered_columns]
    ordered_columns.extend(extra_columns)
    return df[ordered_columns]

def append_row(row):
    ws = get_worksheet()
    columns = fetch_columns()
    values = [str(row.get(c, "")) if c in row else "" for c in columns]
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

    columns = fetch_columns()
    column_positions = {name: idx + 1 for idx, name in enumerate(columns)}

    updates: list[Cell] = []
    for col, value in data_dict.items():
        if col not in column_positions:
            continue
        updates.append(
            Cell(
                row=row_number,
                col=column_positions[col],
                value=_prepare_sheet_value(value),
            )
        )

    if "Ultima_Modificacion" in column_positions:
        updates.append(
            Cell(
                row=row_number,
                col=column_positions["Ultima_Modificacion"],
                value=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )
        )

    if not updates:
        return False

    ws = get_worksheet()
    ws.update_cells(updates)
    return True


def _prepare_sheet_value(value):
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, time):
        return value.strftime("%H:%M")
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    return str(value)


def _sync_total_alineadores(
    *,
    total_key: str,
    sup_key: str,
    inf_key: str,
    sup_value=None,
    inf_value=None,
):
    def _to_int(raw):
        if raw is None:
            return 0
        if isinstance(raw, str):
            raw = raw.strip()
            if not raw:
                return 0
        try:
            return int(raw)
        except (TypeError, ValueError):
            try:
                return int(float(raw))
            except (TypeError, ValueError):
                return 0

    sup_raw = sup_value if sup_value is not None else st.session_state.get(sup_key, 0)
    inf_raw = inf_value if inf_value is not None else st.session_state.get(inf_key, 0)

    total = _to_int(sup_raw) + _to_int(inf_raw)
    return {"Total_alineadores": total}


def persist_field_change(
    identifier,
    field_name,
    *,
    key,
    transform=None,
    extra_resolver=None,
):
    row_index = None
    if isinstance(identifier, tuple) and len(identifier) >= 2:
        row_index = identifier[1]
    elif isinstance(identifier, dict):
        row_index = identifier.get("row_index")

    _focus_sud_tab(row_index)

    raw_value = st.session_state.get(key)
    try:
        value = transform(raw_value) if transform else raw_value
    except Exception:
        st.error(f"No se pudo procesar el campo {field_name}.")
        return

    data = {field_name: _prepare_sheet_value(value)}

    if extra_resolver is not None:
        try:
            extra_data = extra_resolver(value)
        except Exception:
            st.error(
                f"Ocurrió un problema al calcular valores derivados para {field_name}."
            )
            return
        if extra_data:
            data.update(
                {
                    extra_field: _prepare_sheet_value(extra_value)
                    for extra_field, extra_value in extra_data.items()
                }
            )

    columns = fetch_columns()
    filtered_data = {
        col: val for col, val in data.items() if col in columns
    }
    if not filtered_data:
        st.warning(
            f"El campo {field_name} no existe en la hoja de cálculo actual."
        )
        return

    if update_process(identifier, filtered_data):
        st.cache_data.clear()
    else:
        st.error(f"No se pudo actualizar el campo {field_name}.")

# ==============================
# 🚀 APP STREAMLIT
# ==============================
st.set_page_config(page_title="Procesos – ARTTDLAB", layout="wide")
st.title("🧪 Plataforma de Procesos – ARTTDLAB")

_ensure_ui_state_defaults()

tab1, tab_sud, tab2 = st.tabs(TAB_LABELS)

sheet_columns = fetch_columns()

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
            "📌 Status",
            STATUS_VALUES,
            format_func=format_status_option,
        )
        in_status_nemo = st.selectbox(
            "🌐 Status en NEMO",
            STATUS_NEMO_VALUES,
            format_func=format_status_nemo_option,
        )
        in_tipo_alineador = st.selectbox(
            "🦷 Tipo de alineador", ["Graphy", "Convencional"]
        )
        in_fecha_recepcion = None
        if "Fecha_recepcion" in sheet_columns:
            in_fecha_recepcion = st.date_input(
                "📅 Fecha de recepción", datetime.today()
            )
        in_dias_entrega = st.number_input(
            "⏳ Días de entrega", min_value=1, value=1, step=1
        )
        in_comentarios = st.text_area("💬 Comentarios")
        in_notas = st.text_area("📝 Notas")
        enviado = st.form_submit_button("💾 Guardar")

    if enviado and in_paciente and in_doctor:
        row: dict[str, str] = {}

        if "No_orden" in sheet_columns:
            row["No_orden"] = ""
        if "Nombre_paciente" in sheet_columns:
            row["Nombre_paciente"] = in_paciente
        if "Nombre_doctor" in sheet_columns:
            row["Nombre_doctor"] = in_doctor
        if "Status" in sheet_columns:
            row["Status"] = in_status
        if "Status_NEMO" in sheet_columns:
            row["Status_NEMO"] = in_status_nemo
        if "Tipo_alineador" in sheet_columns:
            row["Tipo_alineador"] = in_tipo_alineador
        if "Fecha_recepcion" in sheet_columns and in_fecha_recepcion:
            row["Fecha_recepcion"] = in_fecha_recepcion.strftime("%Y-%m-%d")
        if "Dias_entrega" in sheet_columns:
            row["Dias_entrega"] = str(int(in_dias_entrega))
        if "Comentarios" in sheet_columns:
            row["Comentarios"] = in_comentarios
        if "Notas" in sheet_columns:
            row["Notas"] = in_notas
        if "Ultima_Modificacion" in sheet_columns:
            row["Ultima_Modificacion"] = datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            )

        append_row(row)
        st.success("🎉 Proceso registrado correctamente.")
        st.cache_data.clear()
    elif enviado:
        campos_faltantes = []
        if not in_paciente:
            campos_faltantes.append("Nombre del paciente")
        if not in_doctor:
            campos_faltantes.append("Nombre del doctor")

        if campos_faltantes:
            campos_texto = " y ".join(campos_faltantes)
            prefijo = "los campos obligatorios" if len(campos_faltantes) > 1 else "el campo obligatorio"
            st.error(f"⚠️ Por favor completa {prefijo}: {campos_texto}.")

# 🧩 SUD
with tab_sud:
    st.subheader("Gestión de SUD")
    df_sud = fetch_df()

    if df_sud.empty:
        st.info("No hay procesos disponibles para actualizar.")
    else:
        procesos = df_sud.copy()
        has_fecha_recepcion = "Fecha_recepcion" in sheet_columns

        st.markdown(
            "<style>\n"
            f"{STATUS_SELECTBOX_CSS}\n"
            f"{STATUS_NEMO_SELECTBOX_CSS}\n"
            "</style>",
            unsafe_allow_html=True,
        )

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

            status_value, status_label = _resolve_option_data(
                row.get("Status"),
                by_value=STATUS_OPTIONS_BY_VALUE,
                by_label=STATUS_OPTIONS_BY_LABEL,
            )
            status_options = STATUS_VALUES.copy()
            if status_value and status_value not in status_options:
                status_options.append(status_value)

            status_label_display = status_label or "Sin status"

            (
                status_nemo_value,
                status_nemo_label,
            ) = _resolve_option_data(
                row.get("Status_NEMO"),
                by_value=STATUS_NEMO_BY_VALUE,
                by_label=STATUS_NEMO_BY_LABEL,
            )
            status_nemo_options = STATUS_NEMO_VALUES.copy()
            if status_nemo_value and status_nemo_value not in status_nemo_options:
                status_nemo_options.append(status_nemo_value)
            status_nemo_label = status_nemo_label or "Sin status NEMO"
            status_nemo_emoji = status_nemo_emojis.get(status_nemo_label, "❓")
            expander_title = " • ".join(
                [
                    f"{status_nemo_emoji} {paciente} – {doctor}",
                    status_label_display,
                    status_nemo_label,
                ]
            )

            identifier = (no_orden if no_orden else None, idx)

            fecha_recepcion_val = None
            if has_fecha_recepcion:
                fecha_recepcion_val = _parse_date(row.get("Fecha_recepcion", ""))
            fecha_inicio_val = _parse_date(row.get("Fecha_inicio_SUD", ""))
            fecha_solicitud_val = _parse_date(row.get("Fecha_solicitud_envio", ""))
            hora_inicio_val = _parse_time(row.get("Hora_inicio_SUD", ""))

            dias_entrega_val = max(
                1, _parse_int(row.get("Dias_entrega", "1"), default=1)
            )
            comentarios_default = _normalize_cell(row.get("Comentarios", ""))
            notas_default = _normalize_cell(row.get("Notas", ""))

            tipo_alineador_default = _normalize_cell(row.get("Tipo_alineador", ""))
            tipo_alineador_default = tipo_alineador_default or "Graphy"
            tipo_alineador_options = ["Graphy", "Convencional"]
            if tipo_alineador_default not in tipo_alineador_options:
                tipo_alineador_options.append(tipo_alineador_default)

            status_index = (
                status_options.index(status_value)
                if status_value in status_options
                else 0
            )
            status_nemo_index = (
                status_nemo_options.index(status_nemo_value)
                if status_nemo_value in status_nemo_options
                else 0
            )
            tipo_alineador_index = (
                tipo_alineador_options.index(tipo_alineador_default)
                if tipo_alineador_default in tipo_alineador_options
                else 0
            )

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
            with st.expander(
                expander_title,
                expanded=_is_expander_marked_open(idx),
            ):
                st.caption(
                    f"No. orden: {no_orden if no_orden else 'Sin número de orden'} | "
                    f"Status NEMO ID: {status_nemo_value or 'N/D'}"
                )

                col_left, col_right = st.columns(2)
                save_field = partial(persist_field_change, identifier)

                with col_left:
                    no_orden_key = f"{form_key}_no_orden"
                    st.text_input(
                        "🔢 No. orden",
                        value=no_orden,
                        key=no_orden_key,
                        on_change=save_field,
                        args=("No_orden",),
                        kwargs={
                            "key": no_orden_key,
                            "transform": lambda v: (v or "").strip(),
                        },
                    )

                    status_key = f"{form_key}_status"
                    st.selectbox(
                        "📌 Status",
                        status_options,
                        index=status_index,
                        format_func=format_status_option,
                        key=status_key,
                        on_change=save_field,
                        args=("Status",),
                        kwargs={"key": status_key},
                    )

                    status_nemo_key = f"{form_key}_status_nemo"
                    st.selectbox(
                        "🌐 Status en NEMO",
                        status_nemo_options,
                        index=status_nemo_index,
                        format_func=format_status_nemo_option,
                        key=status_nemo_key,
                        on_change=save_field,
                        args=("Status_NEMO",),
                        kwargs={"key": status_nemo_key},
                    )

                    tipo_alineador_key = f"{form_key}_tipo_alineador"
                    st.selectbox(
                        "🦷 Tipo de alineador",
                        tipo_alineador_options,
                        index=tipo_alineador_index,
                        key=tipo_alineador_key,
                        on_change=save_field,
                        args=("Tipo_alineador",),
                        kwargs={"key": tipo_alineador_key},
                    )

                    if has_fecha_recepcion:
                        fecha_recepcion_key = f"{form_key}_fecha_recepcion"
                        st.date_input(
                            "📅 Fecha de recepción",
                            value=fecha_recepcion_val or datetime.today().date(),
                            key=fecha_recepcion_key,
                            on_change=save_field,
                            args=("Fecha_recepcion",),
                            kwargs={"key": fecha_recepcion_key},
                        )

                    dias_entrega_key = f"{form_key}_dias_entrega"
                    st.number_input(
                        "⏳ Días de entrega",
                        min_value=1,
                        value=int(dias_entrega_val),
                        step=1,
                        key=dias_entrega_key,
                        on_change=save_field,
                        args=("Dias_entrega",),
                        kwargs={
                            "key": dias_entrega_key,
                            "transform": lambda v: int(v) if v is not None else 0,
                        },
                    )

                    comentarios_key = f"{form_key}_comentarios"
                    st.text_area(
                        "💬 Comentarios",
                        value=comentarios_default,
                        key=comentarios_key,
                        on_change=save_field,
                        args=("Comentarios",),
                        kwargs={"key": comentarios_key},
                        height=150,
                    )

                    notas_key = f"{form_key}_notas"
                    st.text_area(
                        "📝 Notas",
                        value=notas_default,
                        key=notas_key,
                        on_change=save_field,
                        args=("Notas",),
                        kwargs={"key": notas_key},
                        height=150,
                    )

                with col_right:
                    responsable_key = f"{form_key}_responsable"
                    st.selectbox(
                        "🧑‍🔧 Responsable hacer SUD",
                        responsable_options,
                        index=responsable_index,
                        key=responsable_key,
                        on_change=save_field,
                        args=("Responsable_SUD",),
                        kwargs={
                            "key": responsable_key,
                            "transform": lambda v: "" if v in ("", "Selecciona") else v,
                        },
                    )

                    fecha_inicio_key = f"{form_key}_fecha_inicio"
                    st.date_input(
                        "📅 Fecha inicio SUD",
                        value=fecha_inicio_val or datetime.today().date(),
                        key=fecha_inicio_key,
                        on_change=save_field,
                        args=("Fecha_inicio_SUD",),
                        kwargs={"key": fecha_inicio_key},
                    )

                    hora_inicio_key = f"{form_key}_hora_inicio"
                    st.time_input(
                        "⏰ Hora de inicio",
                        value=(
                            hora_inicio_val
                            or datetime.now().time().replace(second=0, microsecond=0)
                        ),
                        key=hora_inicio_key,
                        on_change=save_field,
                        args=("Hora_inicio_SUD",),
                        kwargs={"key": hora_inicio_key},
                    )

                    plantilla_sup_key = f"{form_key}_plantilla_sup"
                    st.text_input(
                        "📄 Plantilla superior",
                        value=row.get("Plantilla_superior", ""),
                        key=plantilla_sup_key,
                        on_change=save_field,
                        args=("Plantilla_superior",),
                        kwargs={
                            "key": plantilla_sup_key,
                            "transform": lambda v: v.strip() if isinstance(v, str) else v,
                        },
                    )

                    plantilla_inf_key = f"{form_key}_plantilla_inf"
                    st.text_input(
                        "📄 Plantilla inferior",
                        value=row.get("Plantilla_inferior", ""),
                        key=plantilla_inf_key,
                        on_change=save_field,
                        args=("Plantilla_inferior",),
                        kwargs={
                            "key": plantilla_inf_key,
                            "transform": lambda v: v.strip() if isinstance(v, str) else v,
                        },
                    )

                    ipr_value = row.get("IPR")
                    ipr_default = "No"
                    if isinstance(ipr_value, bool):
                        ipr_default = "Sí" if ipr_value else "No"
                    elif ipr_value is not None:
                        normalized_ipr = (
                            unicodedata.normalize("NFKD", ipr_value)
                            if isinstance(ipr_value, str)
                            else str(ipr_value)
                        )
                        normalized_ipr = "".join(
                            c for c in normalized_ipr if not unicodedata.combining(c)
                        ).strip().lower()
                        if normalized_ipr in {"x", "si", "s", "true", "1", "yes"}:
                            ipr_default = "Sí"

                    ipr_key = f"{form_key}_ipr"
                    ipr_options = ["Sí", "No"]
                    ipr_index = (
                        ipr_options.index(ipr_default) if ipr_default in ipr_options else 1
                    )

                    st.radio(
                        "⚙️ IPR",
                        options=ipr_options,
                        index=ipr_index,
                        key=ipr_key,
                        on_change=save_field,
                        args=("IPR",),
                        kwargs={
                            "key": ipr_key,
                            "transform": lambda v: "Sí" if v == "Sí" else "No",
                        },
                    )

                    total_key = f"{form_key}_total_alineadores"
                    no_sup_key = f"{form_key}_no_sup"
                    no_sup = st.number_input(
                        "🔢 No. alineadores superior",
                        min_value=0,
                        value=_parse_int(row.get("No_alineadores_superior", "0")),
                        step=1,
                        key=no_sup_key,
                        on_change=save_field,
                        args=("No_alineadores_superior",),
                        kwargs={
                            "key": no_sup_key,
                            "transform": lambda v: int(v) if v is not None else 0,
                            "extra_resolver": lambda value,
                            sup_key=no_sup_key,
                            inf_key=f"{form_key}_no_inf",
                            total_key=total_key: _sync_total_alineadores(
                                total_key=total_key,
                                sup_key=sup_key,
                                inf_key=inf_key,
                                sup_value=value,
                            ),
                        },
                    )

                    no_inf_key = f"{form_key}_no_inf"
                    no_inf = st.number_input(
                        "🔢 No. alineadores inferior",
                        min_value=0,
                        value=_parse_int(row.get("No_alineadores_inferior", "0")),
                        step=1,
                        key=no_inf_key,
                        on_change=save_field,
                        args=("No_alineadores_inferior",),
                        kwargs={
                            "key": no_inf_key,
                            "transform": lambda v: int(v) if v is not None else 0,
                            "extra_resolver": lambda value,
                            sup_key=no_sup_key,
                            inf_key=no_inf_key,
                            total_key=total_key: _sync_total_alineadores(
                                total_key=total_key,
                                sup_key=sup_key,
                                inf_key=inf_key,
                                inf_value=value,
                            ),
                        },
                    )

                    try:
                        total_calculado = int(no_sup) + int(no_inf)
                    except (TypeError, ValueError):
                        total_calculado = 0

                    st.number_input(
                        "🧮 Total alineadores",
                        min_value=0,
                        value=total_calculado,
                        step=1,
                        disabled=True,
                        key=total_key,
                    )

                    fecha_solicitud_key = f"{form_key}_fecha_solicitud"
                    st.date_input(
                        "📬 Fecha solicitud de envío",
                        value=fecha_solicitud_val or datetime.today().date(),
                        key=fecha_solicitud_key,
                        on_change=save_field,
                        args=("Fecha_solicitud_envio",),
                        kwargs={"key": fecha_solicitud_key},
                    )

# 📋 CONSULTA
with tab2:
    st.subheader("Listado de procesos")
    df = fetch_df()
    if df.empty:
        st.info("No hay procesos registrados aún.")
    else:
        df_display = df.copy()
        if not df_display.empty:
            status_info = df_display.apply(
                lambda row: _resolve_option_data(
                    row.get("Status"),
                    by_value=STATUS_OPTIONS_BY_VALUE,
                    by_label=STATUS_OPTIONS_BY_LABEL,
                ),
                axis=1,
                result_type="expand",
            )
            status_info.columns = [
                "Status_Value",
                "Status_Label",
            ]
            df_display["Status"] = status_info["Status_Label"]

            nemo_info = df_display.apply(
                lambda row: _resolve_option_data(
                    row.get("Status_NEMO"),
                    by_value=STATUS_NEMO_BY_VALUE,
                    by_label=STATUS_NEMO_BY_LABEL,
                ),
                axis=1,
                result_type="expand",
            )
            nemo_info.columns = [
                "Status_NEMO_Value",
                "Status_NEMO_Label",
            ]
            df_display["Status_NEMO"] = nemo_info["Status_NEMO_Label"]

        excel_buffer = BytesIO()
        df_display = df_display.drop(
            columns=["Status_Color", "Status_NEMO_Color"], errors="ignore"
        )
        df_display = _ensure_unique_column_names(df_display)
        df_display.to_excel(excel_buffer, index=False, sheet_name="Procesos")
        excel_buffer.seek(0)

        st.download_button(
            "⬇️ Descargar Excel",
            data=excel_buffer,
            file_name=f"procesos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        st.dataframe(df_display, use_container_width=True, height=600)
