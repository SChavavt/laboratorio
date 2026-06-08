import json
import math
import unicodedata
from datetime import date, datetime
from typing import Any

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

SHEET_ESTATUS = "ESTATUS APARATOS"
SHEET_PROCESOS = "PROCESOS POR APARATO"
SHEET_TIEMPOS = "TIEMPOS_APARATOS"
ID_COLUMN = "Columna 1"
STATUS_COLUMN = "STATUS"

TIEMPOS_HEADERS = [
    "ID_LOG",
    "Columna 1",
    "APARATO",
    "FASE_ORDEN",
    "STATUS",
    "STATUS_SIGUIENTE",
    "RESPONSABLE",
    "USUARIO",
    "FECHA_INICIO",
    "HORA_INICIO",
    "FECHA_FIN",
    "HORA_FIN",
    "DURACION_HORAS",
    "TIEMPO_MAXIMO_HORAS",
    "ESTADO_ALERTA",
    "COMENTARIOS_CAMBIO",
    "FECHA_REGISTRO_LOG",
]

ACTIVE_USER_LABEL = "Usuario Streamlit"

APARATO_OPTIONS = [
    "MSE",
    "TIGER",
    "REVERSE",
    "HYRAX",
    "TRAMPA LINGUAL",
    "LEONE",
    "DISTALIZADOR",
]

STATUS_OPTIONS = [
    "REVISION DE ARCHIVOS",
    "EN PLANEACIÓN",
    "REVISIÓN DEL DISEÑO POR DR",
    "SOLICITUD DE CAMBIOS",
    "ELABORACIÓN PLATINA BANDAS",
    "ESPERANDO STL PSM",
    "PDTE ENVIO PSM + GUIA",
    "LISTO P/SINTERIZADO",
    "EN SINTERIZADO",
    "LISTO P/CONFECCIÓN",
    "EN CONFECCIÓN",
    "LISTO P/ENVÍO",
    "ENVIADO",
    "FALTA PAGO COMPLETO",
    "CONFECCION EN PAUSA",
    "CANCELO",
]

VENDEDOR_OPTIONS = [
    "JIMENA",
    "JUAN",
    "MICHELLE",
    "IGNACIO",
    "KAREN",
    "ALEJANDRA",
    "CARO",
    "HECTOR",
    "DANIELA",
]

SERVICIO_OPTIONS = [
    "PLANEACIÓN COMPLETA",
    "CONFECCIÓN",
    "PLANEACIÓN & CONFECCIÓN",
    "DISEÑO & CONFECCION",
    "PLANEACION",
]

ARCHIVOS_RECIBIDOS_OPTIONS = [
    "STL",
    "TOMOGRAFÍA",
    "STL+TOMO",
]

PAGO_OPTIONS = [
    "ANTICIPO",
    "TOTAL",
    "SIN PAGO",
    "CANCELO",
]

SELECTBOX_OPTIONS_BY_COLUMN = {
    "APARATO": APARATO_OPTIONS,
    STATUS_COLUMN: STATUS_OPTIONS,
    "VENDEDOR": VENDEDOR_OPTIONS,
    "SERVICIO": SERVICIO_OPTIONS,
    "ARCHIVOS RECIBIDOS": ARCHIVOS_RECIBIDOS_OPTIONS,
    "PAGO": PAGO_OPTIONS,
}

DATE_COLUMNS = {
    "FECHA DE RECEPCIÓN",
    "FECHA PAGO PLANEACION",
    "FECHA PAGO CONFECCION",
    "FECHA PARA ENTREGA",
    "FECHA IMPRESIÓN",
    "FECHA ENVÍO",
}

DATETIME_TEXT_COLUMNS = {
    "FECHA/HORA ENVÍO STEFANO",
    "FECHA/HORA ENTREGA STEFANO",
}

TEXT_AREA_COLUMNS = {
    "DETALLE COMENTARIOS",
    "DETALLES & COMENTARIOS FINALES",
}

SPANISH_MONTHS = {
    "ENERO": 1,
    "FEBRERO": 2,
    "MARZO": 3,
    "ABRIL": 4,
    "MAYO": 5,
    "JUNIO": 6,
    "JULIO": 7,
    "AGOSTO": 8,
    "SEPTIEMBRE": 9,
    "SETIEMBRE": 9,
    "OCTUBRE": 10,
    "NOVIEMBRE": 11,
    "DICIEMBRE": 12,
}


# ==============================
# 🧰 UTILIDADES GENERALES
# ==============================
def normalize_text(value: Any) -> str:
    """Normaliza texto para comparaciones tolerantes a mayúsculas y acentos."""

    if value is None:
        return ""
    text = str(value).strip()
    normalized = unicodedata.normalize("NFKD", text)
    without_accents = "".join(
        char for char in normalized if not unicodedata.combining(char)
    )
    return without_accents.upper()


def parse_simple_date(value: Any) -> date | None:
    """Convierte fechas simples de Sheets a date cuando es seguro hacerlo."""

    text = clean_cell(value).strip()
    if not text:
        return None

    normalized = normalize_text(text).replace("/", "-")
    parts = normalized.split()
    if len(parts) >= 2 and parts[0].isdigit() and parts[1] in SPANISH_MONTHS:
        year = datetime.now().year
        if len(parts) >= 3 and parts[2].isdigit():
            year = int(parts[2])
        try:
            return date(year, SPANISH_MONTHS[parts[1]], int(parts[0]))
        except ValueError:
            return None

    parsed = pd.to_datetime(text, errors="coerce", dayfirst=True)
    if pd.isna(parsed):
        return None
    return parsed.date()


def is_numeric_value(value: Any) -> bool:
    """Indica si un valor de celda puede editarse como número."""

    text = clean_cell(value).strip()
    if not text:
        return False
    try:
        float(text.replace(",", "."))
    except ValueError:
        return False
    return True


def build_selectbox_options(fixed_options: list[str], current_value: str) -> list[str]:
    """Incluye temporalmente valores existentes que no están en el catálogo fijo."""

    options = ["", *fixed_options]
    if current_value and current_value not in options:
        options.append(current_value)
    return options


def clean_cell(value: Any) -> str:
    """Convierte celdas vacías/NaN en string vacío."""

    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    text = str(value)
    return "" if text.strip().lower() == "nan" else text


def prepare_sheet_value(value: Any) -> str:
    """Formatea valores para enviarlos de forma segura a Google Sheets."""

    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    return str(value)


def ensure_unique_column_names(columns: list[str]) -> list[str]:
    """Evita columnas duplicadas al mostrar hojas horizontales en DataFrame."""

    counts: dict[str, int] = {}
    unique_columns: list[str] = []
    for index, raw_column in enumerate(columns):
        base = str(raw_column).strip() or f"Columna_{index + 1}"
        count = counts.get(base, 0)
        column = base if count == 0 else f"{base}_{count + 1}"
        while column in unique_columns:
            count += 1
            column = f"{base}_{count + 1}"
        counts[base] = count + 1
        unique_columns.append(column)
    return unique_columns


def parse_start_datetime(fecha: Any, hora: Any) -> datetime | None:
    """Intenta construir un datetime desde fecha y hora de la hoja de tiempos."""

    fecha_text = clean_cell(fecha).strip()
    hora_text = clean_cell(hora).strip()
    if not fecha_text:
        return None

    combined = f"{fecha_text} {hora_text}".strip()
    parsed = pd.to_datetime(combined, errors="coerce", dayfirst=True)
    if pd.isna(parsed):
        parsed = pd.to_datetime(fecha_text, errors="coerce", dayfirst=True)
    if pd.isna(parsed):
        return None
    return parsed.to_pydatetime()


def calculate_duration_hours(start_dt: datetime | None, end_dt: datetime | None = None) -> str:
    """Calcula duración decimal en horas entre dos momentos."""

    if start_dt is None:
        return ""
    end_dt = end_dt or datetime.now()
    duration = end_dt - start_dt
    hours = duration.total_seconds() / 3600
    return f"{max(hours, 0):.2f}"


def calculate_alert_state(
    fecha_inicio: Any,
    hora_inicio: Any,
    tiempo_maximo_horas: Any,
    *,
    now: datetime | None = None,
) -> str:
    """Calcula el estado visual de alerta para un registro activo."""

    max_time_text = clean_cell(tiempo_maximo_horas).strip()
    if not max_time_text:
        return "Sin tiempo configurado"

    try:
        max_hours = float(max_time_text.replace(",", "."))
    except ValueError:
        return "Sin tiempo configurado"

    if max_hours <= 0:
        return "Sin tiempo configurado"

    start_dt = parse_start_datetime(fecha_inicio, hora_inicio)
    if start_dt is None:
        return "Sin fecha de inicio"

    now = now or datetime.now()
    elapsed_hours = (now - start_dt).total_seconds() / 3600
    if elapsed_hours >= max_hours:
        return "Atrasado"
    if elapsed_hours >= max_hours * 0.8:
        return "Próximo a vencer"
    return "En tiempo"


# ==============================
# 🔐 CLIENTE GOOGLE SHEETS
# ==============================
def _get_gs_client():
    creds_str = st.secrets["gsheets"]["google_credentials"]
    creds_info = json.loads(creds_str)
    credentials = Credentials.from_service_account_info(creds_info, scopes=SCOPE)
    return gspread.authorize(credentials)


@st.cache_resource
def get_spreadsheet():
    client = _get_gs_client()
    sheet_id = st.secrets["gsheets"]["sheet_id"]
    return client.open_by_key(sheet_id)


@st.cache_resource
def get_worksheet(sheet_name: str):
    spreadsheet = get_spreadsheet()
    try:
        return spreadsheet.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        if sheet_name != SHEET_TIEMPOS:
            raise
        return spreadsheet.add_worksheet(
            title=SHEET_TIEMPOS,
            rows="1000",
            cols=str(len(TIEMPOS_HEADERS)),
        )


def ensure_tiempos_headers() -> None:
    """Asegura encabezados de TIEMPOS_APARATOS sin borrar datos existentes."""

    worksheet = get_worksheet(SHEET_TIEMPOS)
    headers = worksheet.row_values(1)
    if not headers:
        worksheet.update("A1", [TIEMPOS_HEADERS])
        st.cache_data.clear()
        return

    missing_headers = [header for header in TIEMPOS_HEADERS if header not in headers]
    if missing_headers:
        worksheet.update("A1", [headers + missing_headers])
        st.cache_data.clear()


@st.cache_data(ttl=30)
def read_sheet_df(sheet_name: str) -> pd.DataFrame:
    """Lee una hoja como DataFrame sin alterar sus encabezados existentes."""

    worksheet = get_worksheet(sheet_name)
    values = worksheet.get_all_values()
    if not values:
        if sheet_name == SHEET_TIEMPOS:
            return pd.DataFrame(columns=TIEMPOS_HEADERS)
        return pd.DataFrame()

    header_row_index = 1 if sheet_name == SHEET_ESTATUS else 0
    data_start_index = header_row_index + 1
    if len(values) <= header_row_index:
        return pd.DataFrame(columns=TIEMPOS_HEADERS if sheet_name == SHEET_TIEMPOS else [])

    headers = values[header_row_index]
    if not headers:
        return pd.DataFrame(columns=TIEMPOS_HEADERS if sheet_name == SHEET_TIEMPOS else [])

    unique_headers = ensure_unique_column_names(headers)
    width = len(unique_headers)
    normalized_rows = [
        row[:width] + [""] * max(width - len(row), 0)
        for row in values[data_start_index:]
    ]
    df = pd.DataFrame(normalized_rows, columns=unique_headers)

    if sheet_name == SHEET_ESTATUS and not df.empty:
        non_empty_rows = df.apply(
            lambda row: any(clean_cell(value).strip() for value in row),
            axis=1,
        )
        df = df[non_empty_rows]
        if ID_COLUMN in df.columns:
            has_identifier = df[ID_COLUMN].apply(lambda value: bool(clean_cell(value).strip()))
            df = df[has_identifier]
        df = df.reset_index(drop=True)

    return df


@st.cache_data(ttl=30)
def read_sheet_values(sheet_name: str) -> list[list[str]]:
    """Lee valores crudos para cálculos que dependen de estructura horizontal."""

    return get_worksheet(sheet_name).get_all_values()


def update_row_by_columna_1(identifier: str, changes: dict[str, Any]) -> bool:
    """Actualiza solo las celdas modificadas de ESTATUS APARATOS por Columna 1."""

    if not changes:
        return False

    worksheet = get_worksheet(SHEET_ESTATUS)
    values = worksheet.get_all_values()
    if not values:
        return False

    if len(values) < 2:
        return False

    headers = values[1]
    if ID_COLUMN not in headers:
        return False

    id_index = headers.index(ID_COLUMN)
    target_row_number: int | None = None
    for row_number, row in enumerate(values[2:], start=3):
        row_identifier = row[id_index] if id_index < len(row) else ""
        if clean_cell(row_identifier).strip() == clean_cell(identifier).strip():
            target_row_number = row_number
            break

    if target_row_number is None:
        return False

    column_positions = {column: index + 1 for index, column in enumerate(headers)}
    updates = [
        Cell(target_row_number, column_positions[column], prepare_sheet_value(value))
        for column, value in changes.items()
        if column in column_positions
    ]
    if not updates:
        return False

    worksheet.update_cells(updates, value_input_option="USER_ENTERED")
    return True




def find_first_empty_estatus_row() -> int:
    """Encuentra la primera fila de ESTATUS APARATOS con Columna 1 vacía."""

    worksheet = get_worksheet(SHEET_ESTATUS)
    values = worksheet.get_all_values()

    if len(values) < 2:
        raise ValueError("No se encontraron encabezados en la fila 2 de ESTATUS APARATOS.")

    headers = values[1]
    if ID_COLUMN not in headers:
        raise ValueError(f"No se encontró la columna {ID_COLUMN} en ESTATUS APARATOS.")

    id_col_index = headers.index(ID_COLUMN)
    for row_number, row in enumerate(values[2:], start=3):
        current_id = row[id_col_index] if id_col_index < len(row) else ""
        if not clean_cell(current_id).strip():
            return row_number

    return len(values) + 1


def append_estatus_row(row_dict: dict[str, Any]) -> None:
    """Escribe un nuevo pedido en el primer renglón disponible de ESTATUS APARATOS."""

    worksheet = get_worksheet(SHEET_ESTATUS)
    headers = worksheet.row_values(2)
    if not headers:
        raise ValueError("No se encontraron encabezados en la fila 2 de ESTATUS APARATOS.")

    target_row = find_first_empty_estatus_row()
    allowed_new_order_fields = {
        ID_COLUMN,
        "APARATO",
        STATUS_COLUMN,
        "NOMBRE DOCTOR",
        "NOMBRE PACIENTE",
        "DETALLE COMENTARIOS",
        "VENDEDOR",
        "SERVICIO",
        "PAGO",
        "DÍAS DE ENTREGA",
        "FECHA PARA ENTREGA",
    }
    column_positions = {header: index + 1 for index, header in enumerate(headers)}

    updates = []
    for column, value in row_dict.items():
        if column not in allowed_new_order_fields:
            continue
        if column not in column_positions:
            continue
        updates.append(
            Cell(
                row=target_row,
                col=column_positions[column],
                value=prepare_sheet_value(value),
            )
        )

    if not updates:
        raise ValueError("No hay campos válidos para guardar en ESTATUS APARATOS.")

    worksheet.update_cells(updates, value_input_option="USER_ENTERED")


def columna_1_exists(identifier: str) -> bool:
    """Revisa si ya existe un identificador en Columna 1 de ESTATUS APARATOS."""

    cleaned_identifier = clean_cell(identifier).strip()
    if not cleaned_identifier:
        return False

    values = get_worksheet(SHEET_ESTATUS).get_all_values()
    if len(values) < 2 or ID_COLUMN not in values[1]:
        return False

    id_index = values[1].index(ID_COLUMN)
    for row in values[2:]:
        existing_identifier = row[id_index] if id_index < len(row) else ""
        if clean_cell(existing_identifier).strip() == cleaned_identifier:
            return True
    return False


def get_next_log_id(tiempos_df: pd.DataFrame) -> int:
    """Devuelve el siguiente ID_LOG consecutivo."""

    if tiempos_df.empty or "ID_LOG" not in tiempos_df.columns:
        return 1
    numeric_ids = pd.to_numeric(tiempos_df["ID_LOG"], errors="coerce").dropna()
    if numeric_ids.empty:
        return 1
    return int(numeric_ids.max()) + 1


def find_tiempo_maximo_horas(apparatus: str, status: str) -> str:
    """Busca de forma flexible el tiempo máximo en PROCESOS POR APARATO."""

    values = read_sheet_values(SHEET_PROCESOS)
    if not values or not apparatus or not status:
        return ""

    apparatus_norm = normalize_text(apparatus)
    status_norm = normalize_text(status)
    header = values[0]

    for start_index, header_value in enumerate(header):
        if apparatus_norm and apparatus_norm not in normalize_text(header_value):
            continue

        next_group_index = len(header)
        for candidate_index in range(start_index + 1, len(header)):
            candidate = normalize_text(header[candidate_index])
            if candidate and "FASE" not in candidate and "TIEMPO" not in candidate:
                next_group_index = candidate_index
                break

        group_headers = header[start_index:next_group_index]
        phase_offsets = [
            offset for offset, column in enumerate(group_headers) if "FASE" in normalize_text(column)
        ]
        time_offsets = [
            offset for offset, column in enumerate(group_headers) if "TIEMPO" in normalize_text(column)
        ]
        if not phase_offsets or not time_offsets:
            phase_offsets = [0]
            time_offsets = [1] if next_group_index - start_index > 1 else []

        for row in values[1:]:
            for phase_offset in phase_offsets:
                phase_index = start_index + phase_offset
                phase_value = row[phase_index] if phase_index < len(row) else ""
                if normalize_text(phase_value) != status_norm:
                    continue

                time_index = None
                later_times = [offset for offset in time_offsets if offset > phase_offset]
                if later_times:
                    time_index = start_index + later_times[0]
                elif time_offsets:
                    time_index = start_index + time_offsets[0]

                if time_index is not None and time_index < len(row):
                    return clean_cell(row[time_index]).strip()
    return ""


def close_previous_active_time(identifier: str) -> bool:
    """Cierra el registro activo anterior de TIEMPOS_APARATOS, si existe."""

    worksheet = get_worksheet(SHEET_TIEMPOS)
    values = worksheet.get_all_values()
    if len(values) <= 1:
        return False

    headers = values[0]
    required = [ID_COLUMN, "FECHA_INICIO", "HORA_INICIO", "FECHA_FIN", "HORA_FIN", "DURACION_HORAS"]
    if any(column not in headers for column in required):
        return False

    id_index = headers.index(ID_COLUMN)
    fecha_fin_index = headers.index("FECHA_FIN")
    target_row_number: int | None = None
    target_row: list[str] | None = None

    for row_number, row in enumerate(values[1:], start=2):
        row_identifier = row[id_index] if id_index < len(row) else ""
        fecha_fin = row[fecha_fin_index] if fecha_fin_index < len(row) else ""
        if clean_cell(row_identifier).strip() == clean_cell(identifier).strip() and not clean_cell(fecha_fin).strip():
            target_row_number = row_number
            target_row = row
            break

    if target_row_number is None or target_row is None:
        return False

    end_dt = datetime.now()
    start_dt = parse_start_datetime(
        target_row[headers.index("FECHA_INICIO")] if headers.index("FECHA_INICIO") < len(target_row) else "",
        target_row[headers.index("HORA_INICIO")] if headers.index("HORA_INICIO") < len(target_row) else "",
    )
    updates = [
        Cell(target_row_number, headers.index("FECHA_FIN") + 1, end_dt.strftime("%Y-%m-%d")),
        Cell(target_row_number, headers.index("HORA_FIN") + 1, end_dt.strftime("%H:%M:%S")),
        Cell(target_row_number, headers.index("DURACION_HORAS") + 1, calculate_duration_hours(start_dt, end_dt)),
    ]
    worksheet.update_cells(updates, value_input_option="USER_ENTERED")
    return True


def register_status_change(
    *,
    identifier: str,
    apparatus: str,
    previous_status: str,
    new_status: str,
    previous_identifier: str | None = None,
    change_comment: str = "",
) -> None:
    """Registra el cambio de STATUS y abre un nuevo tiempo activo."""

    ensure_tiempos_headers()
    close_previous_active_time(previous_identifier or identifier)
    st.cache_data.clear()

    tiempos_df = read_sheet_df(SHEET_TIEMPOS)
    now = datetime.now()
    tiempo_maximo = find_tiempo_maximo_horas(apparatus, new_status)
    default_comment = (
        f"Cambio de status desde app: {previous_status or 'Sin status'} → {new_status}"
    )
    row = {
        "ID_LOG": get_next_log_id(tiempos_df),
        ID_COLUMN: identifier,
        "APARATO": apparatus,
        "FASE_ORDEN": "",
        "STATUS": new_status,
        "STATUS_SIGUIENTE": "",
        "RESPONSABLE": "",
        "USUARIO": ACTIVE_USER_LABEL,
        "FECHA_INICIO": now.strftime("%Y-%m-%d"),
        "HORA_INICIO": now.strftime("%H:%M:%S"),
        "FECHA_FIN": "",
        "HORA_FIN": "",
        "DURACION_HORAS": "",
        "TIEMPO_MAXIMO_HORAS": tiempo_maximo,
        "ESTADO_ALERTA": "En tiempo",
        "COMENTARIOS_CAMBIO": change_comment.strip() or default_comment,
        "FECHA_REGISTRO_LOG": now.strftime("%Y-%m-%d %H:%M:%S"),
    }
    tiempos_worksheet = get_worksheet(SHEET_TIEMPOS)
    current_headers = tiempos_worksheet.row_values(1) or TIEMPOS_HEADERS
    tiempos_worksheet.append_row(
        [prepare_sheet_value(row.get(column, "")) for column in current_headers],
        value_input_option="USER_ENTERED",
    )


# ==============================
# 🖥️ COMPONENTES STREAMLIT
# ==============================
def apply_estatus_filters(df: pd.DataFrame) -> pd.DataFrame:
    """Renderiza filtros y devuelve la tabla filtrada."""

    filtered_df = df.copy()
    col_aparato, col_status, col_vendedor, col_pago, col_search = st.columns([1, 1, 1, 1, 1.4])

    def options_for(column: str) -> list[str]:
        if column not in df.columns:
            return ["Todos"]
        values = sorted({clean_cell(value).strip() for value in df[column] if clean_cell(value).strip()})
        return ["Todos", *values]

    with col_aparato:
        aparato_filter = st.selectbox("APARATO", options_for("APARATO"))
    with col_status:
        status_filter = st.selectbox("STATUS", options_for(STATUS_COLUMN))
    with col_vendedor:
        vendedor_filter = st.selectbox("VENDEDOR", options_for("VENDEDOR"))
    with col_pago:
        pago_filter = st.selectbox("PAGO", options_for("PAGO"))
    with col_search:
        search_text = st.text_input("Buscar", placeholder="Columna 1, doctor o paciente")

    for column, selected in {
        "APARATO": aparato_filter,
        STATUS_COLUMN: status_filter,
        "VENDEDOR": vendedor_filter,
        "PAGO": pago_filter,
    }.items():
        if selected != "Todos" and column in filtered_df.columns:
            filtered_df = filtered_df[filtered_df[column].astype(str) == selected]

    if search_text:
        search_norm = normalize_text(search_text)
        search_columns = [
            column
            for column in [ID_COLUMN, "NOMBRE DOCTOR", "NOMBRE PACIENTE"]
            if column in filtered_df.columns
        ]
        if search_columns:
            mask = filtered_df[search_columns].apply(
                lambda row: search_norm in normalize_text(" ".join(row.astype(str))), axis=1
            )
            filtered_df = filtered_df[mask]

    return filtered_df


def render_edit_field(column: str, value: Any, key: str) -> Any:
    """Renderiza un input seguro según la columna real de ESTATUS APARATOS."""

    text_value = clean_cell(value)

    if column == ID_COLUMN:
        st.text_input(column, value=text_value, key=key, disabled=True)
        return text_value

    if column in SELECTBOX_OPTIONS_BY_COLUMN:
        options = build_selectbox_options(SELECTBOX_OPTIONS_BY_COLUMN[column], text_value)
        index = options.index(text_value) if text_value in options else 0
        return st.selectbox(column, options, index=index, key=key)

    if column in DATE_COLUMNS:
        parsed_date = parse_simple_date(text_value)
        if parsed_date is not None:
            selected_date = st.date_input(column, value=parsed_date, key=key)
            return text_value if selected_date == parsed_date else selected_date.isoformat()
        return st.text_input(column, value=text_value, key=key)

    if column in TEXT_AREA_COLUMNS:
        return st.text_area(column, value=text_value, key=key, height=100)

    if column == "DÍAS DE ENTREGA" and is_numeric_value(text_value):
        numeric_value = float(text_value.replace(",", "."))
        if numeric_value.is_integer():
            return st.number_input(column, value=int(numeric_value), step=1, key=key)
        return st.number_input(column, value=numeric_value, step=0.5, key=key)

    return st.text_input(column, value=text_value, key=key)


def render_optional_date_input(label: str, key: str) -> str:
    """Permite guardar una fecha opcional sin establecer defaults no deseados."""

    enabled = st.checkbox(f"Agregar {label}", key=f"{key}_enabled")
    if not enabled:
        return ""
    return st.date_input(label, value=date.today(), key=key).isoformat()


def render_nuevo_pedido_tab() -> None:
    st.subheader("➕ Nuevo pedido")
    st.caption("Agrega una nueva fila en ESTATUS APARATOS y crea el registro inicial en TIEMPOS_APARATOS.")

    with st.form("form_nuevo_pedido"):
        col_left, col_right = st.columns(2)

        with col_left:
            pedido_id = st.text_input("Columna 1 *")
            aparato = st.selectbox("APARATO", APARATO_OPTIONS)
            status = st.selectbox(
                "STATUS",
                STATUS_OPTIONS,
                index=STATUS_OPTIONS.index("REVISION DE ARCHIVOS"),
            )
            nombre_doctor = st.text_input("NOMBRE DOCTOR")
            nombre_paciente = st.text_input("NOMBRE PACIENTE")
            detalle_comentarios = st.text_area("DETALLE COMENTARIOS", height=100)
            vendedor = st.selectbox("VENDEDOR", ["", *VENDEDOR_OPTIONS])
            servicio = st.selectbox("SERVICIO", ["", *SERVICIO_OPTIONS])
            archivos_recibidos = st.selectbox(
                "ARCHIVOS RECIBIDOS",
                ["", *ARCHIVOS_RECIBIDOS_OPTIONS],
            )
            fecha_recepcion = st.date_input("FECHA DE RECEPCIÓN", value=date.today())

        with col_right:
            fecha_hora_envio_stefano = st.text_input("FECHA/HORA ENVÍO STEFANO")
            fecha_hora_entrega_stefano = st.text_input("FECHA/HORA ENTREGA STEFANO")
            pago = st.selectbox("PAGO", ["", *PAGO_OPTIONS])
            fecha_pago_planeacion = render_optional_date_input(
                "FECHA PAGO PLANEACION",
                "nuevo_fecha_pago_planeacion",
            )
            fecha_pago_confeccion = render_optional_date_input(
                "FECHA PAGO CONFECCION",
                "nuevo_fecha_pago_confeccion",
            )
            dias_entrega = st.number_input("DÍAS DE ENTREGA", min_value=0, value=0, step=1)
            fecha_para_entrega = render_optional_date_input(
                "FECHA PARA ENTREGA",
                "nuevo_fecha_para_entrega",
            )
            fecha_impresion = render_optional_date_input(
                "FECHA IMPRESIÓN",
                "nuevo_fecha_impresion",
            )
            fecha_envio = render_optional_date_input(
                "FECHA ENVÍO",
                "nuevo_fecha_envio",
            )
            detalles_finales = st.text_area("DETALLES & COMENTARIOS FINALES", height=100)

        submitted = st.form_submit_button("💾 Guardar nuevo pedido")

    if not submitted:
        return

    cleaned_identifier = clean_cell(pedido_id).strip()
    if not cleaned_identifier:
        st.error("Columna 1 es obligatoria.")
        return

    if columna_1_exists(cleaned_identifier):
        st.error(f"Ya existe un pedido con Columna 1: {cleaned_identifier}")
        return

    row_dict = {
        ID_COLUMN: cleaned_identifier,
        "APARATO": aparato,
        STATUS_COLUMN: status,
        "NOMBRE DOCTOR": nombre_doctor,
        "NOMBRE PACIENTE": nombre_paciente,
        "DETALLE COMENTARIOS": detalle_comentarios,
        "VENDEDOR": vendedor,
        "SERVICIO": servicio,
        "ARCHIVOS RECIBIDOS": archivos_recibidos,
        "FECHA DE RECEPCIÓN": fecha_recepcion.isoformat(),
        "FECHA/HORA ENVÍO STEFANO": fecha_hora_envio_stefano,
        "FECHA/HORA ENTREGA STEFANO": fecha_hora_entrega_stefano,
        "PAGO": pago,
        "FECHA PAGO PLANEACION": fecha_pago_planeacion,
        "FECHA PAGO CONFECCION": fecha_pago_confeccion,
        "DÍAS DE ENTREGA": int(dias_entrega),
        "FECHA PARA ENTREGA": fecha_para_entrega,
        "FECHA IMPRESIÓN": fecha_impresion,
        "FECHA ENVÍO": fecha_envio,
        "DETALLES & COMENTARIOS FINALES": detalles_finales,
    }

    try:
        append_estatus_row(row_dict)
        register_status_change(
            identifier=cleaned_identifier,
            apparatus=aparato,
            previous_status="",
            new_status=status,
            change_comment="Registro inicial desde app",
        )
    except Exception as exc:
        st.error("No se pudo guardar el nuevo pedido.")
        st.exception(exc)
        return

    st.cache_data.clear()
    st.success("Nuevo pedido registrado correctamente.")
    st.rerun()


def render_estatus_tab() -> None:
    st.subheader("📋 Seguimiento de pedidos")
    estatus_df = read_sheet_df(SHEET_ESTATUS)

    if estatus_df.empty:
        st.info("La hoja ESTATUS APARATOS no tiene registros para mostrar.")
        return
    if ID_COLUMN not in estatus_df.columns:
        st.error(f"No se encontró la columna obligatoria '{ID_COLUMN}' en ESTATUS APARATOS.")
        return

    filtered_df = apply_estatus_filters(estatus_df)
    st.caption(f"Registros encontrados: {len(filtered_df)}")
    st.dataframe(filtered_df, use_container_width=True, hide_index=True)

    selectable_ids = [
        clean_cell(value).strip()
        for value in filtered_df[ID_COLUMN].tolist()
        if clean_cell(value).strip()
    ]
    if not selectable_ids:
        st.warning("No hay registros con Columna 1 para seleccionar en el resultado filtrado.")
        return

    selected_id = st.selectbox("Selecciona un registro por Columna 1", selectable_ids)
    selected_rows = estatus_df[estatus_df[ID_COLUMN].astype(str).str.strip() == selected_id]
    if selected_rows.empty:
        st.warning("No se pudo encontrar el registro seleccionado.")
        return

    row = selected_rows.iloc[0]
    st.markdown("### Editar registro seleccionado")
    st.caption("Se actualizarán únicamente las columnas modificadas.")

    with st.form(f"edit_estatus_{selected_id}"):
        edited_values: dict[str, str] = {}
        columns = list(estatus_df.columns)
        for start in range(0, len(columns), 2):
            left, right = st.columns(2)
            for container, column in zip((left, right), columns[start : start + 2]):
                with container:
                    edited_values[column] = render_edit_field(
                        column,
                        row.get(column, ""),
                        key=f"field_{selected_id}_{column}",
                    )

        change_comment = st.text_area(
            "Comentario del cambio",
            value="",
            key=f"change_comment_{selected_id}",
            help="Solo se guarda en TIEMPOS_APARATOS cuando cambia el STATUS.",
        )
        submitted = st.form_submit_button("💾 Guardar cambios")

    if submitted:
        changes = {
            column: value
            for column, value in edited_values.items()
            if column != ID_COLUMN and clean_cell(value) != clean_cell(row.get(column, ""))
        }
        if not changes:
            st.info("No se detectaron cambios para guardar.")
            return

        previous_status = clean_cell(row.get(STATUS_COLUMN, ""))
        new_status = clean_cell(changes.get(STATUS_COLUMN, previous_status))
        apparatus = clean_cell(changes.get("APARATO", row.get("APARATO", "")))

        if update_row_by_columna_1(selected_id, changes):
            if STATUS_COLUMN in changes and previous_status != new_status:
                register_status_change(
                    identifier=clean_cell(changes.get(ID_COLUMN, selected_id)).strip(),
                    apparatus=apparatus,
                    previous_status=previous_status,
                    new_status=new_status,
                    previous_identifier=selected_id,
                    change_comment=change_comment,
                )
            st.cache_data.clear()
            st.success("Registro actualizado correctamente.")
            st.rerun()
        else:
            st.error("No se pudo actualizar el registro. Revisa que Columna 1 sea única y exista en la hoja.")


def render_tiempos_tab() -> None:
    st.subheader("⏱️ Tiempos y Alertas")
    tiempos_df = read_sheet_df(SHEET_TIEMPOS)

    if tiempos_df.empty:
        st.info("TIEMPOS_APARATOS aún no tiene registros de cambios de status.")
        return

    for column in TIEMPOS_HEADERS:
        if column not in tiempos_df.columns:
            tiempos_df[column] = ""

    active_mask = tiempos_df["FECHA_FIN"].astype(str).str.strip() == ""
    tiempos_df["REGISTRO_ACTIVO"] = active_mask.map({True: "Sí", False: "No"})
    tiempos_df["ESTADO_ALERTA_VISUAL"] = tiempos_df.apply(
        lambda row: calculate_alert_state(
            row.get("FECHA_INICIO", ""),
            row.get("HORA_INICIO", ""),
            row.get("TIEMPO_MAXIMO_HORAS", ""),
        )
        if clean_cell(row.get("FECHA_FIN", "")).strip() == ""
        else clean_cell(row.get("ESTADO_ALERTA", "")) or "Cerrado",
        axis=1,
    )
    tiempos_df["HORAS_TRANSCURRIDAS"] = tiempos_df.apply(
        lambda row: calculate_duration_hours(
            parse_start_datetime(row.get("FECHA_INICIO", ""), row.get("HORA_INICIO", ""))
        )
        if clean_cell(row.get("FECHA_FIN", "")).strip() == ""
        else clean_cell(row.get("DURACION_HORAS", "")),
        axis=1,
    )

    ordered_df = pd.concat([tiempos_df[active_mask], tiempos_df[~active_mask]], ignore_index=True)
    st.dataframe(
        ordered_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "ESTADO_ALERTA_VISUAL": st.column_config.TextColumn("Estado alerta visual"),
            "HORAS_TRANSCURRIDAS": st.column_config.TextColumn("Horas transcurridas"),
        },
    )

    counts = ordered_df["ESTADO_ALERTA_VISUAL"].value_counts().to_dict()
    st.markdown("#### Resumen")
    summary_cols = st.columns(4)
    for container, label in zip(
        summary_cols,
        ["En tiempo", "Próximo a vencer", "Atrasado", "Sin tiempo configurado"],
    ):
        container.metric(label, counts.get(label, 0))


def render_procesos_tab() -> None:
    st.subheader("⚙️ Procesos por Aparato")
    procesos_df = read_sheet_df(SHEET_PROCESOS)
    if procesos_df.empty:
        st.info("La hoja PROCESOS POR APARATO no tiene datos para mostrar.")
        return
    st.caption("Consulta de procesos y tiempos configurados. Esta hoja no se edita en esta versión.")
    st.dataframe(procesos_df, use_container_width=True, hide_index=True)


# ==============================
# 🚀 APP STREAMLIT
# ==============================
st.set_page_config(page_title="Control de Aparatos – ARTTDLAB", layout="wide")
st.title("🦷 Control de Aparatos – ARTTDLAB")
st.caption("Primera versión para edición manual de estatus y registro automático de tiempos.")

if st.button("🔄 Recargar datos", type="secondary"):
    st.cache_data.clear()
    st.cache_resource.clear()
    st.rerun()

try:
    ensure_tiempos_headers()
    tab_nuevo, tab_seguimiento, tab_tiempos, tab_procesos = st.tabs(
        [
            "➕ Nuevo pedido",
            "📋 Seguimiento de pedidos",
            "⏱️ Tiempos y Alertas",
            "⚙️ Procesos por Aparato",
        ]
    )

    with tab_nuevo:
        render_nuevo_pedido_tab()
    with tab_seguimiento:
        render_estatus_tab()
    with tab_tiempos:
        render_tiempos_tab()
    with tab_procesos:
        render_procesos_tab()
except Exception as exc:
    st.error("Ocurrió un problema al cargar la app.")
    st.exception(exc)
