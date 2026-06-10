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
from gspread.utils import rowcol_to_a1

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
    "LESLY",
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
    "DISEÑO & CONFECCIÓN",
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

APARATO_DISPLAY = {
    "MSE": "🟤 MSE",
    "TIGER": "🟡 TIGER",
    "REVERSE": "🟢 REVERSE",
    "HYRAX": "🟠 HYRAX",
    "TRAMPA LINGUAL": "⚪ TRAMPA LINGUAL",
    "LEONE": "🔵 LEONE",
    "DISTALIZADOR": "⚫ DISTALIZADOR",
}

STATUS_DISPLAY = {
    "REVISION DE ARCHIVOS": "🔵 REVISION DE ARCHIVOS",
    "EN PLANEACIÓN": "🟡 EN PLANEACIÓN",
    "REVISIÓN DEL DISEÑO POR DR": "⚪ REVISIÓN DEL DISEÑO POR DR",
    "SOLICITUD DE CAMBIOS": "🟤 SOLICITUD DE CAMBIOS",
    "ELABORACIÓN PLATINA BANDAS": "🟣 ELABORACIÓN PLATINA BANDAS",
    "ESPERANDO STL PSM": "🟨 ESPERANDO STL PSM",
    "PDTE ENVIO PSM + GUIA": "🔴 PDTE ENVIO PSM + GUIA",
    "LISTO P/SINTERIZADO": "⚪ LISTO P/SINTERIZADO",
    "EN SINTERIZADO": "⚫ EN SINTERIZADO",
    "LISTO P/CONFECCIÓN": "🌸 LISTO P/CONFECCIÓN",
    "EN CONFECCIÓN": "🔴 EN CONFECCIÓN",
    "LISTO P/ENVÍO": "🟢 LISTO P/ENVÍO",
    "ENVIADO": "✅ ENVIADO",
    "FALTA PAGO COMPLETO": "🟣 FALTA PAGO COMPLETO",
    "CONFECCION EN PAUSA": "🚫 CONFECCION EN PAUSA",
    "CANCELO": "🔵 CANCELO",
}

VENDEDOR_DISPLAY = {
    "JIMENA": "👩 JIMENA",
    "JUAN": "👨 JUAN",
    "MICHELLE": "👩 MICHELLE",
    "IGNACIO": "👨 IGNACIO",
    "LESLY": "👩 LESLY",
    "KAREN": "👩 KAREN",
    "ALEJANDRA": "👩 ALEJANDRA",
    "CARO": "👩 CARO",
    "HECTOR": "👨 HECTOR",
    "DANIELA": "👩 DANIELA",
}

SERVICIO_DISPLAY = {
    "PLANEACIÓN COMPLETA": "📋 PLANEACIÓN COMPLETA",
    "CONFECCIÓN": "🦷 CONFECCIÓN",
    "PLANEACIÓN & CONFECCIÓN": "🔵 PLANEACIÓN & CONFECCIÓN",
    "DISEÑO & CONFECCIÓN": "🟣 DISEÑO & CONFECCIÓN",
    "DISEÑO & CONFECCION": "🟣 DISEÑO & CONFECCION",
    "PLANEACION": "📄 PLANEACION",
}

ARCHIVOS_RECIBIDOS_DISPLAY = {
    "STL": "📁 STL",
    "TOMOGRAFÍA": "🩻 TOMOGRAFÍA",
    "STL+TOMO": "📁🩻 STL+TOMO",
}

PAGO_DISPLAY = {
    "ANTICIPO": "🔴 ANTICIPO",
    "TOTAL": "🟢 TOTAL",
    "SIN PAGO": "⚪ SIN PAGO",
    "CANCELO": "🔵 CANCELO",
}

DISPLAY_OPTIONS_BY_COLUMN = {
    "APARATO": APARATO_DISPLAY,
    STATUS_COLUMN: STATUS_DISPLAY,
    "VENDEDOR": VENDEDOR_DISPLAY,
    "SERVICIO": SERVICIO_DISPLAY,
    "ARCHIVOS RECIBIDOS": ARCHIVOS_RECIBIDOS_DISPLAY,
    "PAGO": PAGO_DISPLAY,
}

SHEET_STYLE_COLORS = {
    "APARATO": {
        "MSE": ("#7B3F0A", "#FFFFFF"),
        "TIGER": ("#C99A2E", "#FFFFFF"),
        "REVERSE": ("#A8B94B", "#FFFFFF"),
        "HYRAX": ("#FF6A2A", "#FFFFFF"),
        "TRAMPA LINGUAL": ("#E6E6E6", "#333333"),
        "LEONE": ("#2D6373", "#FFFFFF"),
        "DISTALIZADOR": ("#444444", "#FFFFFF"),
    },
    STATUS_COLUMN: {
        "REVISION DE ARCHIVOS": ("#C9E6EC", "#2A5964"),
        "EN PLANEACIÓN": ("#FFE86A", "#000000"),
        "REVISIÓN DEL DISEÑO POR DR": ("#E6E6E6", "#333333"),
        "SOLICITUD DE CAMBIOS": ("#6B4B17", "#FFFFFF"),
        "ELABORACIÓN PLATINA BANDAS": ("#DCC4F4", "#6A3D8E"),
        "ESPERANDO STL PSM": ("#FAD98A", "#6B4B17"),
        "PDTE ENVIO PSM + GUIA": ("#FFA7A0", "#B00000"),
        "LISTO P/SINTERIZADO": ("#E6E6E6", "#333333"),
        "EN SINTERIZADO": ("#E6E6E6", "#333333"),
        "LISTO P/CONFECCIÓN": ("#F7A7C3", "#7A1740"),
        "EN CONFECCIÓN": ("#FF6B6B", "#000000"),
        "LISTO P/ENVÍO": ("#8FD84A", "#000000"),
        "ENVIADO": ("#7BE84D", "#000000"),
        "FALTA PAGO COMPLETO": ("#5D3B93", "#FFFFFF"),
        "CONFECCION EN PAUSA": ("#B80F0F", "#FFFFFF"),
        "CANCELO": ("#1F6DD1", "#000000"),
    },
    "SERVICIO": {
        "PLANEACIÓN COMPLETA": ("#FAD98A", "#6B4B17"),
        "CONFECCIÓN": ("#F6C09B", "#8A3F16"),
        "PLANEACIÓN & CONFECCIÓN": ("#9DD7FF", "#005EA8"),
        "DISEÑO & CONFECCIÓN": ("#DCC4F4", "#6A3D8E"),
        "DISEÑO & CONFECCION": ("#DCC4F4", "#6A3D8E"),
        "PLANEACION": ("#E6E6E6", "#333333"),
    },
    "ARCHIVOS RECIBIDOS": {
        "STL": ("#B80F0F", "#FFFFFF"),
        "TOMOGRAFÍA": ("#B80F0F", "#FFFFFF"),
        "STL+TOMO": ("#7BE84D", "#000000"),
    },
    "PAGO": {
        "ANTICIPO": ("#B80F0F", "#FFFFFF"),
        "TOTAL": ("#7BE84D", "#000000"),
        "SIN PAGO": ("#E6E6E6", "#333333"),
        "CANCELO": ("#1F6DD1", "#FFFFFF"),
    },
}

STYLE_COLUMNS = set(SHEET_STYLE_COLORS)

FIELD_LABEL_DISPLAY = {
    ID_COLUMN: "🆔 Columna 1",
    "APARATO": "🦷 APARATO",
    STATUS_COLUMN: "🚦 STATUS",
    "NOMBRE DOCTOR": "👩‍⚕️ NOMBRE DOCTOR",
    "NOMBRE PACIENTE": "🙂 NOMBRE PACIENTE",
    "DETALLE COMENTARIOS": "📝 DETALLE COMENTARIOS",
    "DETALLES & COMENTARIOS FINALES": "🗒️ DETALLES & COMENTARIOS FINALES",
    "VENDEDOR": "🤝 VENDEDOR",
    "SERVICIO": "🛠️ SERVICIO",
    "ARCHIVOS RECIBIDOS": "📁 ARCHIVOS RECIBIDOS",
    "PAGO": "💳 PAGO",
    "DÍAS DE ENTREGA": "📆 DÍAS DE ENTREGA",
    "FECHA DE RECEPCIÓN": "📥 FECHA DE RECEPCIÓN",
    "FECHA PAGO PLANEACION": "💳 FECHA PAGO PLANEACION",
    "FECHA PAGO CONFECCION": "💳 FECHA PAGO CONFECCION",
    "FECHA PARA ENTREGA": "📦 FECHA PARA ENTREGA",
    "FECHA IMPRESIÓN": "🖨️ FECHA IMPRESIÓN",
    "FECHA ENVÍO": "🚚 FECHA ENVÍO",
    "FECHA/HORA ENVÍO STEFANO": "🚚 FECHA/HORA ENVÍO STEFANO",
    "FECHA/HORA ENTREGA STEFANO": "📬 FECHA/HORA ENTREGA STEFANO",
    "ID_LOG": "🔢 ID_LOG",
    "FASE_ORDEN": "🧭 FASE_ORDEN",
    "STATUS_SIGUIENTE": "➡️ STATUS_SIGUIENTE",
    "RESPONSABLE": "🙋 RESPONSABLE",
    "USUARIO": "👤 USUARIO",
    "FECHA_INICIO": "▶️ FECHA_INICIO",
    "HORA_INICIO": "🕒 HORA_INICIO",
    "FECHA_FIN": "🏁 FECHA_FIN",
    "HORA_FIN": "🕔 HORA_FIN",
    "DURACION_HORAS": "⏳ DURACION_HORAS",
    "TIEMPO_MAXIMO_HORAS": "⏱️ TIEMPO_MAXIMO_HORAS",
    "ESTADO_ALERTA": "🚨 ESTADO_ALERTA",
    "COMENTARIOS_CAMBIO": "💬 COMENTARIOS_CAMBIO",
    "FECHA_REGISTRO_LOG": "🗓️ FECHA_REGISTRO_LOG",
    "REGISTRO_ACTIVO": "🟢 REGISTRO_ACTIVO",
    "ESTADO_ALERTA_VISUAL": "🚦 ESTADO_ALERTA_VISUAL",
    "HORAS_TRANSCURRIDAS": "⌛ HORAS_TRANSCURRIDAS",
}

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


def parse_spanish_datetime(value: Any) -> datetime | None:
    """Convierte textos como '23 DICIEMBRE 9:00 AM' a datetime cuando sea posible."""

    text = " ".join(clean_cell(value).strip().split())
    if not text:
        return None

    normalized = normalize_text(text).replace("/", "-")
    parts = normalized.split()
    if len(parts) >= 2 and parts[0].isdigit() and parts[1] in SPANISH_MONTHS:
        day = int(parts[0])
        month = SPANISH_MONTHS[parts[1]]
        year = datetime.now().year
        time_parts = parts[2:]
        if time_parts and time_parts[0].isdigit() and len(time_parts[0]) == 4:
            year = int(time_parts[0])
            time_parts = time_parts[1:]

        parsed_time = datetime.min.time()
        if time_parts:
            time_text = " ".join(time_parts)
            parsed_time_candidate = pd.to_datetime(
                time_text, errors="coerce"
            )
            if not pd.isna(parsed_time_candidate):
                parsed_time = parsed_time_candidate.time().replace(
                    second=0, microsecond=0
                )

        try:
            return datetime.combine(date(year, month, day), parsed_time)
        except ValueError:
            return None

    parsed = pd.to_datetime(text, errors="coerce", dayfirst=True)
    if pd.isna(parsed):
        return None
    return parsed.to_pydatetime().replace(second=0, microsecond=0)


def format_sheet_date(value: date) -> str:
    """Formatea fechas como texto YYYY/MM/DD para que Sheets las muestre establemente."""

    return value.strftime("%Y/%m/%d")


def format_sheet_datetime(value: datetime) -> str:
    """Formatea fecha/hora como texto YYYY/MM/DD HH:MM para columnas combinadas."""

    return value.strftime("%Y/%m/%d %H:%M")

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


def clean_display_value(value: Any) -> str:
    """Quita el emoji de una opción visual antes de guardar en Google Sheets."""

    text = clean_cell(value).strip()
    display_values = {
        display_value
        for display_options in DISPLAY_OPTIONS_BY_COLUMN.values()
        for display_value in display_options.values()
    }
    return text.split(" ", 1)[1] if text in display_values and " " in text else text


def display_selectbox_value(column: str, value: str) -> str:
    """Devuelve el texto visual con emoji para una columna selectbox."""

    cleaned_value = clean_display_value(clean_cell(value).strip())
    if not cleaned_value:
        return ""
    display_options = DISPLAY_OPTIONS_BY_COLUMN.get(canonical_column_name(column), {})
    return display_options.get(cleaned_value, cleaned_value)


def build_display_selectbox_options(
    column: str, fixed_options: list[str], current_value: str
) -> list[str]:
    """Construye opciones visuales con emoji manteniendo valores limpios internos."""

    canonical_column = canonical_column_name(column)
    cleaned_current_value = clean_display_value(clean_cell(current_value).strip())
    clean_options = build_selectbox_options(fixed_options, cleaned_current_value)
    return [display_selectbox_value(canonical_column, option) for option in clean_options]


def display_field_label(column: str, *, required: bool = False) -> str:
    """Devuelve un encabezado de campo más visual sin cambiar el nombre real de columna."""

    canonical_column = canonical_column_name(column)
    label = FIELD_LABEL_DISPLAY.get(canonical_column, clean_cell(column).strip())
    return f"{label} *" if required else label


def build_dataframe_column_config(df: pd.DataFrame) -> dict[str, Any]:
    """Configura encabezados visuales con emoji para tablas sin alterar el DataFrame."""

    return {
        column: st.column_config.Column(display_field_label(column))
        for column in df.columns
        if column in FIELD_LABEL_DISPLAY
    }


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


def canonical_header_key(value: Any) -> str:
    """Normaliza encabezados de Sheets ignorando saltos de línea y espacios extra."""

    return " ".join(normalize_text(value).split())


def build_header_positions(headers: list[str]) -> dict[str, int]:
    """Mapea encabezados a posiciones 1-based con comparación flexible."""

    positions: dict[str, int] = {}
    for index, header in enumerate(headers, start=1):
        cleaned_header = clean_cell(header).strip()
        if cleaned_header and cleaned_header not in positions:
            positions[cleaned_header] = index

        canonical_header = canonical_header_key(header)
        if canonical_header and canonical_header not in positions:
            positions[canonical_header] = index

    return positions


def get_header_position(headers: list[str], column: str) -> int | None:
    """Devuelve la posición 1-based de una columna aceptando encabezados con saltos."""

    positions = build_header_positions(headers)
    return positions.get(column) or positions.get(canonical_header_key(column))


def known_estatus_columns() -> set[str]:
    """Columnas conocidas de ESTATUS APARATOS para tratar encabezados con espacios."""

    return {
        *FIELD_LABEL_DISPLAY,
        *SELECTBOX_OPTIONS_BY_COLUMN,
        *DATE_COLUMNS,
        *DATETIME_TEXT_COLUMNS,
        *TEXT_AREA_COLUMNS,
    }


def canonical_column_name(column: str) -> str:
    """Devuelve el nombre esperado de una columna aunque Sheets traiga espacios extra."""

    column_key = canonical_header_key(column)
    for known_column in known_estatus_columns():
        if canonical_header_key(known_column) == column_key:
            return known_column
    return clean_cell(column).strip()


def values_equivalent_for_column(column: str, old_value: Any, new_value: Any) -> bool:
    """Compara valores editados sin marcar cambios falsos por formato visual de fecha."""

    canonical_column = canonical_column_name(column)
    old_text = clean_display_value(clean_cell(old_value)).strip()
    new_text = clean_display_value(clean_cell(new_value)).strip()

    if canonical_column in DATE_COLUMNS:
        old_date = parse_simple_date(old_text)
        new_date = parse_simple_date(new_text)
        if old_date is not None and new_date is not None:
            return old_date == new_date

    if canonical_column in DATETIME_TEXT_COLUMNS:
        old_datetime = parse_spanish_datetime(old_text)
        new_datetime = parse_spanish_datetime(new_text)
        if old_datetime is not None and new_datetime is not None:
            return old_datetime == new_datetime

    return old_text == new_text


def get_row_value_by_column(row: pd.Series, column: str, default: Any = "") -> Any:
    """Lee una celda de DataFrame aceptando encabezados con espacios extra."""

    canonical_column = canonical_column_name(column)
    for row_column in row.index:
        if canonical_column_name(row_column) == canonical_column:
            return row.get(row_column, default)
    return default


def build_canonical_changes(changes: dict[str, Any]) -> dict[str, Any]:
    """Mapea cambios por nombre canónico para consultar STATUS/APARATO sin espacios."""

    return {canonical_column_name(column): value for column, value in changes.items()}


def hex_to_sheets_color(hex_color: str) -> dict[str, float]:
    """Convierte color hexadecimal a formato RGB decimal de Google Sheets."""

    cleaned = hex_color.lstrip("#")
    return {
        "red": int(cleaned[0:2], 16) / 255,
        "green": int(cleaned[2:4], 16) / 255,
        "blue": int(cleaned[4:6], 16) / 255,
    }


def build_cell_style(background_hex: str, text_hex: str) -> dict[str, Any]:
    """Construye formato visual para simular chips de colores en Sheets."""

    return {
        "backgroundColor": hex_to_sheets_color(background_hex),
        "textFormat": {
            "foregroundColor": hex_to_sheets_color(text_hex),
            "bold": True,
        },
        "horizontalAlignment": "CENTER",
        "verticalAlignment": "MIDDLE",
    }


def apply_estatus_row_styles(
    worksheet: gspread.Worksheet,
    headers: list[str],
    row_number: int,
    row_dict: dict[str, Any],
) -> None:
    """Aplica colores en ESTATUS APARATOS según el valor guardado."""

    formats = []
    for column in STYLE_COLUMNS:
        value = clean_display_value(clean_cell(row_dict.get(column, "")).strip())
        color_pair = SHEET_STYLE_COLORS[column].get(value)
        column_position = get_header_position(headers, column)
        if not value or color_pair is None or column_position is None:
            continue

        background_hex, text_hex = color_pair
        formats.append(
            {
                "range": rowcol_to_a1(row_number, column_position),
                "format": build_cell_style(background_hex, text_hex),
            }
        )

    if formats:
        worksheet.batch_format(formats)


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


def update_row_by_columna_1(identifier: str, changes: dict[str, Any]) -> dict[str, Any]:
    """Actualiza solo las celdas modificadas de ESTATUS APARATOS por Columna 1."""

    result = {"success": False, "updated_columns": [], "skipped_columns": [], "error": ""}
    if not changes:
        return result

    worksheet = get_worksheet(SHEET_ESTATUS)
    values = worksheet.get_all_values()
    if not values:
        result["skipped_columns"] = list(changes)
        result["error"] = "La hoja ESTATUS APARATOS está vacía."
        return result

    if len(values) < 2:
        result["skipped_columns"] = list(changes)
        result["error"] = "No encontré la fila de encabezados en ESTATUS APARATOS."
        return result

    headers = values[1]
    id_position = get_header_position(headers, ID_COLUMN)
    if id_position is None:
        result["skipped_columns"] = list(changes)
        result["error"] = f"No encontré la columna obligatoria {ID_COLUMN}."
        return result

    id_index = id_position - 1
    target_row_number: int | None = None
    for row_number, row in enumerate(values[2:], start=3):
        row_identifier = row[id_index] if id_index < len(row) else ""
        if clean_cell(row_identifier).strip() == clean_cell(identifier).strip():
            target_row_number = row_number
            break

    if target_row_number is None:
        result["skipped_columns"] = list(changes)
        result["error"] = f"No encontré el registro con {ID_COLUMN} {identifier}."
        return result

    updates = []
    canonical_changes: dict[str, Any] = {}
    for column, value in changes.items():
        column_position = get_header_position(headers, column)
        if column_position is None:
            result["skipped_columns"].append(column)
            continue
        updates.append(
            Cell(target_row_number, column_position, prepare_sheet_value(value))
        )
        result["updated_columns"].append(column)
        canonical_changes[canonical_column_name(column)] = value

    if not updates:
        result["error"] = "No encontré encabezados válidos para las columnas modificadas."
        return result

    try:
        worksheet.update_cells(updates, value_input_option="USER_ENTERED")
    except Exception as exc:
        result["skipped_columns"] = [*result["updated_columns"], *result["skipped_columns"]]
        result["updated_columns"] = []
        result["error"] = f"Google Sheets rechazó la actualización: {exc}"
        return result

    apply_estatus_row_styles(worksheet, headers, target_row_number, canonical_changes)
    result["success"] = True
    return result



NEW_ORDER_ESTATUS_FIELDS = [
    ID_COLUMN,
    "APARATO",
    STATUS_COLUMN,
    "NOMBRE DOCTOR",
    "NOMBRE PACIENTE",
    "DETALLE COMENTARIOS",
    "VENDEDOR",
    "SERVICIO",
    "ARCHIVOS RECIBIDOS",
    "PAGO",
    "DÍAS DE ENTREGA",
    "FECHA DE RECEPCIÓN",
    "FECHA PARA ENTREGA",
]


def find_first_available_estatus_row(values: list[list[str]], headers: list[str]) -> int:
    """Encuentra la primera fila donde Columna 1 esté vacía."""

    id_position = get_header_position(headers, ID_COLUMN)
    if id_position is None:
        raise ValueError(f"No se encontró la columna {ID_COLUMN} en ESTATUS APARATOS.")

    id_index = id_position - 1
    for row_number, row in enumerate(values[2:], start=3):
        current_id = row[id_index] if id_index < len(row) else ""
        if not clean_cell(current_id).strip():
            return row_number

    return len(values) + 1


def append_estatus_row(row_dict: dict[str, Any]) -> int:
    """Escribe un nuevo pedido en ESTATUS APARATOS y devuelve el renglón usado."""

    worksheet = get_worksheet(SHEET_ESTATUS)
    values = worksheet.get_all_values()
    if len(values) < 2:
        raise ValueError("No se encontraron encabezados en la fila 2 de ESTATUS APARATOS.")

    headers = values[1]
    target_row = find_first_available_estatus_row(values, headers)

    updates = []
    for column in NEW_ORDER_ESTATUS_FIELDS:
        if column not in row_dict:
            continue

        column_position = get_header_position(headers, column)
        if column_position is None:
            continue

        updates.append(
            Cell(
                row=target_row,
                col=column_position,
                value=prepare_sheet_value(row_dict[column]),
            )
        )

    if not updates:
        raise ValueError("No hay campos válidos para guardar en ESTATUS APARATOS.")

    worksheet.update_cells(updates, value_input_option="USER_ENTERED")
    apply_estatus_row_styles(worksheet, headers, target_row, row_dict)
    return target_row


def columna_1_exists(identifier: str) -> bool:
    """Revisa si ya existe un identificador en Columna 1 de ESTATUS APARATOS."""

    cleaned_identifier = clean_cell(identifier).strip()
    if not cleaned_identifier:
        return False

    values = get_worksheet(SHEET_ESTATUS).get_all_values()
    if len(values) < 2:
        return False

    id_position = get_header_position(values[1], ID_COLUMN)
    if id_position is None:
        return False

    id_index = id_position - 1
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
def apply_custom_css() -> None:
    """Aplica mejoras visuales globales para la app Streamlit."""

    st.markdown(
        """
        <style>
            .stApp {
                background: linear-gradient(180deg, #f8fbff 0%, #eef5ff 45%, #ffffff 100%);
            }

            h1, h2, h3 {
                color: #123a63;
                font-weight: 800 !important;
                letter-spacing: -0.02em;
            }

            h1 {
                padding-bottom: 0.35rem;
                border-bottom: 3px solid #8ec5ff;
            }

            h2, h3 {
                margin-top: 0.75rem;
            }

            div[data-testid="stCaptionContainer"],
            .stMarkdown p {
                color: #4f6478;
                font-weight: 500;
            }

            label, div[data-testid="stWidgetLabel"] p {
                color: #54708c !important;
                font-weight: 700 !important;
            }

            div[data-testid="stForm"] {
                background: rgba(255, 255, 255, 0.92);
                border: 1px solid #d8e7f7;
                border-radius: 22px;
                box-shadow: 0 16px 40px rgba(18, 58, 99, 0.10);
                padding: 1.35rem 1.5rem 1.55rem;
            }

            div[data-baseweb="select"] > div,
            div[data-baseweb="input"] > div,
            textarea,
            div[data-testid="stDateInput"] input,
            div[data-testid="stNumberInput"] input {
                border-radius: 14px !important;
                border-color: #b7cce1 !important;
                background-color: #ffffff !important;
            }

            div[data-baseweb="select"] > div:hover,
            div[data-baseweb="input"] > div:hover,
            textarea:hover {
                border-color: #4d9de0 !important;
                box-shadow: 0 0 0 2px rgba(77, 157, 224, 0.12);
            }

            div[data-testid="stFormSubmitButton"] button {
                background: linear-gradient(135deg, #0f7cdb 0%, #35c2ff 50%, #4fd1a5 100%);
                border: 0;
                border-radius: 999px;
                color: #ffffff;
                font-weight: 800;
                padding: 0.65rem 1.25rem;
                box-shadow: 0 10px 22px rgba(15, 124, 219, 0.28);
                transition: transform 120ms ease, box-shadow 120ms ease;
            }

            div[data-testid="stFormSubmitButton"] button:hover {
                transform: translateY(-1px);
                box-shadow: 0 14px 28px rgba(15, 124, 219, 0.34);
            }

            div[data-testid="stTabs"] button p {
                color: #123a63;
                font-weight: 700;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def apply_estatus_filters(df: pd.DataFrame) -> pd.DataFrame:
    """Renderiza filtros y devuelve la tabla filtrada."""

    filtered_df = df.copy()
    col_aparato, col_status, col_vendedor, col_pago, col_search = st.columns(
        [1, 1, 1, 1, 1.4]
    )

    def options_for(column: str) -> list[str]:
        if column not in df.columns:
            return ["Todos"]
        values = sorted(
            {
                clean_display_value(clean_cell(value).strip())
                for value in df[column]
                if clean_cell(value).strip()
            }
        )
        return ["Todos", *[display_selectbox_value(column, value) for value in values]]

    with col_aparato:
        aparato_filter = clean_display_value(
            st.selectbox(display_field_label("APARATO"), options_for("APARATO"))
        )
    with col_status:
        status_filter = clean_display_value(
            st.selectbox(display_field_label(STATUS_COLUMN), options_for(STATUS_COLUMN))
        )
    with col_vendedor:
        vendedor_filter = clean_display_value(
            st.selectbox(display_field_label("VENDEDOR"), options_for("VENDEDOR"))
        )
    with col_pago:
        pago_filter = clean_display_value(
            st.selectbox(display_field_label("PAGO"), options_for("PAGO"))
        )
    with col_search:
        search_text = st.text_input("🔎 Buscar", placeholder="Columna 1, doctor o paciente")

    for column, selected in {
        "APARATO": aparato_filter,
        STATUS_COLUMN: status_filter,
        "VENDEDOR": vendedor_filter,
        "PAGO": pago_filter,
    }.items():
        if selected != "Todos" and column in filtered_df.columns:
            filtered_df = filtered_df[
                filtered_df[column].apply(
                    lambda value: clean_display_value(clean_cell(value).strip())
                )
                == selected
            ]

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
    canonical_column = canonical_column_name(column)

    if canonical_column == ID_COLUMN:
        st.text_input(
            display_field_label(column), value=text_value, key=key, disabled=True
        )
        return text_value

    if canonical_column in SELECTBOX_OPTIONS_BY_COLUMN:
        options = build_display_selectbox_options(
            canonical_column, SELECTBOX_OPTIONS_BY_COLUMN[canonical_column], text_value
        )
        current_display_value = display_selectbox_value(canonical_column, text_value)
        index = options.index(current_display_value) if current_display_value in options else 0
        selected_value = st.selectbox(
            display_field_label(column), options, index=index, key=key
        )
        return clean_display_value(selected_value)

    if canonical_column in DATE_COLUMNS:
        parsed_date = parse_simple_date(text_value)
        if parsed_date is None and not text_value.strip():
            selected_date = st.date_input(
                display_field_label(column), value=None, key=key
            )
            return format_sheet_date(selected_date) if selected_date else ""

        initial_date = parsed_date or date.today()
        selected_date = st.date_input(
            display_field_label(column), value=initial_date, key=key
        )
        if parsed_date is None and text_value.strip():
            st.caption(
                "Valor anterior no reconocido; se conserva si no eliges otra fecha: "
                f"{text_value}"
            )
            return (
                format_sheet_date(selected_date)
                if selected_date != initial_date
                else text_value
            )
        return format_sheet_date(selected_date)

    if canonical_column in DATETIME_TEXT_COLUMNS:
        parsed_datetime = parse_spanish_datetime(text_value)
        initial_datetime = parsed_datetime or datetime.combine(
            date.today(), datetime.now().time()
        ).replace(second=0, microsecond=0)
        date_col, time_col = st.columns([1, 1])
        with date_col:
            selected_date = st.date_input(
                display_field_label(column),
                value=(
                    initial_datetime.date()
                    if parsed_datetime or text_value.strip()
                    else None
                ),
                key=f"{key}_date",
            )
        with time_col:
            selected_time = st.time_input(
                "🕒 Hora", value=initial_datetime.time(), key=f"{key}_time"
            )
        if selected_date is None and not text_value.strip():
            return ""

        selected_datetime = datetime.combine(selected_date, selected_time).replace(
            second=0, microsecond=0
        )
        if parsed_datetime is None and text_value.strip():
            st.caption(
                "Valor anterior no reconocido; se conserva si no eliges otra fecha/hora: "
                f"{text_value}"
            )
            return (
                format_sheet_datetime(selected_datetime)
                if selected_datetime != initial_datetime
                else text_value
            )
        return format_sheet_datetime(selected_datetime)

    if canonical_column in TEXT_AREA_COLUMNS:
        return st.text_area(
            display_field_label(column), value=text_value, key=key, height=100
        )

    if canonical_column == "DÍAS DE ENTREGA" and is_numeric_value(text_value):
        numeric_value = float(text_value.replace(",", "."))
        if numeric_value.is_integer():
            return st.number_input(
                display_field_label(column), value=int(numeric_value), step=1, key=key
            )
        return st.number_input(
            display_field_label(column), value=numeric_value, step=0.5, key=key
        )

    return st.text_input(display_field_label(column), value=text_value, key=key)


def render_optional_date_input(label: str, key: str) -> str:
    """Permite guardar una fecha opcional sin establecer defaults no deseados."""

    enabled = st.checkbox(f"Agregar {display_field_label(label)}", key=f"{key}_enabled")
    if not enabled:
        return ""
    return st.date_input(
        display_field_label(label), value=date.today(), key=key
    ).isoformat()


def render_success_feedback(
    message_key: str, celebrate_key: str, clear_button_key: str
) -> None:
    """Muestra éxito persistente con toast, globos y botón para limpiar."""

    message = st.session_state.get(message_key)
    if not message:
        return

    if st.session_state.pop(celebrate_key, False):
        st.toast(message, icon="✅")
        st.balloons()

    st.success(message)
    if st.button("✅ Aceptar y limpiar mensaje", key=clear_button_key):
        st.session_state.pop(message_key, None)
        st.rerun()


def render_warning_feedback(message_key: str, clear_button_key: str) -> None:
    """Muestra una advertencia persistente hasta que el usuario la limpie."""

    message = st.session_state.get(message_key)
    if not message:
        return

    st.warning(message)
    if st.button("⚠️ Aceptar y limpiar advertencia", key=clear_button_key):
        st.session_state.pop(message_key, None)
        st.rerun()


def render_nuevo_pedido_tab() -> None:
    st.subheader("➕ Nuevo pedido")
    st.caption(
        "Captura los datos principales en ESTATUS APARATOS; "
        "TIEMPOS_APARATOS solo recibe el log complementario para tiempos y alertas."
    )
    render_success_feedback(
        "nuevo_pedido_success_message",
        "nuevo_pedido_show_celebration",
        "clear_nuevo_pedido_success",
    )

    form_version = st.session_state.get("nuevo_pedido_form_version", 0)
    form_key = f"form_nuevo_pedido_{form_version}"

    with st.form(form_key):
        col_left, col_right = st.columns(2)

        with col_left:
            pedido_id = st.text_input(
                display_field_label(ID_COLUMN, required=True),
                key=f"nuevo_pedido_id_{form_version}",
            )
            aparato = st.selectbox(
                display_field_label("APARATO"),
                build_display_selectbox_options("APARATO", APARATO_OPTIONS, ""),
                key=f"nuevo_pedido_aparato_{form_version}",
            )
            nombre_doctor = st.text_input(
                display_field_label("NOMBRE DOCTOR"),
                key=f"nuevo_pedido_doctor_{form_version}",
            )
            nombre_paciente = st.text_input(
                display_field_label("NOMBRE PACIENTE"),
                key=f"nuevo_pedido_paciente_{form_version}",
            )
            detalle_comentarios = st.text_area(
                display_field_label("DETALLE COMENTARIOS"),
                height=100,
                key=f"nuevo_pedido_comentarios_{form_version}",
            )

        with col_right:
            vendedor = st.selectbox(
                display_field_label("VENDEDOR"),
                build_display_selectbox_options("VENDEDOR", VENDEDOR_OPTIONS, ""),
                key=f"nuevo_pedido_vendedor_{form_version}",
            )
            servicio = st.selectbox(
                display_field_label("SERVICIO"),
                build_display_selectbox_options("SERVICIO", SERVICIO_OPTIONS, ""),
                key=f"nuevo_pedido_servicio_{form_version}",
            )
            archivos_recibidos = st.selectbox(
                display_field_label("ARCHIVOS RECIBIDOS"),
                build_display_selectbox_options(
                    "ARCHIVOS RECIBIDOS", ARCHIVOS_RECIBIDOS_OPTIONS, ""
                ),
                key=f"nuevo_pedido_archivos_{form_version}",
            )
            pago = st.selectbox(
                display_field_label("PAGO"),
                build_display_selectbox_options("PAGO", PAGO_OPTIONS, ""),
                key=f"nuevo_pedido_pago_{form_version}",
            )
            fecha_recepcion = st.date_input(
                display_field_label("FECHA DE RECEPCIÓN"),
                value=date.today(),
                key=f"nuevo_pedido_fecha_recepcion_{form_version}",
            )
            dias_entrega = st.number_input(
                display_field_label("DÍAS DE ENTREGA"),
                min_value=0,
                value=0,
                step=1,
                key=f"nuevo_pedido_dias_entrega_{form_version}",
            )
            fecha_para_entrega = st.date_input(
                display_field_label("FECHA PARA ENTREGA"),
                value=date.today(),
                key=f"nuevo_pedido_fecha_entrega_{form_version}",
            )

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

    clean_aparato = clean_display_value(aparato)
    clean_vendedor = clean_display_value(vendedor)
    clean_servicio = clean_display_value(servicio)
    clean_archivos_recibidos = clean_display_value(archivos_recibidos)
    clean_pago = clean_display_value(pago)

    default_status = STATUS_OPTIONS[0] if STATUS_OPTIONS else ""

    row_dict = {
        ID_COLUMN: cleaned_identifier,
        "APARATO": clean_aparato,
        STATUS_COLUMN: default_status,
        "NOMBRE DOCTOR": nombre_doctor,
        "NOMBRE PACIENTE": nombre_paciente,
        "DETALLE COMENTARIOS": detalle_comentarios,
        "VENDEDOR": clean_vendedor,
        "SERVICIO": clean_servicio,
        "ARCHIVOS RECIBIDOS": clean_archivos_recibidos,
        "PAGO": clean_pago,
        "DÍAS DE ENTREGA": int(dias_entrega),
        "FECHA DE RECEPCIÓN": format_sheet_date(fecha_recepcion),
        "FECHA PARA ENTREGA": format_sheet_date(fecha_para_entrega),
    }

    try:
        target_row = append_estatus_row(row_dict)
    except Exception as exc:
        st.error("No se pudo guardar el nuevo pedido en ESTATUS APARATOS.")
        st.exception(exc)
        return

    timing_log_saved = False
    if default_status:
        try:
            register_status_change(
                identifier=cleaned_identifier,
                apparatus=clean_aparato,
                previous_status="",
                new_status=default_status,
                change_comment="Registro inicial desde app",
            )
            timing_log_saved = True
        except Exception as exc:
            st.warning(
                "El pedido sí se guardó en ESTATUS APARATOS, "
                "pero no se pudo registrar el log inicial en TIEMPOS_APARATOS."
            )
            st.exception(exc)

    st.cache_data.clear()
    log_text = (
        "y también se registró el log inicial en TIEMPOS_APARATOS"
        if timing_log_saved
        else "pero quedó pendiente el log inicial en TIEMPOS_APARATOS"
    )
    st.session_state["nuevo_pedido_success_message"] = (
        f"Pedido {cleaned_identifier} guardado en ESTATUS APARATOS "
        f"en la fila {target_row}; {log_text}."
    )
    st.session_state["nuevo_pedido_form_version"] = form_version + 1
    st.session_state["nuevo_pedido_show_celebration"] = True
    st.rerun()


def render_estatus_tab() -> None:
    st.subheader("📋 Seguimiento de pedidos")
    render_success_feedback(
        "estatus_success_message",
        "estatus_show_success_toast",
        "clear_estatus_success_message",
    )
    render_warning_feedback(
        "estatus_warning_message", "clear_estatus_warning_message"
    )
    estatus_df = read_sheet_df(SHEET_ESTATUS)

    if estatus_df.empty:
        st.info("La hoja ESTATUS APARATOS no tiene registros para mostrar.")
        return
    if ID_COLUMN not in estatus_df.columns:
        st.error(f"No se encontró la columna obligatoria '{ID_COLUMN}' en ESTATUS APARATOS.")
        return

    filtered_df = apply_estatus_filters(estatus_df)
    st.caption(f"Registros encontrados: {len(filtered_df)}")
    st.dataframe(
        filtered_df,
        use_container_width=True,
        hide_index=True,
        column_config=build_dataframe_column_config(filtered_df),
    )

    selectable_ids = [
        clean_cell(value).strip()
        for value in filtered_df[ID_COLUMN].tolist()
        if clean_cell(value).strip()
    ]
    if not selectable_ids:
        st.warning("No hay registros con Columna 1 para seleccionar en el resultado filtrado.")
        return

    selection_labels = {}
    for _, option_row in filtered_df.iterrows():
        option_id = clean_cell(option_row.get(ID_COLUMN, "")).strip()
        if not option_id or option_id in selection_labels:
            continue
        label_parts = [f"🆔 {option_id}"]
        status = display_selectbox_value(
            STATUS_COLUMN, clean_cell(option_row.get(STATUS_COLUMN, "")).strip()
        )
        doctor = clean_cell(option_row.get("NOMBRE DOCTOR", "")).strip()
        vendedor = display_selectbox_value(
            "VENDEDOR", clean_cell(option_row.get("VENDEDOR", "")).strip()
        )
        if status:
            label_parts.append(f"🚦 {status}")
        if doctor:
            label_parts.append(f"👩‍⚕️ {doctor}")
        if vendedor:
            label_parts.append(f"🤝 {vendedor}")
        selection_labels[option_id] = " | ".join(label_parts)

    selected_id = st.selectbox(
        "🆔 Selecciona un registro por Columna 1, STATUS, doctor o vendedor",
        selectable_ids,
        format_func=lambda option: selection_labels.get(option, option),
    )
    selected_rows = estatus_df[estatus_df[ID_COLUMN].astype(str).str.strip() == selected_id]
    if selected_rows.empty:
        st.warning("No se pudo encontrar el registro seleccionado.")
        return

    row = selected_rows.iloc[0]
    st.markdown("### 📝 Editar registro seleccionado")
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

        submitted = st.form_submit_button("💾 Guardar cambios")

    if submitted:
        changes = {
            column: clean_display_value(clean_cell(value))
            for column, value in edited_values.items()
            if canonical_column_name(column) != ID_COLUMN
            and not values_equivalent_for_column(column, row.get(column, ""), value)
        }
        if not changes:
            st.info("No se detectaron cambios para guardar.")
            return

        canonical_changes = build_canonical_changes(changes)
        previous_status = clean_display_value(
            clean_cell(get_row_value_by_column(row, STATUS_COLUMN, ""))
        )
        new_status = clean_display_value(
            clean_cell(canonical_changes.get(STATUS_COLUMN, previous_status))
        )
        apparatus = clean_display_value(
            clean_cell(
                canonical_changes.get(
                    "APARATO", get_row_value_by_column(row, "APARATO", "")
                )
            )
        )

        update_result = update_row_by_columna_1(selected_id, changes)
        if update_result["success"]:
            if STATUS_COLUMN in canonical_changes and previous_status != new_status:
                register_status_change(
                    identifier=clean_cell(changes.get(ID_COLUMN, selected_id)).strip(),
                    apparatus=apparatus,
                    previous_status=previous_status,
                    new_status=new_status,
                    previous_identifier=selected_id,
                )
            st.cache_data.clear()
            saved_columns = ", ".join(
                display_field_label(column) for column in update_result["updated_columns"]
            )
            st.session_state["estatus_success_message"] = (
                f"Registro actualizado correctamente. Columnas guardadas: {saved_columns}."
            )
            st.session_state["estatus_show_success_toast"] = True
            if update_result["skipped_columns"]:
                skipped_columns = ", ".join(
                    display_field_label(column) for column in update_result["skipped_columns"]
                )
                st.session_state["estatus_warning_message"] = (
                    "No se guardaron estas columnas porque no encontré su encabezado "
                    f"en Google Sheets: {skipped_columns}."
                )
            else:
                st.session_state.pop("estatus_warning_message", None)
            st.rerun()
        else:
            skipped_columns = ", ".join(
                display_field_label(column) for column in update_result["skipped_columns"]
            )
            detail = f" Columnas sin guardar: {skipped_columns}." if skipped_columns else ""
            reason = f" Motivo: {update_result['error']}" if update_result["error"] else ""
            st.error(
                "No se pudo actualizar el registro. Revisa que Columna 1 sea única "
                f"y exista en la hoja.{detail}{reason}"
            )


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
            **build_dataframe_column_config(ordered_df),
            "ESTADO_ALERTA_VISUAL": st.column_config.Column(
                display_field_label("ESTADO_ALERTA_VISUAL")
            ),
            "HORAS_TRANSCURRIDAS": st.column_config.Column(
                display_field_label("HORAS_TRANSCURRIDAS")
            ),
        },
    )

    counts = ordered_df["ESTADO_ALERTA_VISUAL"].value_counts().to_dict()
    st.markdown("#### 📊 Resumen")
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
    st.dataframe(
        procesos_df,
        use_container_width=True,
        hide_index=True,
        column_config=build_dataframe_column_config(procesos_df),
    )


# ==============================
# 🚀 APP STREAMLIT
# ==============================
st.set_page_config(page_title="Control de Aparatos – ARTTDLAB", layout="wide")
apply_custom_css()
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
