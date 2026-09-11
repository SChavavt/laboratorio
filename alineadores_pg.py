"""Aplicación de seguimiento para ``Control ALINEADORES``.

La hoja ``PROCESOS POR PRODUCTO`` es la fuente de verdad del flujo y de los
plazos. Los estados marcados como pausa se ofrecen de forma opcional y no
forman parte de la secuencia obligatoria.
"""

from __future__ import annotations

import hashlib
import hmac
import html
import json
import math
import re
import time
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Callable, Iterable
from zoneinfo import ZoneInfo

import gspread
import pandas as pd
import streamlit as st
from google.oauth2.service_account import Credentials
from gspread.cell import Cell
from gspread.utils import rowcol_to_a1

from aligners_grid import build_aligners_grid_options, render_aligners_grid


# ==============================
# Configuración
# ==============================
SCOPE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

DEFAULT_SPREADSHEET_ID = "1wNKD4bl__w1qMG182xFfu-1NlbcWTZdFEpa-h8ZLtik"
SHEET_ORDERS = "ALINEADORES (nuevo)"
SHEET_PROCESSES = "PROCESOS POR PRODUCTO"
SHEET_TIMES = "TIEMPOS_ALINEADORES"
ORDER_HEADER_ROW = 2

ID_COLUMN = "No. Orden"
STATUS_COLUMN = "STATUS"
PRODUCT_COLUMN = "PRODUCTO"
APP_TIMEZONE_NAME = "America/Mexico_City"
APP_TIMEZONE = ZoneInfo(APP_TIMEZONE_NAME)

APP_USERS = ("Admin", "Jime", "Lesly", "Vero")

PAUSE_STATUSES = (
    "BORRADOR TITAN",
    "SOLICITUD DE CAMBIOS",
    "IMPRESIÓN EN PAUSA",
)
TERMINAL_STATUSES = {"ENVIADO", "CANCELADO", "CANCELO"}

TIMES_HEADERS = [
    "ID_LOG",
    ID_COLUMN,
    PRODUCT_COLUMN,
    "FASE_ORDEN",
    STATUS_COLUMN,
    "STATUS_SIGUIENTE",
    "USUARIO",
    "FECHA_INICIO",
    "HORA_INICIO",
    "FECHA_LIMITE",
    "HORA_LIMITE",
    "FECHA_FIN",
    "HORA_FIN",
    "DURACION_HORAS",
    "TIEMPO_CONFIGURADO",
    "TIEMPO_MAXIMO_HORAS",
    "ESTADO_ALERTA",
    "COMENTARIOS_CAMBIO",
    "STATUS_REANUDAR",
    "ES_PAUSA",
    "FECHA_REGISTRO_LOG",
]

PRIMARY_COLUMNS = [
    ID_COLUMN,
    STATUS_COLUMN,
    "TIPO",
    PRODUCT_COLUMN,
    "ETAPA SOLICITUD",
    "NOMBRE DOCTOR",
    "NOMBRE PACIENTE",
    "DETALLE COMENTARIOS",
    "VENDEDOR",
    "SERVICIO",
    "PAQUETE MARCA BLANCA",
    "ARCHIVOS RECIBIDOS",
    "FECHA DE RECEPCIÓN",
    "FECHA PAGO PLANEACION",
    "FECHA SUBIDO A TITAN",
    "FECHA ENTREGA TITAN",
    "FECHA OBJETIVO ENVÍO",
]

DATE_COLUMNS = {
    "FECHA DE RECEPCIÓN",
    "FECHA PAGO PLANEACION",
    "FECHA SUBIDO A TITAN",
    "FECHA ENTREGA TITAN",
    "FECHA OBJETIVO ENVÍO",
}

SELECT_COLUMNS = {
    "ETAPA SOLICITUD",
    "VENDEDOR",
    "SERVICIO",
    "PAQUETE MARCA BLANCA",
    "ARCHIVOS RECIBIDOS",
}

COMPUTED_COLUMNS = [
    "SEMÁFORO",
    "HORAS EN ETAPA",
    "PLAZO HORAS",
    "LÍMITE ETAPA",
    "SIGUIENTE ETAPA",
    "DETALLE SEMÁFORO",
]
AUTOMATIC_COLUMNS = {*COMPUTED_COLUMNS, "FECHA OBJETIVO ENVÍO"}


def aligners_editable_columns(available_columns: Any = ()) -> set[str]:
    """Devuelve las columnas de la hoja que se pueden corregir desde Seguimiento."""
    protected = {"SELECCIONAR", ID_COLUMN, *COMPUTED_COLUMNS}
    return set(available_columns) - protected

GRID_COLUMNS = [
    ID_COLUMN,
    "SEMÁFORO",
    PRODUCT_COLUMN,
    STATUS_COLUMN,
    "NOMBRE DOCTOR",
    "NOMBRE PACIENTE",
    "DETALLE COMENTARIOS",
    "TIPO",
    "ETAPA SOLICITUD",
    "VENDEDOR",
    "SERVICIO",
    "PAQUETE MARCA BLANCA",
    "ARCHIVOS RECIBIDOS",
    "FECHA DE RECEPCIÓN",
    "FECHA PAGO PLANEACION",
    "FECHA SUBIDO A TITAN",
    "FECHA ENTREGA TITAN",
    "FECHA OBJETIVO ENVÍO",
    "HORAS EN ETAPA",
    "PLAZO HORAS",
    "LÍMITE ETAPA",
    "SIGUIENTE ETAPA",
    "DETALLE SEMÁFORO",
]

STATUS_EMOJIS = {
    "REVISION DE ARCHIVOS": "🔵",
    "POR HACER SETUP": "🧩",
    "BORRADOR TITAN": "⏸️",
    "EN PLANEACION": "🟡",
    "REVISION DEL SETUP POR DR": "👩‍⚕️",
    "SOLICITUD DE CAMBIOS": "⏸️",
    "CASO APROBADO": "✅",
    "LISTO STL PARA IMPRESION": "📁",
    "EN IMPRESION": "🖨️",
    "IMPRESION EN PAUSA": "⏸️",
    "LISTO P/ENVIO": "📦",
    "ENVIADO": "🚚",
    "CANCELADO": "🗄️",
}

STATUS_COLORS = {
    "REVISION DE ARCHIVOS": ("#C9E6EC", "#24545E"),
    "POR HACER SETUP": ("#DDEBFF", "#123A63"),
    "BORRADOR TITAN": ("#F4CCCC", "#8A1C1C"),
    "EN PLANEACION": ("#FFE89A", "#6B4B00"),
    "REVISION DEL SETUP POR DR": ("#E6E6E6", "#333333"),
    "SOLICITUD DE CAMBIOS": ("#F4CCCC", "#8A1C1C"),
    "CASO APROBADO": ("#C8F1D2", "#175B2B"),
    "LISTO STL PARA IMPRESION": ("#D8EAF7", "#174D6B"),
    "EN IMPRESION": ("#DCC4F4", "#5B337B"),
    "IMPRESION EN PAUSA": ("#F4CCCC", "#8A1C1C"),
    "LISTO P/ENVIO": ("#D9F0C7", "#2B5A18"),
    "ENVIADO": ("#7BE84D", "#174B0B"),
    "CANCELADO": ("#1C4587", "#FFFFFF"),
}

PRODUCT_EMOJIS = {
    "CONVENCIONAL": "🦷",
    "GRAPHY": "🔷",
    "RETENEDORES": "🛡️",
    "MODELOS": "🧱",
    "SETUP": "🧩",
    "GUARDA": "🌙",
    "GUIA": "🧭",
}

PRODUCT_COLORS = {
    "CONVENCIONAL": ("#E7E0F4", "#4A2C6F"),
    "GRAPHY": ("#D8EAF7", "#174D6B"),
    "RETENEDORES": ("#DDEED9", "#285A27"),
    "MODELOS": ("#EEE5D8", "#654B2B"),
    "SETUP": ("#F7E0C5", "#754516"),
    "GUARDA": ("#E1E5F5", "#39466F"),
    "GUIA": ("#D9EFEA", "#1C6157"),
}

ALERT_COLORS = {
    "🔴 Atrasado": ("#FFD8DF", "#A72B48"),
    "🟡 Por vencer": ("#FFE7AB", "#885A08"),
    "🟣 En pausa": ("#E9D8FD", "#553C9A"),
    "🟢 En tiempo": ("#BFEBDC", "#13654E"),
    "⚪ Sin medición": ("#DCE3F2", "#4C5E7D"),
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


@dataclass(frozen=True)
class ProcessDefinition:
    """Flujo normal y pausas opcionales de un producto."""

    name: str
    normal_steps: tuple[tuple[str, str | None], ...]
    pauses: tuple[str, ...]

    @property
    def normal_statuses(self) -> tuple[str, ...]:
        return tuple(status for status, _ in self.normal_steps)

    def time_for(self, status: str) -> str | None:
        wanted = status_key(status)
        return next(
            (limit for step, limit in self.normal_steps if status_key(step) == wanted),
            None,
        )


# ==============================
# Normalización y flujo
# ==============================
def app_now() -> datetime:
    return datetime.now(APP_TIMEZONE).replace(tzinfo=None)


def app_today() -> date:
    return app_now().date()


def clean_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    text = str(value)
    return "" if text.strip().lower() == "nan" else text


def normalize_text(value: Any) -> str:
    text = clean_cell(value).strip()
    normalized = unicodedata.normalize("NFKD", text)
    without_accents = "".join(
        character for character in normalized if not unicodedata.combining(character)
    )
    return without_accents.upper()


def strip_visual_prefix(value: Any) -> str:
    text = clean_cell(value).strip()
    return re.sub(r"^[^A-Za-zÁÉÍÓÚÜÑ0-9]+\s*", "", text).strip()


def status_key(value: Any) -> str:
    key = normalize_text(strip_visual_prefix(value))
    key = key.replace("SET UP", "SETUP")
    key = re.sub(r"\s*/\s*", "/", key)
    key = re.sub(r"\s+", " ", key).strip(" .")
    if key == "CANCELO":
        return "CANCELADO"
    return key


def product_key(value: Any) -> str:
    key = normalize_text(strip_visual_prefix(value))
    key = re.sub(r"[^A-Z0-9]+", "", key)
    if key.startswith("CONVENC"):
        return "CONVENCIONAL"
    return key


def canonical_status(value: Any, configured: Iterable[str] = ()) -> str:
    cleaned = strip_visual_prefix(value)
    key = status_key(cleaned)
    if key == "CANCELADO":
        return "CANCELADO"
    for candidate in configured:
        if status_key(candidate) == key:
            return candidate
    return cleaned


def is_pause_status(value: Any) -> bool:
    return status_key(value) in {status_key(item) for item in PAUSE_STATUSES}


def is_terminal_status(value: Any) -> bool:
    return status_key(value) in {status_key(item) for item in TERMINAL_STATUSES}


def parse_process_matrix(values: list[list[Any]]) -> dict[str, ProcessDefinition]:
    """Convierte la matriz horizontal de PROCESOS POR PRODUCTO en configuración."""

    if len(values) < 3:
        return {}
    width = max((len(row) for row in values), default=0)
    definitions: dict[str, ProcessDefinition] = {}
    pause_keys = {status_key(item) for item in PAUSE_STATUSES}

    for column in range(0, width, 2):
        product_name = clean_cell(values[0][column] if column < len(values[0]) else "").strip()
        if not product_name:
            continue
        normal_steps: list[tuple[str, str | None]] = []
        pauses: list[str] = []
        seen: set[str] = set()
        for row in values[2:]:
            raw_status = clean_cell(row[column] if column < len(row) else "").strip()
            if not raw_status:
                continue
            raw_limit = clean_cell(row[column + 1] if column + 1 < len(row) else "").strip()
            status = canonical_status(raw_status)
            key = status_key(status)
            if key in seen:
                continue
            seen.add(key)
            if key in pause_keys:
                pauses.append(status)
            elif key != "CANCELADO":
                normal_steps.append((status, raw_limit or None))

        definitions[product_key(product_name)] = ProcessDefinition(
            name=product_name,
            normal_steps=tuple(normal_steps),
            pauses=tuple(pauses),
        )
    return definitions


def get_process_definition(
    product: Any, definitions: dict[str, ProcessDefinition]
) -> ProcessDefinition | None:
    return definitions.get(product_key(product))


def configured_statuses(definitions: dict[str, ProcessDefinition]) -> list[str]:
    statuses = [
        status
        for definition in definitions.values()
        for status in (*definition.normal_statuses, *definition.pauses)
    ]
    return list(dict.fromkeys([*statuses, "CANCELADO"]))


def get_allowed_next_statuses(
    product: Any,
    current_status: Any,
    definitions: dict[str, ProcessDefinition],
) -> list[str]:
    """Ofrece el siguiente paso normal, pausas opcionales y cancelación.

    Al salir de una pausa se muestran todas las etapas normales. Esto permite
    volver al paso correcto sin convertir la pausa en un escalón obligatorio.
    """

    definition = get_process_definition(product, definitions)
    all_statuses = configured_statuses(definitions)
    current = canonical_status(current_status, all_statuses)
    if is_terminal_status(current):
        return [current]
    if definition is None:
        return list(dict.fromkeys([current, "CANCELADO"] if current else ["CANCELADO"]))

    normal = list(definition.normal_statuses)
    pauses = list(definition.pauses)
    current_key = status_key(current)
    normal_keys = [status_key(item) for item in normal]

    if current_key in {status_key(item) for item in pauses}:
        return list(dict.fromkeys([current, *normal, *pauses, "CANCELADO"]))
    if current_key not in normal_keys:
        return list(dict.fromkeys([current, *normal, *pauses, "CANCELADO"]))

    index = normal_keys.index(current_key)
    allowed = [normal[index]]
    if index + 1 < len(normal):
        allowed.append(normal[index + 1])
    if status_key(normal[index]) != "ENVIADO":
        allowed.extend([*pauses, "CANCELADO"])
    return list(dict.fromkeys(allowed))


def next_normal_status(
    product: Any,
    current_status: Any,
    definitions: dict[str, ProcessDefinition],
) -> str:
    definition = get_process_definition(product, definitions)
    if definition is None or is_pause_status(current_status):
        return "Elegir etapa al reanudar" if is_pause_status(current_status) else "—"
    keys = [status_key(item) for item in definition.normal_statuses]
    current_key = status_key(current_status)
    if current_key not in keys:
        return "Revisar flujo"
    index = keys.index(current_key)
    return definition.normal_statuses[index + 1] if index + 1 < len(keys) else "—"


def get_time_limit(
    product: Any,
    status: Any,
    definitions: dict[str, ProcessDefinition],
) -> str | None:
    definition = get_process_definition(product, definitions)
    if definition is None or is_pause_status(status):
        return None
    return definition.time_for(canonical_status(status, definition.normal_statuses))


def status_display_value(value: Any, definitions: dict[str, ProcessDefinition] | None = None) -> str:
    configured = configured_statuses(definitions or {})
    status = canonical_status(value, configured)
    if not status:
        return ""
    emoji = STATUS_EMOJIS.get(status_key(status), "⚪")
    return f"{emoji} {status}"


def product_display_value(value: Any) -> str:
    product = strip_visual_prefix(value)
    if not product:
        return ""
    emoji = PRODUCT_EMOJIS.get(product_key(product), "🦷")
    return f"{emoji} {product}"


def status_palette_value(status: Any) -> tuple[str, str]:
    return STATUS_COLORS.get(status_key(status), ("#E8E8EE", "#3D4350"))


def product_palette_value(product: Any) -> tuple[str, str]:
    return PRODUCT_COLORS.get(product_key(product), ("#E8E8EE", "#3D4350"))


# ==============================
# Fechas y tiempos hábiles
# ==============================
def parse_datetime_text(value: Any) -> pd.Timestamp:
    text = clean_cell(value).strip()
    if not text:
        return pd.NaT
    normalized = " ".join(text.replace("/", "-").split())
    for date_format in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%d-%m-%Y %H:%M:%S",
        "%d-%m-%Y %H:%M",
        "%d-%m-%Y",
    ):
        try:
            return pd.Timestamp(datetime.strptime(normalized, date_format))
        except ValueError:
            continue
    return pd.to_datetime(text, errors="coerce", dayfirst=True)


def parse_simple_date(value: Any) -> date | None:
    text = clean_cell(value).strip()
    if not text:
        return None
    normalized = normalize_text(text).replace("/", "-")
    parts = normalized.split()
    if len(parts) >= 2 and parts[0].isdigit() and parts[1] in SPANISH_MONTHS:
        year = app_today().year
        if len(parts) >= 3 and parts[2].isdigit():
            year = int(parts[2])
        try:
            return date(year, SPANISH_MONTHS[parts[1]], int(parts[0]))
        except ValueError:
            return None
    parsed = parse_datetime_text(text)
    return None if pd.isna(parsed) else parsed.date()


def values_equivalent(column: str, old_value: Any, new_value: Any) -> bool:
    old_text = strip_visual_prefix(old_value) if column in {STATUS_COLUMN, PRODUCT_COLUMN} else clean_cell(old_value).strip()
    new_text = strip_visual_prefix(new_value) if column in {STATUS_COLUMN, PRODUCT_COLUMN} else clean_cell(new_value).strip()
    if column == STATUS_COLUMN:
        return status_key(old_text) == status_key(new_text)
    if column == PRODUCT_COLUMN:
        return product_key(old_text) == product_key(new_text)
    if column in DATE_COLUMNS:
        old_date, new_date = parse_simple_date(old_text), parse_simple_date(new_text)
        if old_date is not None and new_date is not None:
            return old_date == new_date
    return old_text == new_text


def is_business_day(value: datetime | date) -> bool:
    current = value.date() if isinstance(value, datetime) else value
    return current.weekday() < 5


def parse_time_limit_to_business_hours(value: Any) -> float | None:
    cleaned = normalize_text(value).replace("<", " ").strip()
    parts = cleaned.split()
    if not parts:
        return None
    try:
        amount = float(parts[0].replace(",", "."))
    except ValueError:
        return None
    unit = parts[1] if len(parts) > 1 else ""
    if unit.startswith("DIA"):
        return amount * 24
    if unit.startswith("HR") or unit.startswith("HORA"):
        return amount
    return None


def add_business_time(start: datetime, configured_time: Any) -> tuple[str, str]:
    maximum = parse_time_limit_to_business_hours(configured_time)
    if maximum is None:
        return "", ""
    current = start
    if not is_business_day(current):
        while not is_business_day(current):
            current = datetime.combine(current.date() + timedelta(days=1), current.time())
    remaining = maximum
    while remaining > 0:
        step = min(remaining, 1.0)
        current += timedelta(hours=step)
        if is_business_day(current):
            remaining -= step
    return current.strftime("%Y-%m-%d"), current.strftime("%H:%M:%S")


def business_hours_elapsed(start: datetime, end: datetime) -> float:
    if end <= start:
        return 0.0
    current = start
    elapsed = 0.0
    while current < end:
        next_midnight = datetime.combine(current.date() + timedelta(days=1), datetime.min.time())
        next_step = min(next_midnight, end)
        if is_business_day(current):
            elapsed += (next_step - current).total_seconds() / 3600
        current = next_step
    return elapsed


def parse_start_datetime(date_value: Any, time_value: Any) -> datetime | None:
    date_text = clean_cell(date_value).strip()
    time_text = clean_cell(time_value).strip()
    if not date_text:
        return None
    parsed = parse_datetime_text(f"{date_text} {time_text}".strip())
    return None if pd.isna(parsed) else parsed.to_pydatetime()


def format_duration(start: datetime | None, end: datetime | None = None) -> str:
    if start is None:
        return ""
    return f"{business_hours_elapsed(start, end or app_now()):.2f}"


def signal_for_elapsed(elapsed: float, maximum: float) -> str:
    epsilon = 1e-9
    if elapsed + epsilon >= maximum:
        return "🔴 Atrasado"
    if elapsed + epsilon >= maximum * 0.8:
        return "🟡 Por vencer"
    return "🟢 En tiempo"


def signal_rank(signal: str) -> int:
    return {
        "🔴 Atrasado": 0,
        "🟡 Por vencer": 1,
        "🟣 En pausa": 2,
        "⚪ Sin medición": 3,
        "🟢 En tiempo": 4,
    }.get(signal, 3)


# ==============================
# Autenticación y Google Sheets
# ==============================
def is_google_sheets_rate_limit_error(exc: Exception) -> bool:
    if not isinstance(exc, gspread.exceptions.APIError):
        return False
    response = getattr(exc, "response", None)
    status_code = getattr(response, "status_code", None)
    text = str(exc).upper()
    return status_code == 429 or "RATE_LIMIT_EXCEEDED" in text or "RESOURCE_EXHAUSTED" in text


def run_gsheets_request(
    operation: Callable[[], Any], *, retries: int = 3, base_delay: float = 1.0
) -> Any:
    for attempt in range(retries + 1):
        try:
            return operation()
        except Exception as exc:
            if not is_google_sheets_rate_limit_error(exc) or attempt >= retries:
                raise
            time.sleep(base_delay * (2**attempt))
    raise RuntimeError("No se pudo completar la operación de Google Sheets.")


def get_user_passwords(secret_source: Any | None = None) -> dict[str, str]:
    source = st.secrets if secret_source is None and hasattr(st, "secrets") else (secret_source or {})
    configured: dict[str, str] = {}
    auth = source.get("auth", {}) if hasattr(source, "get") else {}
    if hasattr(auth, "get"):
        passwords = auth.get("passwords", {})
        if hasattr(passwords, "items"):
            configured.update({str(user): str(password) for user, password in passwords.items()})
    root_passwords = source.get("user_passwords", {}) if hasattr(source, "get") else {}
    if hasattr(root_passwords, "items"):
        configured.update(
            {str(user): str(password) for user, password in root_passwords.items()}
        )
    return {
        user: configured[user]
        for user in APP_USERS
        if clean_cell(configured.get(user, "")).strip()
    }


def missing_user_passwords(passwords: dict[str, str] | None = None) -> list[str]:
    configured = get_user_passwords() if passwords is None else passwords
    return [user for user in APP_USERS if not clean_cell(configured.get(user, "")).strip()]


def password_matches(password: str, stored_value: str) -> bool:
    if stored_value.startswith("pbkdf2_sha256$"):
        try:
            _, rounds_text, salt, expected = stored_value.split("$", 3)
            candidate = hashlib.pbkdf2_hmac(
                "sha256", password.encode(), salt.encode(), int(rounds_text)
            ).hex()
        except (TypeError, ValueError):
            return False
        return hmac.compare_digest(candidate, expected)
    return hmac.compare_digest(password, stored_value)


def get_query_param_value(key: str) -> str:
    """Lee un parámetro del URL de Streamlit de forma compatible."""

    value = st.query_params.get(key, "")
    if isinstance(value, list):
        return clean_cell(value[0]).strip() if value else ""
    return clean_cell(value).strip()


def login_url_signature(
    username: str, passwords: dict[str, str] | None = None
) -> str:
    """Firma el usuario recordado para impedir cambiarlo editando el URL."""

    configured = get_user_passwords() if passwords is None else passwords
    signing_value = clean_cell(configured.get(username, "")).strip()
    if username not in APP_USERS or not signing_value:
        return ""
    return hmac.new(
        signing_value.encode(),
        f"control-alineadores:{username}".encode(),
        hashlib.sha256,
    ).hexdigest()


def valid_url_login(
    username: str,
    signature: str,
    passwords: dict[str, str] | None = None,
) -> bool:
    expected = login_url_signature(username, passwords)
    return bool(expected and signature and hmac.compare_digest(signature, expected))


def persist_login_in_url(username: str) -> None:
    """Recuerda la sesión en el enlace mediante un usuario firmado."""

    signature = login_url_signature(username)
    if not signature:
        return
    st.query_params["usuario"] = username
    st.query_params["firma"] = signature


def clear_persisted_login_url() -> None:
    for key in ("usuario", "firma"):
        if key in st.query_params:
            del st.query_params[key]


def restore_user_from_url() -> str:
    username = get_query_param_value("usuario")
    signature = get_query_param_value("firma")
    return username if valid_url_login(username, signature) else ""


def set_authenticated_user(username: str, *, remember_in_url: bool = True) -> None:
    st.session_state["aligners_authenticated_user"] = username
    st.session_state.pop("aligners_login_error", None)
    if remember_in_url:
        persist_login_in_url(username)


def login_user(username: str, password: str) -> bool:
    expected = get_user_passwords().get(username, "")
    if not expected or not password_matches(password, expected):
        return False
    set_authenticated_user(username)
    return True


def logout_user() -> None:
    for key in [
        "aligners_authenticated_user",
        "aligners_snapshot",
        "aligners_editor_key",
        "aligners_editor_baseline",
    ]:
        st.session_state.pop(key, None)
    clear_persisted_login_url()


def require_authenticated_user() -> str | None:
    current_user = st.session_state.get("aligners_authenticated_user")
    if current_user not in APP_USERS:
        remembered_user = restore_user_from_url()
        if remembered_user:
            set_authenticated_user(remembered_user, remember_in_url=False)
            current_user = remembered_user

    if current_user in APP_USERS:
        st.sidebar.success(f"Sesión activa: {current_user}")
        st.sidebar.caption(
            "El usuario queda recordado en el link. Todos pueden atender cualquier producto."
        )
        if st.sidebar.button("🚪 Cerrar sesión", key="aligners_logout"):
            logout_user()
            st.rerun()
        return current_user

    missing = missing_user_passwords()
    if missing:
        st.error("Falta configurar la contraseña de: " + ", ".join(missing) + ".")
        st.info(
            "Usa los mismos hashes de la app de aparatos en Streamlit Secrets, "
            "dentro de [auth.passwords]."
        )
        return None

    st.subheader("🔐 Acceso requerido")
    st.caption(
        "Selecciona tu usuario e ingresa la misma contraseña de Control de aparatos. "
        "Después de entrar quedará recordado en el link."
    )
    with st.form("aligners_login_form"):
        username = st.selectbox("Usuario", list(APP_USERS))
        password = st.text_input("Contraseña", type="password")
        submitted = st.form_submit_button("🔓 Entrar y guardar usuario en link")
    if submitted:
        if login_user(username, password):
            st.rerun()
        st.session_state["aligners_login_error"] = "Usuario o contraseña incorrectos."
    if st.session_state.get("aligners_login_error"):
        st.error(st.session_state["aligners_login_error"])
    return None


def _get_gs_client():
    credentials_text = st.secrets["gsheets"]["google_credentials"]
    credentials_info = json.loads(credentials_text)
    credentials = Credentials.from_service_account_info(credentials_info, scopes=SCOPE)
    return gspread.authorize(credentials)


def configured_spreadsheet_id(secret_source: Any | None = None) -> str:
    """Usa una clave separada para no reemplazar el sheet_id de Control de aparatos."""

    source = st.secrets if secret_source is None and hasattr(st, "secrets") else (secret_source or {})
    gsheets = source.get("gsheets", {}) if hasattr(source, "get") else {}
    if hasattr(gsheets, "get"):
        for key in ("alineadores_sheet_id", "sheet_id_alineadores"):
            value = clean_cell(gsheets.get(key, "")).strip()
            if value:
                return value
    value = clean_cell(source.get("alineadores_sheet_id", "") if hasattr(source, "get") else "").strip()
    return value or DEFAULT_SPREADSHEET_ID


@st.cache_resource
def get_spreadsheet():
    client = _get_gs_client()
    return run_gsheets_request(lambda: client.open_by_key(configured_spreadsheet_id()))


@st.cache_resource
def get_worksheet(sheet_name: str):
    spreadsheet = get_spreadsheet()
    try:
        return run_gsheets_request(lambda: spreadsheet.worksheet(sheet_name))
    except gspread.WorksheetNotFound:
        if sheet_name != SHEET_TIMES:
            raise
        return run_gsheets_request(
            lambda: spreadsheet.add_worksheet(
                title=SHEET_TIMES,
                rows="2000",
                cols=str(len(TIMES_HEADERS)),
            )
        )


def ensure_unique_column_names(columns: list[Any]) -> list[str]:
    counts: dict[str, int] = {}
    result: list[str] = []
    for index, raw in enumerate(columns):
        base = clean_cell(raw).strip() or f"Columna_{index + 1}"
        count = counts.get(base, 0)
        candidate = base if count == 0 else f"{base}_{count + 1}"
        while candidate in result:
            count += 1
            candidate = f"{base}_{count + 1}"
        counts[base] = count + 1
        result.append(candidate)
    return result


def canonical_header_key(value: Any) -> str:
    return " ".join(normalize_text(value).split())


def canonical_column_name(value: Any) -> str:
    cleaned = clean_cell(value).strip()
    key = canonical_header_key(cleaned)
    for known in [*PRIMARY_COLUMNS, *TIMES_HEADERS]:
        if canonical_header_key(known) == key:
            return known
    return cleaned


def get_header_position(headers: list[str], column: str) -> int | None:
    wanted = canonical_header_key(column)
    return next(
        (index for index, header in enumerate(headers, start=1) if canonical_header_key(header) == wanted),
        None,
    )


def normalize_rows(values: list[list[Any]], headers: list[str]) -> list[list[str]]:
    width = len(headers)
    return [
        [clean_cell(item) for item in row[:width]] + [""] * max(width - len(row), 0)
        for row in values
    ]


@st.cache_data(ttl=30)
def read_order_values() -> list[list[str]]:
    return run_gsheets_request(lambda: get_worksheet(SHEET_ORDERS).get_all_values())


@st.cache_data(ttl=30)
def read_process_values() -> list[list[str]]:
    return run_gsheets_request(lambda: get_worksheet(SHEET_PROCESSES).get_all_values())


@st.cache_data(ttl=30)
def read_times_values() -> list[list[str]]:
    return run_gsheets_request(lambda: get_worksheet(SHEET_TIMES).get_all_values())


def clear_sheet_data_cache() -> None:
    st.cache_data.clear()


def read_orders_df() -> pd.DataFrame:
    values = read_order_values()
    if len(values) < ORDER_HEADER_ROW:
        return pd.DataFrame()
    raw_headers = values[ORDER_HEADER_ROW - 1]
    headers = [canonical_column_name(item) for item in ensure_unique_column_names(raw_headers)]
    rows = normalize_rows(values[ORDER_HEADER_ROW:], headers)
    frame = pd.DataFrame(rows, columns=headers)
    frame["_SHEET_ROW"] = range(ORDER_HEADER_ROW + 1, ORDER_HEADER_ROW + 1 + len(frame))
    if ID_COLUMN not in frame:
        return pd.DataFrame()
    frame = frame[frame[ID_COLUMN].astype(str).str.strip().ne("")].copy()
    if STATUS_COLUMN in frame:
        frame[STATUS_COLUMN] = frame[STATUS_COLUMN].map(canonical_status)
    return frame.reset_index(drop=True)


def read_times_df() -> pd.DataFrame:
    values = read_times_values()
    if not values:
        return pd.DataFrame(columns=TIMES_HEADERS)
    headers = [canonical_column_name(item) for item in ensure_unique_column_names(values[0])]
    rows = normalize_rows(values[1:], headers)
    return pd.DataFrame(rows, columns=headers)


def read_process_definitions() -> dict[str, ProcessDefinition]:
    return parse_process_matrix(read_process_values())


@st.cache_data(ttl=3600)
def ensure_times_headers() -> None:
    worksheet = get_worksheet(SHEET_TIMES)
    headers = run_gsheets_request(lambda: worksheet.row_values(1))
    if not headers:
        run_gsheets_request(lambda: worksheet.update("A1", [TIMES_HEADERS]))
        clear_sheet_data_cache()
        return
    missing = [header for header in TIMES_HEADERS if get_header_position(headers, header) is None]
    if missing:
        run_gsheets_request(lambda: worksheet.update("A1", [headers + missing]))
        clear_sheet_data_cache()


def prepare_sheet_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


# ==============================
# Bitácora y escrituras
# ==============================
def get_next_log_id(values: list[list[str]]) -> int:
    if len(values) <= 1:
        return 1
    headers = values[0]
    position = get_header_position(headers, "ID_LOG")
    if position is None:
        return 1
    identifiers = []
    for row in values[1:]:
        value = row[position - 1] if position <= len(row) else ""
        try:
            identifiers.append(int(float(value)))
        except (TypeError, ValueError):
            continue
    return max(identifiers, default=0) + 1


def close_active_times(identifier: str, next_status: str) -> str:
    """Cierra cualquier medición activa y devuelve la etapa de reanudación guardada."""

    worksheet = get_worksheet(SHEET_TIMES)
    values = run_gsheets_request(lambda: worksheet.get_all_values())
    if len(values) <= 1:
        return ""
    headers = values[0]
    positions = {header: get_header_position(headers, header) for header in TIMES_HEADERS}
    required = [ID_COLUMN, "FECHA_INICIO", "HORA_INICIO", "FECHA_FIN", "HORA_FIN"]
    if any(positions.get(header) is None for header in required):
        return ""

    now = app_now()
    updates: list[Cell] = []
    resume_status = ""
    for row_number, row in enumerate(values[1:], start=2):
        id_position = positions[ID_COLUMN]
        end_position = positions["FECHA_FIN"]
        row_identifier = row[id_position - 1] if id_position and id_position <= len(row) else ""
        date_finished = row[end_position - 1] if end_position and end_position <= len(row) else ""
        if clean_cell(row_identifier).strip() != clean_cell(identifier).strip() or clean_cell(date_finished).strip():
            continue

        start_date_position = positions["FECHA_INICIO"]
        start_time_position = positions["HORA_INICIO"]
        start = parse_start_datetime(
            row[start_date_position - 1] if start_date_position and start_date_position <= len(row) else "",
            row[start_time_position - 1] if start_time_position and start_time_position <= len(row) else "",
        )
        resume_position = positions.get("STATUS_REANUDAR")
        if resume_position and resume_position <= len(row):
            resume_status = clean_cell(row[resume_position - 1]).strip() or resume_status

        closing_values = {
            "STATUS_SIGUIENTE": next_status,
            "FECHA_FIN": now.strftime("%Y-%m-%d"),
            "HORA_FIN": now.strftime("%H:%M:%S"),
            "DURACION_HORAS": format_duration(start, now),
            "ESTADO_ALERTA": "Cerrado",
        }
        for column, value in closing_values.items():
            position = positions.get(column)
            if position:
                updates.append(Cell(row_number, position, value))
    if updates:
        run_gsheets_request(
            lambda: worksheet.update_cells(updates, value_input_option="USER_ENTERED")
        )
    return resume_status


def process_phase_order(
    product: Any, status: Any, definitions: dict[str, ProcessDefinition]
) -> str:
    if is_pause_status(status):
        return "PAUSA"
    if is_terminal_status(status):
        return "TERMINAL"
    definition = get_process_definition(product, definitions)
    if definition is None:
        return ""
    keys = [status_key(item) for item in definition.normal_statuses]
    return str(keys.index(status_key(status)) + 1) if status_key(status) in keys else ""


def register_status_change(
    *,
    identifier: str,
    product: str,
    previous_status: str,
    new_status: str,
    current_user: str,
    definitions: dict[str, ProcessDefinition],
    comment: str = "",
) -> None:
    """Cierra la etapa anterior y abre la nueva medición en TIEMPOS_ALINEADORES."""

    ensure_times_headers()
    configured = configured_statuses(definitions)
    previous = canonical_status(previous_status, configured)
    new = canonical_status(new_status, configured)
    previous_resume = close_active_times(identifier, new)
    clear_sheet_data_cache()

    worksheet = get_worksheet(SHEET_TIMES)
    values = run_gsheets_request(lambda: worksheet.get_all_values())
    headers = values[0] if values else TIMES_HEADERS
    now = app_now()
    configured_time = get_time_limit(product, new, definitions)
    maximum = parse_time_limit_to_business_hours(configured_time)
    limit_date, limit_time = add_business_time(now, configured_time)
    paused = is_pause_status(new)
    terminal = is_terminal_status(new)
    resume_status = ""
    if paused:
        resume_status = previous_resume or ("" if is_pause_status(previous) else previous)

    default_comment = f"Cambio desde la app: {previous or 'Sin status'} → {new}"
    combined_comment = (
        f"{default_comment}\nComentario: {comment.strip()}" if comment.strip() else default_comment
    )
    row = {
        "ID_LOG": get_next_log_id(values),
        ID_COLUMN: identifier,
        PRODUCT_COLUMN: product,
        "FASE_ORDEN": process_phase_order(product, new, definitions),
        STATUS_COLUMN: new,
        "STATUS_SIGUIENTE": next_normal_status(product, new, definitions) if not terminal else "",
        "USUARIO": current_user,
        "FECHA_INICIO": now.strftime("%Y-%m-%d"),
        "HORA_INICIO": now.strftime("%H:%M:%S"),
        "FECHA_LIMITE": limit_date,
        "HORA_LIMITE": limit_time,
        "FECHA_FIN": now.strftime("%Y-%m-%d") if terminal else "",
        "HORA_FIN": now.strftime("%H:%M:%S") if terminal else "",
        "DURACION_HORAS": "0.00" if terminal else "",
        "TIEMPO_CONFIGURADO": configured_time or "",
        "TIEMPO_MAXIMO_HORAS": "" if maximum is None else f"{maximum:g}",
        "ESTADO_ALERTA": (
            "Cerrado"
            if terminal
            else f"Pausa: {new}"
            if paused
            else "Sin tiempo configurado"
            if maximum is None
            else "En tiempo"
        ),
        "COMENTARIOS_CAMBIO": combined_comment,
        "STATUS_REANUDAR": resume_status,
        "ES_PAUSA": "Sí" if paused else "No",
        "FECHA_REGISTRO_LOG": now.strftime("%Y-%m-%d %H:%M:%S"),
    }
    run_gsheets_request(
        lambda: worksheet.append_row(
            [prepare_sheet_value(row.get(canonical_column_name(header), "")) for header in headers],
            value_input_option="USER_ENTERED",
        )
    )
    clear_sheet_data_cache()


def apply_status_style(
    worksheet: gspread.Worksheet, row_number: int, status_position: int, status: str
) -> None:
    background, foreground = status_palette_value(status)

    def color(hex_value: str) -> dict[str, float]:
        cleaned = hex_value.lstrip("#")
        return {
            "red": int(cleaned[0:2], 16) / 255,
            "green": int(cleaned[2:4], 16) / 255,
            "blue": int(cleaned[4:6], 16) / 255,
        }

    try:
        run_gsheets_request(
            lambda: worksheet.batch_format(
                [
                    {
                        "range": rowcol_to_a1(row_number, status_position),
                        "format": {
                            "backgroundColor": color(background),
                            "textFormat": {"foregroundColor": color(foreground), "bold": True},
                            "horizontalAlignment": "CENTER",
                        },
                    }
                ]
            )
        )
    except Exception:
        # El formato es complementario; nunca invalida un guardado de datos exitoso.
        pass


def update_order_row(
    identifier: str,
    changes: dict[str, Any],
    *,
    expected_values: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Actualiza celdas por número de orden con control de cambios concurrentes."""

    result = {"success": False, "updated_columns": [], "skipped_columns": [], "error": ""}
    if not changes:
        return result
    worksheet = get_worksheet(SHEET_ORDERS)
    values = run_gsheets_request(lambda: worksheet.get_all_values())
    if len(values) < ORDER_HEADER_ROW:
        result["error"] = "No encontré la fila 2 de encabezados en ALINEADORES (nuevo)."
        return result
    headers = values[ORDER_HEADER_ROW - 1]
    id_position = get_header_position(headers, ID_COLUMN)
    if id_position is None:
        result["error"] = f"No encontré la columna {ID_COLUMN}."
        return result

    matches: list[tuple[int, list[str]]] = []
    for row_number, row in enumerate(values[ORDER_HEADER_ROW:], start=ORDER_HEADER_ROW + 1):
        row_identifier = row[id_position - 1] if id_position <= len(row) else ""
        if clean_cell(row_identifier).strip() == clean_cell(identifier).strip():
            matches.append((row_number, row))
    if len(matches) != 1:
        result["error"] = (
            "El número de orden está duplicado." if len(matches) > 1 else "No encontré el pedido."
        )
        return result
    row_number, fresh_row = matches[0]

    for column, expected in (expected_values or {}).items():
        position = get_header_position(headers, column)
        actual = fresh_row[position - 1] if position and position <= len(fresh_row) else ""
        if not values_equivalent(column, expected, actual):
            result["error"] = f"Otro usuario cambió {column}. Actualiza antes de guardar."
            return result

    updates: list[Cell] = []
    for column, value in changes.items():
        position = get_header_position(headers, column)
        if position is None:
            result["skipped_columns"].append(column)
            continue
        updates.append(Cell(row_number, position, prepare_sheet_value(value)))
        result["updated_columns"].append(column)
    if not updates:
        result["error"] = "No encontré encabezados válidos para los cambios."
        return result
    try:
        run_gsheets_request(
            lambda: worksheet.update_cells(updates, value_input_option="USER_ENTERED")
        )
    except Exception as exc:
        result["error"] = f"Google Sheets rechazó la actualización: {exc}"
        return result
    if STATUS_COLUMN in changes:
        status_position = get_header_position(headers, STATUS_COLUMN)
        if status_position:
            apply_status_style(worksheet, row_number, status_position, str(changes[STATUS_COLUMN]))
    result["success"] = True
    return result


# ==============================
# Modelo de la mesa de trabajo
# ==============================
def canonical_orders_df(
    frame: pd.DataFrame, definitions: dict[str, ProcessDefinition]
) -> pd.DataFrame:
    result = frame.copy()
    result.columns = [canonical_column_name(column) for column in result.columns]
    result = result.loc[:, ~result.columns.duplicated()].copy()
    for column in result:
        if column != "_SHEET_ROW":
            result[column] = result[column].map(clean_cell)
    if ID_COLUMN in result:
        result[ID_COLUMN] = result[ID_COLUMN].str.strip()
    if STATUS_COLUMN in result:
        allowed = configured_statuses(definitions)
        result[STATUS_COLUMN] = result[STATUS_COLUMN].map(
            lambda value: canonical_status(value, allowed)
        )
    return result


def active_orders(
    frame: pd.DataFrame, definitions: dict[str, ProcessDefinition]
) -> pd.DataFrame:
    result = canonical_orders_df(frame, definitions)
    if not {ID_COLUMN, STATUS_COLUMN}.issubset(result.columns):
        return result.iloc[0:0]
    meaningful = [
        column
        for column in [PRODUCT_COLUMN, STATUS_COLUMN, "NOMBRE DOCTOR", "NOMBRE PACIENTE"]
        if column in result
    ]
    mask = (
        result[ID_COLUMN].ne("")
        & ~result[STATUS_COLUMN].map(is_terminal_status)
        & result[meaningful].apply(
            lambda row: any(clean_cell(value).strip() for value in row), axis=1
        )
    )
    return result[mask].reset_index(drop=True)


def canonical_times_df(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    result.columns = [canonical_column_name(column) for column in result.columns]
    result = result.loc[:, ~result.columns.duplicated()].copy()
    for column in result:
        result[column] = result[column].map(clean_cell)
    return result


def build_tracking_table(
    orders_df: pd.DataFrame,
    times_df: pd.DataFrame,
    definitions: dict[str, ProcessDefinition],
    *,
    now: datetime | None = None,
) -> pd.DataFrame:
    """Une pedidos y bitácora sin inventar una fecha de inicio."""

    cases = active_orders(orders_df, definitions)
    if cases.empty:
        for column in COMPUTED_COLUMNS:
            cases[column] = pd.Series(dtype="object")
        return cases
    logs = canonical_times_df(times_df)
    active_logs: dict[str, pd.DataFrame] = {}
    if {ID_COLUMN, "FECHA_FIN"}.issubset(logs.columns):
        open_logs = logs[logs["FECHA_FIN"].astype(str).str.strip().eq("")]
        active_logs = dict(tuple(open_logs.groupby(ID_COLUMN)))

    current_time = now or app_now()
    computed: list[dict[str, Any]] = []
    duplicate_orders = set(
        cases.loc[cases[ID_COLUMN].duplicated(keep=False), ID_COLUMN]
    )
    all_statuses = configured_statuses(definitions)

    for _, case in cases.iterrows():
        identifier = clean_cell(case.get(ID_COLUMN, "")).strip()
        product = clean_cell(case.get(PRODUCT_COLUMN, "")).strip()
        status = canonical_status(case.get(STATUS_COLUMN, ""), all_statuses)
        matches = active_logs.get(identifier, pd.DataFrame())
        matching_log: dict[str, Any] = {}
        detail = ""
        start: datetime | None = None
        elapsed: float | None = None
        maximum: float | None = None
        deadline = "—"

        if identifier in duplicate_orders:
            signal = "⚪ Sin medición"
            detail = "Número de orden duplicado; corrige la hoja antes de editar."
        elif is_pause_status(status):
            signal = "🟣 En pausa"
            if len(matches) == 1 and status_key(matches.iloc[0].get(STATUS_COLUMN, "")) == status_key(status):
                matching_log = matches.iloc[0].to_dict()
                start = parse_start_datetime(
                    matching_log.get("FECHA_INICIO", ""), matching_log.get("HORA_INICIO", "")
                )
                elapsed = business_hours_elapsed(start, current_time) if start else None
            suffix = f" · {elapsed:.2f} h hábiles" if elapsed is not None else " · inicia la medición para contar su duración"
            detail = f"Pausa: {status}{suffix}"
        elif get_process_definition(product, definitions) is None:
            signal = "⚪ Sin medición"
            detail = f"Producto {product or 'vacío'} sin configuración en {SHEET_PROCESSES}."
        elif len(matches) > 1:
            signal = "⚪ Sin medición"
            detail = "Hay varios registros de tiempo activos; usa Reparar medición."
        elif len(matches) == 0:
            signal = "⚪ Sin medición"
            detail = "Sin registro de inicio para la etapa actual."
        elif status_key(matches.iloc[0].get(STATUS_COLUMN, "")) != status_key(status):
            signal = "⚪ Sin medición"
            detail = "La bitácora activa no corresponde al status actual; usa Reparar medición."
        else:
            matching_log = matches.iloc[0].to_dict()
            start = parse_start_datetime(
                matching_log.get("FECHA_INICIO", ""), matching_log.get("HORA_INICIO", "")
            )
            maximum_value = pd.to_numeric(
                matching_log.get("TIEMPO_MAXIMO_HORAS", ""), errors="coerce"
            )
            if pd.notna(maximum_value) and math.isfinite(float(maximum_value)) and float(maximum_value) > 0:
                maximum = float(maximum_value)
            else:
                maximum = parse_time_limit_to_business_hours(
                    get_time_limit(product, status, definitions)
                )
            if start is None:
                signal = "⚪ Sin medición"
                detail = "La fecha de inicio de la bitácora no es válida."
            elif maximum is None:
                signal = "⚪ Sin medición"
                detail = "La etapa no tiene un tiempo configurado."
            else:
                elapsed = business_hours_elapsed(start, current_time)
                signal = signal_for_elapsed(elapsed, maximum)
                detail = (
                    f"{elapsed:.2f} de {maximum:g} h hábiles consumidas "
                    f"({elapsed / maximum:.0%})."
                )
            deadline = " ".join(
                filter(
                    None,
                    [
                        clean_cell(matching_log.get("FECHA_LIMITE", "")).strip(),
                        clean_cell(matching_log.get("HORA_LIMITE", "")).strip(),
                    ],
                )
            ) or "—"

        computed.append(
            {
                "SEMÁFORO": signal,
                "HORAS EN ETAPA": round(elapsed, 2) if elapsed is not None else None,
                "PLAZO HORAS": maximum,
                "LÍMITE ETAPA": deadline,
                "SIGUIENTE ETAPA": next_normal_status(product, status, definitions),
                "DETALLE SEMÁFORO": detail,
            }
        )

    for column in COMPUTED_COLUMNS:
        cases[column] = [item[column] for item in computed]
    return cases


def grid_cell_value(
    column: str, value: Any, definitions: dict[str, ProcessDefinition]
) -> str:
    if column == STATUS_COLUMN:
        return canonical_status(value, configured_statuses(definitions))
    if column == PRODUCT_COLUMN:
        return strip_visual_prefix(value)
    return clean_cell(value).strip()


def display_workbench_df(
    frame: pd.DataFrame, definitions: dict[str, ProcessDefinition]
) -> pd.DataFrame:
    displayed = frame.copy()
    if STATUS_COLUMN in displayed:
        displayed[STATUS_COLUMN] = displayed[STATUS_COLUMN].map(
            lambda value: status_display_value(value, definitions)
        )
    if PRODUCT_COLUMN in displayed:
        displayed[PRODUCT_COLUMN] = displayed[PRODUCT_COLUMN].map(product_display_value)
    return displayed


def grid_changes(
    original: pd.DataFrame,
    edited: pd.DataFrame,
    definitions: dict[str, ProcessDefinition],
) -> list[tuple[str, dict[str, str]]]:
    if ID_COLUMN not in original or ID_COLUMN not in edited:
        raise ValueError(f"Falta la columna {ID_COLUMN}.")
    if original[ID_COLUMN].duplicated().any() or edited[ID_COLUMN].duplicated().any():
        raise ValueError("Hay números de orden duplicados; corrige la hoja antes de guardar.")
    if set(original[ID_COLUMN]) != set(edited[ID_COLUMN]) or len(original) != len(edited):
        raise ValueError("No se pueden agregar, borrar ni cambiar órdenes desde la tabla.")

    source = original.set_index(ID_COLUMN, drop=False)
    changes: list[tuple[str, dict[str, str]]] = []
    for _, row in edited.iterrows():
        identifier = clean_cell(row[ID_COLUMN]).strip()
        previous = source.loc[identifier]
        delta: dict[str, str] = {}
        for column in aligners_editable_columns(original.columns) & set(edited.columns):
            old_value = grid_cell_value(column, previous[column], definitions)
            new_value = grid_cell_value(column, row[column], definitions)
            if not values_equivalent(column, old_value, new_value):
                delta[column] = new_value
        if delta:
            changes.append((identifier, delta))
    return changes


def validate_delta(
    row: pd.Series,
    delta: dict[str, str],
    definitions: dict[str, ProcessDefinition],
) -> list[str]:
    errors: list[str] = []
    identifier = clean_cell(row.get(ID_COLUMN, "")).strip()
    invalid_columns = set(delta) - aligners_editable_columns(row.index)
    if invalid_columns:
        errors.append(f"{identifier}: no se puede editar {', '.join(sorted(invalid_columns))}.")
    for column in DATE_COLUMNS & set(delta):
        value = clean_cell(delta[column]).strip()
        if value and parse_simple_date(value) is None:
            errors.append(f"{identifier}: la fecha de {column} no es válida.")
    if STATUS_COLUMN in delta:
        previous = canonical_status(row.get(STATUS_COLUMN, ""), configured_statuses(definitions))
        product = row.get(PRODUCT_COLUMN, "")
        allowed = get_allowed_next_statuses(product, previous, definitions)
        new = canonical_status(delta[STATUS_COLUMN], configured_statuses(definitions))
        if status_key(new) not in {status_key(item) for item in allowed}:
            errors.append(f"{identifier}: {previous} → {new} no es una transición permitida.")
    return errors


def save_workbench_changes(
    source: pd.DataFrame,
    baseline: pd.DataFrame,
    edited: pd.DataFrame,
    current_user: str,
    definitions: dict[str, ProcessDefinition],
) -> tuple[list[str], list[str]]:
    try:
        changes = grid_changes(baseline, edited, definitions)
    except ValueError as exc:
        return [], [str(exc)]
    if not changes:
        return [], []

    source_by_id = source.set_index(ID_COLUMN, drop=False)
    saved: list[str] = []
    errors: list[str] = []
    for identifier, delta in changes:
        if identifier not in source_by_id.index:
            errors.append(f"{identifier}: el pedido ya no está en la vista actual.")
            break
        row = source_by_id.loc[identifier]
        validation_errors = validate_delta(row, delta, definitions)
        if validation_errors:
            errors.extend(validation_errors)
            break
        previous_status = canonical_status(
            row.get(STATUS_COLUMN, ""), configured_statuses(definitions)
        )
        expected = {
            column: row.get(column, "")
            for column in {STATUS_COLUMN, PRODUCT_COLUMN, *delta}
        }
        result = update_order_row(identifier, delta, expected_values=expected)
        if not result["success"]:
            errors.append(f"{identifier}: {result['error'] or 'no se pudo guardar'}")
            break
        saved.append(identifier)
        if STATUS_COLUMN in delta:
            try:
                register_status_change(
                    identifier=identifier,
                    product=clean_cell(row.get(PRODUCT_COLUMN, "")).strip(),
                    previous_status=previous_status,
                    new_status=delta[STATUS_COLUMN],
                    current_user=current_user,
                    definitions=definitions,
                    comment=delta.get("DETALLE COMENTARIOS", ""),
                )
            except Exception as exc:
                errors.append(
                    f"{identifier}: el status sí se guardó, pero la bitácora no pudo abrirse ({exc}). "
                    "Usa Reparar medición en el pedido."
                )
                break
    clear_sheet_data_cache()
    reset_workbench()
    return saved, errors


def initialize_order_timer(
    row: pd.Series,
    current_user: str,
    definitions: dict[str, ProcessDefinition],
) -> None:
    identifier = clean_cell(row.get(ID_COLUMN, "")).strip()
    status = canonical_status(row.get(STATUS_COLUMN, ""), configured_statuses(definitions))
    product = clean_cell(row.get(PRODUCT_COLUMN, "")).strip()
    register_status_change(
        identifier=identifier,
        product=product,
        previous_status=status,
        new_status=status,
        current_user=current_user,
        definitions=definitions,
        comment="Medición iniciada o reparada manualmente sin cambiar el status.",
    )


def grid_stage_options(
    row: pd.Series, definitions: dict[str, ProcessDefinition]
) -> list[str]:
    options = get_allowed_next_statuses(
        row.get(PRODUCT_COLUMN, ""), row.get(STATUS_COLUMN, ""), definitions
    )
    return [status_display_value(option, definitions) for option in options]


def build_grid_configuration(
    grid: pd.DataFrame,
    source: pd.DataFrame,
    definitions: dict[str, ProcessDefinition],
    *,
    hidden_columns: set[str] | None = None,
) -> dict:
    duplicates = set(source.loc[source[ID_COLUMN].duplicated(keep=False), ID_COLUMN])
    stages = {
        row[ID_COLUMN]: (
            grid_stage_options(row, definitions)
            if row[ID_COLUMN] not in duplicates
            else [status_display_value(row[STATUS_COLUMN], definitions)]
        )
        for _, row in source.iterrows()
    }
    selections: dict[str, list[str]] = {}
    for column in SELECT_COLUMNS & set(grid):
        values = [clean_cell(value).strip() for value in source[column]]
        selections[column] = list(dict.fromkeys(["", *[value for value in values if value]]))
    dates: dict[str, dict[str, str]] = {}
    for column in DATE_COLUMNS & set(grid):
        dates[column] = {
            value: parsed.isoformat()
            for value in set(grid[column])
            if (parsed := parse_simple_date(value)) is not None
        }

    statuses = list(
        dict.fromkeys(
            [
                *configured_statuses(definitions),
                *source.get(STATUS_COLUMN, pd.Series(dtype=str)).tolist(),
            ]
        )
    )
    products = list(source.get(PRODUCT_COLUMN, pd.Series(dtype=str)).unique())
    palettes = {
        STATUS_COLUMN: {
            status_display_value(status, definitions): status_palette_value(status)
            for status in statuses
            if clean_cell(status).strip()
        },
        PRODUCT_COLUMN: {
            product_display_value(product): product_palette_value(product)
            for product in products
            if clean_cell(product).strip()
        },
        "SEMÁFORO": ALERT_COLORS,
    }
    return build_aligners_grid_options(
        grid,
        editable=aligners_editable_columns(grid.columns),
        automatic=AUTOMATIC_COLUMNS,
        stage_options=stages,
        select_options=selections,
        date_values=dates,
        palettes=palettes,
        time_zone=APP_TIMEZONE_NAME,
        hidden_columns=hidden_columns,
    )


def reset_workbench() -> None:
    st.session_state.pop("aligners_snapshot", None)
    st.session_state.pop("aligners_editor_key", None)
    st.session_state.pop("aligners_editor_baseline", None)
    st.session_state["aligners_revision"] = st.session_state.get("aligners_revision", 0) + 1


def pending_change_count(definitions: dict[str, ProcessDefinition]) -> int:
    key = st.session_state.get("aligners_editor_key", "")
    state = st.session_state.get(key) or {}
    baseline = st.session_state.get("aligners_editor_baseline")
    if baseline is None or state.get("rows") is None:
        return 0
    try:
        return len(grid_changes(baseline, pd.DataFrame(state["rows"]), definitions))
    except ValueError:
        return 1


# ==============================
# Interfaz
# ==============================
def get_snapshot(current_user: str) -> dict[str, Any]:
    snapshot = st.session_state.get("aligners_snapshot")
    if snapshot is None or snapshot.get("user") != current_user:
        definitions = read_process_definitions()
        orders = read_orders_df()
        times = read_times_df()
        snapshot = {
            "user": current_user,
            "definitions": definitions,
            "orders": orders,
            "times": times,
            "table": build_tracking_table(orders, times, definitions),
            "at": app_now(),
        }
        st.session_state["aligners_snapshot"] = snapshot
    return snapshot


def render_metrics(table: pd.DataFrame, *, namespace: str = "") -> None:
    metrics = st.columns(6)
    labels = [
        ("align_total", "🦷 Pedidos activos", None),
        ("align_red", "🔴 Atrasados", "🔴 Atrasado"),
        ("align_amber", "🟡 Por vencer", "🟡 Por vencer"),
        ("align_pause", "🟣 En pausa", "🟣 En pausa"),
        ("align_green", "🟢 En tiempo", "🟢 En tiempo"),
        ("align_gray", "⚪ Sin medición", "⚪ Sin medición"),
    ]
    for container, (key, label, signal) in zip(metrics, labels):
        value = len(table) if signal is None else int(table["SEMÁFORO"].eq(signal).sum())
        namespaced_key = f"{key}_{namespace}" if namespace else key
        with container.container(key=namespaced_key):
            st.metric(label, value)


def filter_tracking_table(
    table: pd.DataFrame,
    *,
    search: str,
    signal: str,
    products: list[str],
    statuses: list[str],
    order_mode: str,
) -> pd.DataFrame:
    filtered = table.copy()
    if search:
        search_columns = [
            column
            for column in [ID_COLUMN, PRODUCT_COLUMN, "NOMBRE DOCTOR", "NOMBRE PACIENTE"]
            if column in filtered
        ]
        wanted = normalize_text(search)
        filtered = filtered[
            filtered[search_columns].apply(
                lambda row: wanted in normalize_text(" ".join(map(clean_cell, row))), axis=1
            )
        ]
    if signal != "Todos":
        filtered = filtered[filtered["SEMÁFORO"] == signal]
    if products:
        selected_keys = {product_key(item) for item in products}
        filtered = filtered[filtered[PRODUCT_COLUMN].map(product_key).isin(selected_keys)]
    if statuses:
        selected_keys = {status_key(item) for item in statuses}
        filtered = filtered[filtered[STATUS_COLUMN].map(status_key).isin(selected_keys)]
    if order_mode == "Atender urgentes primero":
        filtered = filtered.sort_values(
            "SEMÁFORO", key=lambda column: column.map(signal_rank), kind="stable"
        )
    return filtered.reset_index(drop=True)


def render_case_actions(
    selected: pd.DataFrame,
    times_df: pd.DataFrame,
    current_user: str,
    definitions: dict[str, ProcessDefinition],
    pending: bool,
) -> None:
    if selected.empty:
        st.caption("Marca la casilla de un pedido para ver sus datos, historial y acciones.")
        return
    if pending:
        st.info("Guarda o descarta los cambios de la tabla antes de abrir acciones del pedido.")
        return
    if len(selected) != 1:
        st.caption("Selecciona un solo pedido para abrir su detalle e historial.")
        return

    row = selected.iloc[0]
    identifier = clean_cell(row.get(ID_COLUMN, "")).strip()
    status = clean_cell(row.get(STATUS_COLUMN, "")).strip()
    product = clean_cell(row.get(PRODUCT_COLUMN, "")).strip()
    background, foreground = status_palette_value(status)
    with st.container(border=True):
        st.markdown(f"### Pedido {html.escape(identifier)} · {html.escape(product)}")
        st.markdown(
            f'<span class="align-stage-chip" style="background:{background};color:{foreground}">'
            f'{html.escape(status_display_value(status, definitions))}</span>',
            unsafe_allow_html=True,
        )
        st.caption(
            f"{clean_cell(row.get('NOMBRE DOCTOR', '')).strip()} · "
            f"{clean_cell(row.get('NOMBRE PACIENTE', '')).strip()}"
        )
        st.write(f"{row.get('SEMÁFORO', '')} — {row.get('DETALLE SEMÁFORO', '')}")
        if is_pause_status(status):
            st.warning(
                f"El pedido está en pausa por **{status}**. Al resolverla podrás elegir la "
                "etapa real a la que debe regresar."
            )
        targets = [
            item
            for item in get_allowed_next_statuses(product, status, definitions)
            if status_key(item) != status_key(status)
        ]
        if targets:
            st.caption(
                "Opciones permitidas: "
                + " · ".join(status_display_value(item, definitions) for item in targets)
            )

        detail_text = clean_cell(row.get("DETALLE SEMÁFORO", ""))
        repairable = any(
            phrase in detail_text.lower()
            for phrase in [
                "sin registro de inicio",
                "no corresponde",
                "varios registros",
                "fecha de inicio",
                "inicia la medición",
            ]
        )
        if repairable and get_process_definition(product, definitions) is not None:
            if st.button(
                "⏱️ Iniciar / reparar medición de esta etapa",
                key=f"repair_aligner_{identifier}",
                help="No cambia el status; cierra mediciones incoherentes y comienza a contar desde ahora.",
            ):
                try:
                    initialize_order_timer(row, current_user, definitions)
                    reset_workbench()
                    st.session_state["aligners_feedback"] = (
                        [identifier],
                        [],
                        "Medición iniciada en la etapa actual.",
                    )
                    st.rerun()
                except Exception as exc:
                    st.error(f"No se pudo iniciar la medición: {exc}")

        with st.expander("🕘 Historial del pedido"):
            history = canonical_times_df(times_df)
            if ID_COLUMN in history:
                history = history[
                    history[ID_COLUMN].astype(str).str.strip().eq(identifier)
                ]
            history_columns = [
                column
                for column in [
                    STATUS_COLUMN,
                    "ES_PAUSA",
                    "USUARIO",
                    "FECHA_INICIO",
                    "HORA_INICIO",
                    "FECHA_FIN",
                    "HORA_FIN",
                    "DURACION_HORAS",
                    "COMENTARIOS_CAMBIO",
                ]
                if column in history
            ]
            if history.empty:
                st.info("Este pedido todavía no tiene historial en TIEMPOS_ALINEADORES.")
            else:
                st.dataframe(
                    history[history_columns].iloc[::-1],
                    hide_index=True,
                    use_container_width=True,
                )

        with st.expander("📋 Todos los datos de la hoja"):
            excluded = {*COMPUTED_COLUMNS, "_SHEET_ROW"}
            details = [
                {"Campo": column, "Valor": clean_cell(value)}
                for column, value in row.items()
                if column not in excluded and clean_cell(value).strip()
            ]
            st.dataframe(pd.DataFrame(details), hide_index=True, use_container_width=True)


@st.fragment
def render_workbench(current_user: str) -> None:
    snapshot = get_snapshot(current_user)
    definitions: dict[str, ProcessDefinition] = snapshot["definitions"]
    table: pd.DataFrame = snapshot["table"]
    stored_pending = pending_change_count(definitions)
    pending_before_grid = stored_pending > 0

    heading, refresh = st.columns([5, 1])
    with heading:
        st.caption(
            f"{current_user} · Datos consultados {snapshot['at']:%d/%m/%Y %H:%M} · "
            "Hora de Ciudad de México"
        )
    with refresh:
        if st.button(
            "Actualizar datos",
            disabled=pending_before_grid,
            use_container_width=True,
            key="aligners_refresh",
        ):
            clear_sheet_data_cache()
            reset_workbench()
            st.rerun()

    feedback = st.session_state.pop("aligners_feedback", None)
    if feedback:
        saved, errors, message = feedback
        if saved:
            st.toast(message or "Guardado: " + ", ".join(saved), icon="✅")
        for error in errors:
            st.error(error)

    render_metrics(table)
    st.caption(
        "Verde: menos del 80% del plazo · Amarillo: 80–99% · Rojo: plazo agotado · "
        "Morado: pausa opcional · Gris: falta iniciar o reparar la medición. "
        "Los tiempos cuentan de lunes a viernes."
    )
    if table.empty:
        st.info("No hay pedidos activos. Los enviados y cancelados permanecen archivados en la hoja.")
        return

    bar_text = (
        f"✏️ {stored_pending} pedidos con cambios · Guarda o descarta antes de filtrar."
        if pending_before_grid
        else "✨ Listo para trabajar · Edita una celda habilitada y después guarda."
    )
    st.markdown(
        f'<div class="align-edit-bar {"is-pending" if pending_before_grid else "is-ready"}">{bar_text}</div>',
        unsafe_allow_html=True,
    )

    filters = st.columns([2.2, 1.1, 1.3, 1.4, 1.3])
    search = filters[0].text_input(
        "Buscar pedido",
        placeholder="Orden, doctor, paciente o producto",
        disabled=pending_before_grid,
        key="aligners_search",
    )
    signal = filters[1].selectbox(
        "Semáforo",
        ["Todos", *ALERT_COLORS],
        disabled=pending_before_grid,
        key="aligners_signal",
    )
    product_options = sorted(
        {clean_cell(value).strip() for value in table.get(PRODUCT_COLUMN, []) if clean_cell(value).strip()}
    )
    products = filters[2].multiselect(
        "Producto",
        product_options,
        disabled=pending_before_grid,
        key="aligners_products",
    )
    status_options = sorted(
        {clean_cell(value).strip() for value in table.get(STATUS_COLUMN, []) if clean_cell(value).strip()}
    )
    statuses = filters[3].multiselect(
        "Status",
        status_options,
        disabled=pending_before_grid,
        format_func=lambda value: status_display_value(value, definitions),
        key="aligners_statuses",
    )
    order_mode = filters[4].selectbox(
        "Orden",
        ["Orden de la hoja", "Atender urgentes primero"],
        disabled=pending_before_grid,
        key="aligners_order_mode",
    )
    filtered = filter_tracking_table(
        table,
        search=search,
        signal=signal,
        products=products,
        statuses=statuses,
        order_mode=order_mode,
    )

    visibility = st.columns([1.7, 6.7])
    hide_automatic = visibility[0].checkbox(
        "Ocultar automáticas",
        disabled=pending_before_grid,
        key="aligners_hide_automatic",
    )
    visibility[1].markdown(
        '<div class="align-column-legend"><span class="readonly">🔒 Fijas: No. Orden y Semáforo</span>'
        '<span class="editable">✎ Editable manual</span>'
        '<span class="auto-editable">✎⚙ Editable automática</span>'
        '<span class="automatic">⚙ Cálculo automático</span></div>',
        unsafe_allow_html=True,
    )
    st.caption(
        f"{len(filtered)} de {len(table)} pedidos · Cada fila sólo ofrece su siguiente etapa normal, "
        "las pausas aplicables y Cancelado. En una pausa se habilitan las etapas para reanudar."
    )
    if filtered.empty:
        st.info("No hay pedidos que coincidan con los filtros.")
        return

    visible_columns = [column for column in GRID_COLUMNS if column in filtered]
    source_grid = filtered[visible_columns].copy()
    grid = display_workbench_df(source_grid, definitions)
    grid.insert(0, "SELECCIONAR", False)
    signature = hashlib.sha256(
        json.dumps(
            [
                current_user,
                st.session_state.get("aligners_revision", 0),
                search,
                signal,
                products,
                statuses,
                order_mode,
                hide_automatic,
            ],
            ensure_ascii=False,
            sort_keys=True,
        ).encode()
    ).hexdigest()[:16]
    key = f"aligners_grid_{signature}"
    st.session_state["aligners_editor_key"] = key
    st.session_state["aligners_editor_baseline"] = grid.copy()

    hidden: set[str] = set()
    if hide_automatic:
        hidden |= AUTOMATIC_COLUMNS
    options = build_grid_configuration(
        grid, source_grid, definitions, hidden_columns=hidden
    )
    edited = render_aligners_grid(grid, options, key)
    try:
        changes = grid_changes(grid, edited, definitions)
    except ValueError as exc:
        st.error(str(exc))
        changes = []
    pending = bool(changes)

    save_column, discard_column, count_column, _ = st.columns([1.3, 1.3, 1.8, 4])
    if save_column.button(
        "Guardar cambios",
        type="primary",
        disabled=not changes,
        use_container_width=True,
        key="aligners_save",
    ):
        saved, errors = save_workbench_changes(
            filtered, grid, edited, current_user, definitions
        )
        st.session_state["aligners_feedback"] = (saved, errors, "")
        st.rerun()
    if discard_column.button(
        "Descartar cambios",
        disabled=not pending_before_grid and not pending,
        use_container_width=True,
        key="aligners_discard",
    ):
        reset_workbench()
        st.rerun()
    count_column.caption(f"{len(changes)} pedidos con cambios pendientes")

    selected_ids = edited.loc[edited["SELECCIONAR"].eq(True), ID_COLUMN]
    selected = filtered[filtered[ID_COLUMN].isin(selected_ids)]
    render_case_actions(
        selected,
        snapshot["times"],
        current_user,
        definitions,
        pending,
    )


def render_alerts(current_user: str) -> None:
    snapshot = get_snapshot(current_user)
    table: pd.DataFrame = snapshot["table"].copy()
    render_metrics(table, namespace="alerts")
    st.markdown("### Alertas que requieren atención")
    attention = table[
        table["SEMÁFORO"].isin(
            ["🔴 Atrasado", "🟡 Por vencer", "🟣 En pausa", "⚪ Sin medición"]
        )
    ].copy()
    attention = attention.sort_values(
        "SEMÁFORO", key=lambda column: column.map(signal_rank), kind="stable"
    )
    columns = [
        column
        for column in [
            "SEMÁFORO",
            ID_COLUMN,
            PRODUCT_COLUMN,
            STATUS_COLUMN,
            "NOMBRE DOCTOR",
            "NOMBRE PACIENTE",
            "HORAS EN ETAPA",
            "LÍMITE ETAPA",
            "DETALLE SEMÁFORO",
        ]
        if column in attention
    ]
    if attention.empty:
        st.success("No hay alertas pendientes.")
    else:
        st.dataframe(attention[columns], hide_index=True, use_container_width=True)
    st.caption(
        "Los pedidos grises existentes necesitan que abras el pedido en Seguimiento y pulses "
        "Iniciar / reparar medición. Desde ese momento la app contará el plazo de la etapa actual."
    )


def render_processes(current_user: str) -> None:
    definitions = get_snapshot(current_user)["definitions"]
    st.caption(
        f"Lectura directa de {SHEET_PROCESSES}. Si la hoja cambia, pulsa Actualizar datos en Seguimiento."
    )
    if not definitions:
        st.warning("No pude construir procesos desde la hoja.")
        return
    for definition in definitions.values():
        with st.expander(f"{product_display_value(definition.name)}", expanded=True):
            normal = pd.DataFrame(
                [
                    {"Paso": index, "Etapa normal": status, "Tiempo": limit or "Sin plazo"}
                    for index, (status, limit) in enumerate(definition.normal_steps, start=1)
                ]
            )
            st.dataframe(normal, hide_index=True, use_container_width=True)
            if definition.pauses:
                st.markdown(
                    "**Pausas opcionales:** "
                    + " · ".join(f"⏸️ {pause}" for pause in definition.pauses)
                )
            st.caption("🗄️ CANCELADO está disponible desde cualquier etapa activa y archiva el pedido.")


def apply_custom_css() -> None:
    st.markdown(
        """<style>
        .stApp {
            background: radial-gradient(ellipse at 0 0, #DED1FF 0, transparent 48%),
                        radial-gradient(ellipse at 100% 30%, #CDEDEA 0, transparent 45%), #F1EEFA;
        }
        .block-container {padding-top: 3rem; padding-bottom: 2rem; max-width: 100%;}
        [data-testid="stHeader"] {background: transparent;}
        [data-testid="stSidebar"] {background: #EBE4FA; border-right: 1px solid #CEC0E8;}
        h1, h2, h3 {letter-spacing: -.025em; color: #392365;}
        .align-hero {
            position: relative; overflow: hidden; padding: 24px 30px; margin: 0 0 20px;
            display: flex; justify-content: space-between; align-items: center; gap: 20px;
            color: #FFF; background: linear-gradient(115deg, #342059 0%, #633CB0 54%, #137C87 100%);
            border: 1px solid #9A80CD; border-radius: 20px; box-shadow: 0 12px 30px #39236522;
        }
        .align-hero h1 {color: #FFF; font-size: 2rem; margin: 6px 0; padding: 0;}
        .align-hero p {color: #E9E1FF; margin: 0; font-size: .93rem;}
        .align-brand {font-size: .72rem; font-weight: 800; letter-spacing: .17em; color: #D5C5FA;}
        .align-hero-badge {
            background: #FFFFFF18; border: 1px solid #FFFFFF42; border-radius: 14px;
            padding: 12px 18px; font-size: .84rem; color: #FFF; white-space: nowrap;
        }
        [data-baseweb="tab-list"] {
            gap: 8px; padding: 7px; border-radius: 15px; background: #E2D8F3;
            border: 1px solid #CBBDE3; margin-bottom: 12px;
        }
        [data-baseweb="tab"] {
            height: 45px; padding: 0 20px; border-radius: 10px; background: #F6F2FF;
            border: 1px solid #D5C7EC; color: #493168; font-weight: 700;
        }
        [data-baseweb="tab"][aria-selected="true"] {
            background: linear-gradient(110deg, #7040BD, #4E3992); color: #FFF;
            border-color: #6337A6; box-shadow: 0 4px 12px #6943A833;
        }
        [data-baseweb="tab"][aria-selected="true"] p {color: #FFF;}
        [data-baseweb="tab-highlight"], [data-baseweb="tab-border"] {display: none;}
        [data-testid="stMetric"] {
            border: 1px solid #CDC1E6; border-top-width: 4px; border-radius: 14px;
            padding: 13px 16px; box-shadow: 0 5px 16px #42266C0D;
        }
        .st-key-align_total [data-testid="stMetric"], .st-key-align_total_alerts [data-testid="stMetric"] {background:#EEE7FF;border-color:#8C62D2;color:#4C2883;}
        .st-key-align_red [data-testid="stMetric"], .st-key-align_red_alerts [data-testid="stMetric"] {background:#FFECEF;border-color:#DF5875;color:#A72B48;}
        .st-key-align_amber [data-testid="stMetric"], .st-key-align_amber_alerts [data-testid="stMetric"] {background:#FFF3D7;border-color:#DBA131;color:#885A08;}
        .st-key-align_pause [data-testid="stMetric"], .st-key-align_pause_alerts [data-testid="stMetric"] {background:#F0E7FF;border-color:#8D62CC;color:#553C9A;}
        .st-key-align_green [data-testid="stMetric"], .st-key-align_green_alerts [data-testid="stMetric"] {background:#E0F7EE;border-color:#37A989;color:#13654E;}
        .st-key-align_gray [data-testid="stMetric"], .st-key-align_gray_alerts [data-testid="stMetric"] {background:#EBEFF8;border-color:#8193B4;color:#4C5E7D;}
        .align-edit-bar {
            height: 40px; display:flex; align-items:center; padding:0 14px; border-radius:10px;
            border:1px solid #CDBCEB; font-size:.85rem; font-weight:600; margin: 8px 0;
        }
        .align-edit-bar.is-ready {background:#E7DFF7;color:#53357F;}
        .align-edit-bar.is-pending {background:#FCE9BA;border-color:#DCA544;color:#805410;}
        .align-column-legend {display:flex;justify-content:flex-end;gap:8px;align-items:end;min-height:38px;}
        .align-column-legend span {border-radius:999px;padding:5px 10px;color:#FFF;font-size:.76rem;font-weight:700;}
        .align-column-legend .editable {background:#7443AA;}
        .align-column-legend .readonly {background:#52667D;}
        .align-column-legend .automatic {background:#147C84;}
        .align-column-legend .auto-editable {background:linear-gradient(100deg,#147C84 0 48%,#7443AA 52% 100%);}
        .align-stage-chip {display:inline-block;border-radius:9px;padding:8px 13px;font-size:.85rem;font-weight:750;margin-bottom:10px;}
        [data-testid="stWidgetLabel"] p {color:#493064;font-weight:650;}
        button[kind="primary"], [data-testid="stBaseButton-primary"] {
            background:linear-gradient(110deg,#7A40BE,#5942A1);border-color:#68419D;color:#FFF;
            box-shadow:0 4px 12px #6637A82B;
        }
        button[kind="primary"]:not(:disabled):hover,
        [data-testid="stBaseButton-primary"]:not(:disabled):hover {
            box-shadow:0 5px 16px #6637A84D;
        }
        button[kind="primary"]:disabled,
        [data-testid="stBaseButton-primary"]:disabled {
            background:#DDD2EC;border-color:#B6A1CF;color:#58436F;opacity:1;
            box-shadow:none;cursor:not-allowed;
        }
        button[kind="primary"]:disabled *,
        [data-testid="stBaseButton-primary"]:disabled * {
            color:#58436F !important;-webkit-text-fill-color:#58436F;opacity:1;
        }
        @media (max-width: 800px) {
            .align-hero {padding:20px;}.align-hero h1 {font-size:1.55rem;}.align-hero-badge {display:none;}
        }
        </style>""",
        unsafe_allow_html=True,
    )


def main() -> None:
    st.set_page_config(page_title="Control de Alineadores – ARTTDLAB", layout="wide")
    apply_custom_css()
    st.markdown(
        """<section class="align-hero">
        <div><div class="align-brand">ARTTDLAB / LABORATORIO</div>
        <h1>Control de alineadores</h1>
        <p>Flujos por producto, pausas visibles y tiempos bajo control.</p></div>
        <div class="align-hero-badge">🦷 ALINEADORES (nuevo)</div>
        </section>""",
        unsafe_allow_html=True,
    )
    try:
        current_user = require_authenticated_user()
        if current_user is None:
            return
        ensure_times_headers()
        tabs = st.tabs(["📋 Seguimiento", "🚨 Alertas y pausas", "⚙️ Procesos y plazos"])
        with tabs[0]:
            render_workbench(current_user)
        with tabs[1]:
            render_alerts(current_user)
        with tabs[2]:
            render_processes(current_user)
    except Exception as exc:
        if is_google_sheets_rate_limit_error(exc):
            st.error("Google Sheets alcanzó el límite temporal de lecturas. Espera un minuto y actualiza.")
        elif isinstance(exc, gspread.exceptions.SpreadsheetNotFound):
            st.error(
                "La cuenta de servicio todavía no tiene acceso a Control ALINEADORES. "
                "Comparte el archivo con el client_email de google_credentials."
            )
        else:
            st.error("Ocurrió un problema al cargar la app de alineadores.")
            st.exception(exc)


if __name__ == "__main__":
    main()
