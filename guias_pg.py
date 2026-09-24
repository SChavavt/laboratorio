"""Solicitudes de guía de ventas dentro del control de laboratorio.

Replica la sesión reducida de ARTTD JIMENA en app_v: "📋 Solicitud Guía"
registra el mismo renglón en ``data_pedidos`` y "📦 Guías Cargadas" muestra las
guías que almacén ya subió. Se escribe con el mismo vendedor, ID y columnas que
app_v para que ventas, almacén y esta vista lean exactamente las mismas
solicitudes.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
import uuid
from datetime import date, datetime, timedelta
from typing import Any, Callable, Mapping, Sequence
from urllib.parse import quote, unquote, urlsplit, urlunsplit
from zoneinfo import ZoneInfo

import gspread
import pandas as pd
import streamlit as st
from google.oauth2.service_account import Credentials
from streamlit.errors import StreamlitAPIException

SCOPE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
# Mismo archivo que abre app_v (GOOGLE_SHEET_ID); se puede cambiar en secrets.
DEFAULT_SPREADSHEET_ID = "1aWkSelodaz0nWfQx7FZAysGnIYGQFJxAN7RO3YgCiZY"
SHEET_PEDIDOS_OPERATIVOS = "data_pedidos"
SHEET_PEDIDOS_HISTORICOS = "datos_pedidos"
SHEET_CASOS_ESPECIALES = "casos_especiales"

GUIDE_USER_ID = "ARTTDJIM01"
GUIDE_VENDOR_NAME = "ARTTD JIMENA"
SCHAVA_VENDOR_NAME = "SCHAVA"
GUIDE_VENDORS = (SCHAVA_VENDOR_NAME, GUIDE_VENDOR_NAME)
TIPO_ENVIO_GUIA = "📋 Solicitudes de Guía"
ESTADO_INICIAL = "🟡 Pendiente"

APP_TIMEZONE = ZoneInfo("America/Mexico_City")
GUIDES_HISTORY_DAYS = 30
GUIDES_NOTICE_HOURS = 12
GUIDES_REFRESH_COOLDOWN_SECONDS = 15

GUIDES_TAB_REQUEST = "📋 Solicitud Guía"
GUIDES_TAB_LOADED = "📦 Guías Cargadas"
GUIDES_TAB_LABELS = [GUIDES_TAB_REQUEST, GUIDES_TAB_LOADED]
GUIDES_REFRESH_TOKEN_KEY = "guides_refresh_token"

# app_v agrega estas columnas a data_pedidos si faltan antes de registrar.
REQUIRED_ORDER_HEADERS = [
    "TD_Leal_Etapa",
    "Tipo_Venta",
    "Condicion_Venta_Terceros",
    "Anticipo_Credito",
    "Plazo_Credito_Meses",
    "Frecuencia_Pago_Credito",
    "Dia_Cobro_Credito",
    "Datos_Contacto_Credito",
    "Direccion_Guia_Retorno",
]
DHL_REQUIRED_FIELDS = (
    ("Nombre", "nombre"),
    ("Calle y número", "calle_numero"),
    ("Colonia", "colonia"),
    ("Código Postal", "codigo_postal"),
    ("Ciudad", "ciudad"),
    ("Estado", "estado"),
    ("País", "pais"),
    ("Teléfono", "telefono"),
)
DHL_OPTIONAL_FIELDS = (
    ("Interior / Depto", "interior"),
    ("Municipio / Alcaldía", "municipio"),
    ("Correo electrónico", "correo"),
    ("Referencias de dirección", "referencias"),
)

SOURCE_COLUMNS = [
    "ID_Pedido", "Cliente", "Vendedor_Registro", "Tipo_Envio", "Estado",
    "Fecha_Entrega", "Hora_Registro", "Folio_Factura", "id_vendedor",
    "Completados_Limpiado", "Adjuntos_Guia", "Hoja_Ruta_Mensajero", "Tipo_Caso",
]
GUIDE_COLUMNS = [
    "ID_Pedido", "Cliente", "Vendedor_Registro", "Tipo_Envio", "Estado",
    "Fecha_Entrega", "Hora_Registro", "Folio_Factura", "Adjuntos_Guia",
    "URLs_Guia", "Ultima_Guia", "Fuente", "id_vendedor", "Completados_Limpiado",
    "Hora_Registro_dt", "Fecha_Entrega_dt", "Fecha_Filtro_Referencia", "Folio_O_ID",
]
TABLE_COLUMNS = [
    "Folio_Factura", "Cliente", "Vendedor_Registro", "Tipo_Envio", "Estado",
    "Fecha_Entrega", "Fuente",
]
OFFICE_PREVIEW_EXTENSIONS = {".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx"}
INLINE_CONTENT_TYPES = {
    ".pdf": "application/pdf",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".gif": "image/gif",
    ".webp": "image/webp",
}


# ==============================
# Utilidades
# ==============================
def app_now() -> datetime:
    """Hora de Ciudad de México sin tzinfo, igual que Hora_Registro en Sheets."""

    return datetime.now(APP_TIMEZONE).replace(tzinfo=None)


def app_today() -> date:
    return app_now().date()


def clean_cell(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none", "nat"} else text


def normalize_vendor_id(value: Any) -> str:
    return str(value or "").strip().upper()


def rerun_active_tab() -> None:
    """Recalcula la pestaña activa; fuera de un fragmento recarga la app completa."""
    try:
        st.rerun(scope="fragment")
    except StreamlitAPIException:
        st.rerun()


def is_google_sheets_rate_limit_error(exc: Exception) -> bool:
    if not isinstance(exc, gspread.exceptions.APIError):
        return False
    status_code = getattr(getattr(exc, "response", None), "status_code", None)
    error_text = str(exc).upper()
    return status_code == 429 or "RATE_LIMIT_EXCEEDED" in error_text or "RESOURCE_EXHAUSTED" in error_text


def run_gsheets_request(operation: Callable[[], Any], *, retries: int = 3, base_delay: float = 1.0) -> Any:
    for attempt in range(retries + 1):
        try:
            return operation()
        except Exception as exc:
            if not is_google_sheets_rate_limit_error(exc) or attempt >= retries:
                raise
            time.sleep(base_delay * (2 ** attempt))
    raise RuntimeError("No se pudo completar la operación de Google Sheets.")


# ==============================
# Configuración y conexión
# ==============================
def _secret_section(source: Any, name: str) -> Mapping[str, Any]:
    section = source.get(name, {}) if hasattr(source, "get") else {}
    return section if hasattr(section, "get") else {}


def _secret_source(secret_source: Any | None) -> Any:
    return st.secrets if secret_source is None and hasattr(st, "secrets") else (secret_source or {})


def configured_spreadsheet_id(secret_source: Any | None = None) -> str:
    """Usa una clave propia para no tocar los sheet_id de aparatos ni alineadores."""

    source = _secret_source(secret_source)
    gsheets = _secret_section(source, "gsheets")
    for key in ("ventas_sheet_id", "guias_sheet_id"):
        value = clean_cell(gsheets.get(key, ""))
        if value:
            return value
    value = clean_cell(source.get("ventas_sheet_id", "") if hasattr(source, "get") else "")
    return value or DEFAULT_SPREADSHEET_ID


def configured_credentials_info(secret_source: Any | None = None) -> dict[str, Any]:
    """Cuenta de servicio del laboratorio, o una propia de ventas si se configura."""

    gsheets = _secret_section(_secret_source(secret_source), "gsheets")
    raw = gsheets.get("ventas_google_credentials") or gsheets["google_credentials"]
    info = dict(raw) if hasattr(raw, "keys") else json.loads(str(raw))
    if "private_key" in info:
        info["private_key"] = str(info["private_key"]).replace("\\n", "\n").strip()
    return info


def _get_gs_client():
    credentials = Credentials.from_service_account_info(configured_credentials_info(), scopes=SCOPE)
    return gspread.authorize(credentials)


@st.cache_resource
def get_spreadsheet():
    client = _get_gs_client()
    return run_gsheets_request(lambda: client.open_by_key(configured_spreadsheet_id()))


@st.cache_resource
def get_worksheet(sheet_name: str):
    spreadsheet = get_spreadsheet()
    return run_gsheets_request(lambda: spreadsheet.worksheet(sheet_name))


# ==============================
# Registro de solicitudes
# ==============================
def build_dhl_mexico_address_payload(address: Mapping[str, Any]) -> str:
    """Serializa la dirección como app_v: una línea "Etiqueta: valor" por campo."""

    rows = []
    for label, key in (*DHL_REQUIRED_FIELDS, *DHL_OPTIONAL_FIELDS):
        value = str(address.get(key, "") or "").strip()
        if value:
            rows.append(f"{label}: {value}")
    return "\n".join(rows)


def get_missing_dhl_mexico_required_fields(address: Mapping[str, Any]) -> list[str]:
    return [label for label, key in DHL_REQUIRED_FIELDS if not str(address.get(key, "") or "").strip()]


def build_submission_identity(now: datetime | None = None) -> tuple[str, str]:
    """ID_Pedido y Hora_Registro con el mismo formato que app_v."""

    now = now or app_now()
    pedido_id = f"PED-{now:%Y%m%d%H%M%S}-{uuid.uuid4().hex[:8].upper()}"
    return pedido_id, now.strftime("%Y-%m-%d %H:%M:%S")


def build_guide_request_values(
    headers: Sequence[str],
    *,
    pedido_id: str,
    hora_registro: str,
    cliente: str,
    folio_factura: str,
    comentario: str,
    direccion_guia: str,
    fecha_entrega: date,
) -> list[str]:
    """Arma el renglón exactamente como lo guarda app_v para ARTTD JIMENA.

    Las columnas de pago, crédito y casos especiales quedan vacías porque una
    solicitud de guía no es una venta.
    """

    values_by_header = {
        "ID_Pedido": pedido_id,
        "Hora_Registro": hora_registro,
        "Vendedor": GUIDE_VENDOR_NAME,
        "Vendedor_Registro": GUIDE_VENDOR_NAME,
        "Cliente": cliente,
        "RegistroCliente": cliente,
        "Folio_Factura": folio_factura,
        "Tipo_Venta": "Venta TD",
        "TD_Leal_Etapa": "NO APLICA",
        "Tipo_Envio": TIPO_ENVIO_GUIA,
        "Fecha_Entrega": fecha_entrega.strftime("%Y-%m-%d"),
        "Comentario": comentario,
        "Estado": ESTADO_INICIAL,
        "Aplica_Pago": "No",
        "Direccion_Guia_Retorno": direccion_guia,
    }
    return [
        GUIDE_USER_ID if str(header).lower() == "id_vendedor" else values_by_header.get(header, "")
        for header in headers
    ]


def ensure_order_headers(worksheet) -> list[str]:
    headers = run_gsheets_request(lambda: worksheet.row_values(1))
    if not headers:
        raise ValueError(f"La hoja {SHEET_PEDIDOS_OPERATIVOS} está vacía.")
    missing = [column for column in REQUIRED_ORDER_HEADERS if column not in headers]
    if missing:
        run_gsheets_request(lambda: worksheet.update("A1", [headers + missing]))
        headers = run_gsheets_request(lambda: worksheet.row_values(1))
    return headers


def _column_letters(number: int) -> str:
    letters = ""
    while number > 0:
        number, remainder = divmod(number - 1, 26)
        letters = chr(ord("A") + remainder) + letters
    return letters


def appended_range_start(response: Any) -> tuple[int, int] | None:
    """(renglón, columna) donde Sheets escribió el append, p. ej. AF53 → (53, 32)."""

    updates = response.get("updates") if isinstance(response, Mapping) else None
    if not isinstance(updates, Mapping):
        return None
    cell_range = str(updates.get("updatedRange") or "").rsplit("!", 1)[-1]
    match = re.match(r"\$?([A-Za-z]+)\$?(\d+)", cell_range)
    if not match:
        return None
    column = 0
    for char in match.group(1).upper():
        column = column * 26 + (ord(char) - ord("A") + 1)
    return int(match.group(2)), column


def realign_appended_row(worksheet, response: Any, values: Sequence[Any]) -> None:
    """Regresa a la columna A un renglón que Sheets anexó recorrido (igual que app_v)."""

    start = appended_range_start(response)
    if not start or start[1] == 1:
        return
    row, column = start
    last_column = column + len(values) - 1
    misplaced = f"{_column_letters(column)}{row}:{_column_letters(last_column)}{row}"
    left_cells = worksheet.get(f"A{row}:{_column_letters(column - 1)}{row}")
    if any(str(cell).strip() for left_row in left_cells for cell in left_row):
        worksheet.batch_clear([misplaced])
        raise ValueError(
            f"Google Sheets anexó el pedido recorrido en {misplaced}; "
            "se borró para no mezclarlo con otro renglón."
        )
    padded = list(values) + [""] * (last_column - len(values))
    worksheet.update(
        f"A{row}:{_column_letters(last_column)}{row}", [padded], value_input_option="USER_ENTERED"
    )


def append_row_with_confirmation(worksheet, values: Sequence[Any], pedido_id: str, id_col_index: int,
                                 retries: int = 5, base_delay: float = 1.0) -> None:
    """Agrega el renglón una sola vez: si el ID ya existe no lo vuelve a escribir."""

    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            if pedido_id in worksheet.col_values(id_col_index + 1):
                return
            # "A1" ancla el append a la tabla que inicia en la columna A.
            response = worksheet.append_row(list(values), value_input_option="USER_ENTERED", table_range="A1")
            realign_appended_row(worksheet, response, values)
            if pedido_id in worksheet.col_values(id_col_index + 1):
                return
            raise RuntimeError("La escritura no se confirmó")
        except Exception as exc:
            last_error = exc
            time.sleep(base_delay * (attempt + 1))
    raise RuntimeError(f"No se pudo confirmar la escritura en Google Sheets: {last_error}")


def request_fingerprint(request: Mapping[str, Any]) -> str:
    return hashlib.sha256(json.dumps(request, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def submission_identity_for(request: Mapping[str, Any]) -> tuple[str, str]:
    """Reutiliza el ID si se reenvía la misma solicitud tras un error de conexión.

    Así un reintento no duplica el renglón cuando la primera escritura sí llegó.
    """

    fingerprint = request_fingerprint(request)
    pending = st.session_state.get("guides_pending_identity") or {}
    if pending.get("fingerprint") == fingerprint:
        return pending["pedido_id"], pending["hora_registro"]
    pedido_id, hora_registro = build_submission_identity()
    st.session_state["guides_pending_identity"] = {
        "fingerprint": fingerprint, "pedido_id": pedido_id, "hora_registro": hora_registro,
    }
    return pedido_id, hora_registro


def register_guide_request(request: Mapping[str, Any]) -> str:
    """Escribe la solicitud en data_pedidos y devuelve el cliente registrado."""

    address = request["direccion"]
    cliente = str(address.get("nombre", "") or "").strip()
    pedido_id, hora_registro = submission_identity_for(request)
    worksheet = get_worksheet(SHEET_PEDIDOS_OPERATIVOS)
    headers = ensure_order_headers(worksheet)
    if "ID_Pedido" not in headers:
        raise ValueError(f"No se encontró la columna ID_Pedido en {SHEET_PEDIDOS_OPERATIVOS}.")
    values = build_guide_request_values(
        headers,
        pedido_id=pedido_id,
        hora_registro=hora_registro,
        cliente=cliente,
        folio_factura=str(request.get("folio_factura", "") or "").strip(),
        comentario=str(request.get("comentario", "") or "").strip(),
        direccion_guia=build_dhl_mexico_address_payload(address),
        fecha_entrega=request["fecha_entrega"],
    )
    append_row_with_confirmation(worksheet, values, pedido_id, headers.index("ID_Pedido"))
    st.session_state.pop("guides_pending_identity", None)
    return cliente or pedido_id


# ==============================
# Guías cargadas
# ==============================
def values_to_dataframe(values: Sequence[Sequence[Any]]) -> pd.DataFrame:
    """get_all_values → DataFrame, tolerando encabezados vacíos o repetidos."""

    if not values:
        return pd.DataFrame()
    headers: list[str] = []
    seen: dict[str, int] = {}
    for index, raw in enumerate(values[0], start=1):
        base = str(raw).strip() or f"col_{index}"
        count = seen.get(base, 0)
        seen[base] = count + 1
        headers.append(base if count == 0 else f"{base}_{count + 1}")
    width = len(headers)
    rows = [list(row[:width]) + [""] * (width - len(row[:width])) for row in values[1:]]
    return pd.DataFrame(rows, columns=headers)


@st.cache_data(ttl=60, show_spinner=False)
def read_guides_sources(refresh_token: float | None = None) -> dict[str, pd.DataFrame]:
    """Lee las tres hojas donde almacén carga guías, sólo con las columnas útiles."""

    _ = refresh_token
    sources: dict[str, pd.DataFrame] = {}
    for sheet_name in (SHEET_PEDIDOS_HISTORICOS, SHEET_PEDIDOS_OPERATIVOS, SHEET_CASOS_ESPECIALES):
        try:
            worksheet = get_worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            sources[sheet_name] = pd.DataFrame()
            continue
        df = values_to_dataframe(run_gsheets_request(worksheet.get_all_values))
        sources[sheet_name] = df[[column for column in SOURCE_COLUMNS if column in df.columns]].copy()
    return sources


def _with_columns(df: pd.DataFrame | None, columns: Sequence[str]) -> pd.DataFrame:
    df = pd.DataFrame() if df is None else df.copy()
    for column in columns:
        if column not in df.columns:
            df[column] = ""
    return df.fillna("").astype(str)


def _consolidated_guides(df: pd.DataFrame, primary: str, fallback: str) -> pd.Series:
    primary_values = df[primary].str.strip()
    return primary_values.mask(primary_values.eq(""), df[fallback].str.strip())


def _infer_case_shipping(row: pd.Series) -> str:
    shipping = str(row.get("Tipo_Envio", "")).strip()
    if shipping:
        return shipping
    case_type = str(row.get("Tipo_Caso", "")).lower()
    if case_type.startswith("devol"):
        return "🔁 Devolución"
    if case_type.startswith("garan"):
        return "🛠 Garantía"
    return "Caso especial"


def parse_datetime_series(series: pd.Series) -> pd.Series:
    try:
        return pd.to_datetime(series, errors="coerce", format="mixed")
    except (TypeError, ValueError):
        return pd.to_datetime(series, errors="coerce")


def build_guides_dataset(sources: Mapping[str, pd.DataFrame]) -> pd.DataFrame:
    """Une pedidos y casos especiales que ya tienen guía, igual que app_v.

    En pedidos la guía vive en Adjuntos_Guia (respaldo: Hoja_Ruta_Mensajero);
    en casos especiales es al revés.
    """

    frames = []
    for sheet_name, primary, fallback in (
        (SHEET_PEDIDOS_HISTORICOS, "Adjuntos_Guia", "Hoja_Ruta_Mensajero"),
        (SHEET_PEDIDOS_OPERATIVOS, "Adjuntos_Guia", "Hoja_Ruta_Mensajero"),
        (SHEET_CASOS_ESPECIALES, "Hoja_Ruta_Mensajero", "Adjuntos_Guia"),
    ):
        df = _with_columns(sources.get(sheet_name), SOURCE_COLUMNS)
        guides = _consolidated_guides(df, primary, fallback)
        part = df[guides.ne("")].copy()
        if part.empty:
            continue
        part["Adjuntos_Guia"] = guides[guides.ne("")]
        if sheet_name == SHEET_CASOS_ESPECIALES:
            part["Tipo_Envio"] = part.apply(_infer_case_shipping, axis=1)
        part["Fuente"] = sheet_name
        frames.append(part)
    if not frames:
        return pd.DataFrame(columns=GUIDE_COLUMNS)

    df = pd.concat(frames, ignore_index=True)
    df["URLs_Guia"] = df["Adjuntos_Guia"]
    df["Ultima_Guia"] = df["URLs_Guia"].map(lambda text: text.split(",")[-1].strip())
    df["Hora_Registro_dt"] = parse_datetime_series(df["Hora_Registro"])
    df["Fecha_Entrega_dt"] = parse_datetime_series(df["Fecha_Entrega"])
    df["Fecha_Filtro_Referencia"] = df["Hora_Registro_dt"].fillna(df["Fecha_Entrega_dt"])
    df["Folio_O_ID"] = df["Folio_Factura"].str.strip().mask(df["Folio_Factura"].str.strip().eq(""),
                                                             df["ID_Pedido"].str.strip())
    df = df.sort_values("Fecha_Filtro_Referencia", ascending=False, na_position="last", kind="stable")
    return df[GUIDE_COLUMNS].reset_index(drop=True)


def guide_key(row: pd.Series) -> str:
    reference = str(row.get("ID_Pedido", "")).strip() or str(row.get("Folio_Factura", "")).strip()
    return f"{row.get('Fuente', '')}::{reference}::{str(row.get('Adjuntos_Guia', '')).strip()}"


def summarize_session_guides(df: pd.DataFrame, now: datetime | None = None,
                             id_vendedor: str = GUIDE_USER_ID) -> dict[str, Any]:
    """Guías de las últimas 12 h para el ID de vendedor que no se han limpiado."""

    now = now or app_now()
    if df.empty:
        return {"total": 0, "clientes": [], "keys": []}
    mask = (
        df["id_vendedor"].map(normalize_vendor_id).eq(id_vendedor)
        & df["Completados_Limpiado"].astype(str).str.strip().eq("")
        & df["Hora_Registro_dt"].notna()
        & (df["Hora_Registro_dt"] >= now - timedelta(hours=GUIDES_NOTICE_HOURS))
    )
    rows = df[mask]
    keys = sorted({guide_key(row) for _, row in rows.iterrows()})
    clients = [client for client in rows["Cliente"].astype(str).str.strip() if client]
    return {"total": len(keys), "clientes": list(dict.fromkeys(clients)), "keys": keys}


def format_clients_preview(clients: Sequence[str]) -> str:
    preview = ", ".join(clients[:2])
    if len(clients) > 2:
        preview = f"{preview} y {len(clients) - 2} más"
    return preview


def recent_guides(df: pd.DataFrame, now: datetime | None = None) -> pd.DataFrame:
    """Sólo el último mes: mantiene la vista rápida, igual que app_v."""

    now = now or app_now()
    cutoff = now - timedelta(days=GUIDES_HISTORY_DAYS)
    reference = df["Fecha_Filtro_Referencia"]
    return df[reference.notna() & (reference >= cutoff)].copy()


def visible_vendor_guides(df: pd.DataFrame) -> pd.DataFrame:
    """ARTTD JIMENA sólo ve sus guías y las de SCHAVA."""

    return df[df["Vendedor_Registro"].astype(str).str.strip().isin(GUIDE_VENDORS)].copy()


def apply_guides_filters(df: pd.DataFrame, *, vendor: str = "Todos", date_mode: str = "7d",
                         start: date | None = None, end: date | None = None,
                         day: date | None = None, today: date | None = None) -> pd.DataFrame:
    """date_mode: "range" (start-end), "7d" (últimos 7 días) o "day" (una fecha)."""

    today = today or app_today()
    dates = df["Fecha_Filtro_Referencia"].dt.date
    if date_mode == "range":
        if start and end and start <= end:
            df = df[dates.between(start, end)]
    elif date_mode == "7d":
        df = df[dates.between(today - timedelta(days=6), today)]
    elif day:
        df = df[dates == day]
    if vendor != "Todos":
        df = df[df["Vendedor_Registro"] == vendor]
    return df.copy()


def guide_display_label(row: pd.Series) -> str:
    return (f"📄 {row['Folio_O_ID']} – {row['Cliente']} – {row['Vendedor_Registro']} "
            f"({row['Tipo_Envio']}) · {row['Fuente']}")


def split_urls(value: Any) -> list[str]:
    """Adjuntos pueden venir como JSON o como texto separado por comas."""

    text = clean_cell(value)
    if not text or text.lower() == "n/a":
        return []
    urls: list[str] = []
    try:
        parsed = json.loads(text)
        items = parsed if isinstance(parsed, list) else [parsed]
        for item in items:
            if isinstance(item, str) and item.strip():
                urls.append(item.strip())
            elif isinstance(item, dict):
                urls.extend(str(item[key]).strip() for key in ("url", "URL", "href", "link")
                            if str(item.get(key, "")).strip())
    except (ValueError, TypeError):
        urls = [part.strip() for part in re.split(r"[,\n;]+", text) if part.strip()]
    return list(dict.fromkeys(urls))


def normalize_url(value: str) -> str:
    raw = clean_cell(value)
    parts = urlsplit(raw)
    if not parts.scheme or not parts.netloc:
        return raw
    return urlunsplit((parts.scheme, parts.netloc, quote(parts.path, safe="/%:@"),
                       quote(parts.query, safe="=&%:@"), quote(parts.fragment, safe="=&%:@")))


def file_name_from_url(value: str) -> str:
    cleaned = clean_cell(value).split("?")[0].split("#")[0]
    return unquote(cleaned.rsplit("/", 1)[-1]) or cleaned or "Archivo"


def _aws_config(secret_source: Any | None = None) -> dict[str, str]:
    """Credenciales para abrir guías en vista previa: [ventas_aws] o las del laboratorio."""

    source = _secret_source(secret_source)
    keys = ("aws_access_key_id", "aws_secret_access_key", "aws_region", "s3_bucket_name")
    for section in (_secret_section(source, "ventas_aws"), _secret_section(source, "aws"), source):
        config = {key: clean_cell(section.get(key, "")) for key in keys} if hasattr(section, "get") else {}
        if config and all(config.values()):
            return config
    return {}


@st.cache_data(ttl=1800, show_spinner=False)
def guide_browser_url(url: str) -> str:
    """Firma la URL de S3 para abrirla en el navegador en lugar de descargarla."""

    normalized = normalize_url(url)
    try:
        config = _aws_config()
    except Exception:
        return normalized
    parts = urlsplit(normalized)
    bucket, region = config.get("s3_bucket_name", ""), config.get("aws_region", "")
    hosts = {f"{bucket}.s3.{region}.amazonaws.com".casefold(), f"{bucket}.s3.amazonaws.com".casefold()}
    object_key = unquote(parts.path.lstrip("/"))
    if not config or parts.netloc.casefold() not in hosts or not object_key:
        return normalized
    file_name = object_key.rsplit("/", 1)[-1]
    extension = "." + file_name.rsplit(".", 1)[-1].lower() if "." in file_name else ""
    params = {
        "Bucket": bucket,
        "Key": object_key,
        "ResponseContentDisposition": f"inline; filename*=UTF-8''{quote(file_name)}",
    }
    if extension in INLINE_CONTENT_TYPES:
        params["ResponseContentType"] = INLINE_CONTENT_TYPES[extension]
    try:
        import boto3

        client = boto3.client(
            "s3",
            aws_access_key_id=config["aws_access_key_id"],
            aws_secret_access_key=config["aws_secret_access_key"],
            region_name=region,
        )
        signed = client.generate_presigned_url("get_object", Params=params, ExpiresIn=3600)
    except Exception:
        return normalized
    if extension in OFFICE_PREVIEW_EXTENSIONS:
        return f"https://docs.google.com/gview?url={quote(signed, safe='')}"
    return signed


# ==============================
# Interfaz
# ==============================
def apply_custom_css() -> None:
    """Complementa la paleta azul del chrome compartido con las piezas de esta vista."""

    st.markdown(
        """<style>
        .guides-section-title {
            display:inline-flex; align-items:center; gap:8px; margin:6px 0 10px;
            padding:7px 14px; border-radius:999px; background:#DCE8F9; border:1px solid #B8CDEE;
            color:#17325C; font-size:.86rem; font-weight:800; letter-spacing:.02em;
        }
        [data-testid="stBaseButton-primaryFormSubmit"] {
            background:linear-gradient(110deg,#2F76DA,#1B4F9C); border-color:#1F5DB8; color:#FFF;
            box-shadow:0 4px 12px #2F76DA2B;
        }
        [data-testid="stBaseButton-primaryFormSubmit"]:not(:disabled):hover {box-shadow:0 5px 16px #2F76DA4D;}
        /* El tema del repo es morado: en esta vista los campos también van en azul. */
        [data-testid="stTextInputRootElement"], [data-testid="stTextAreaRootElement"],
        [data-testid="stDateInputField"], [data-testid="stSelectbox"] .react-aria-ComboBox > [role="group"] {
            background-color:#E6EEFB !important; border-color:#C9D9F2 !important;
        }
        [data-testid="stTextInputRootElement"]:focus-within, [data-testid="stTextAreaRootElement"]:focus-within,
        [data-testid="stDateInputField"]:focus-within {border-color:#2F74D6 !important;}
        [data-testid="stExpander"] summary {background:#E4EDFB !important;}
        [data-testid="stCheckbox"] label:has(input:checked) > div:not([data-testid]) {
            background:#1F5DB8 !important; border-color:#1F5DB8 !important;
        }
        .guides-summary {
            display:flex; flex-wrap:wrap; gap:8px; margin:2px 0 12px;
        }
        .guides-summary span {
            border-radius:999px; padding:6px 12px; font-size:.8rem; font-weight:750;
            background:#E8F1FF; border:1px solid #BCD2F1; color:#1A4786;
        }
        </style>""",
        unsafe_allow_html=True,
    )


def section_title(text: str) -> None:
    st.markdown(f'<div class="guides-section-title">{text}</div>', unsafe_allow_html=True)


def current_refresh_token() -> float | None:
    return st.session_state.get(GUIDES_REFRESH_TOKEN_KEY)


def load_guides_dataset() -> pd.DataFrame:
    return build_guides_dataset(read_guides_sources(current_refresh_token()))


def render_guides_notice(dataset: pd.DataFrame) -> None:
    """Mismo aviso que app_v muestra arriba de las pestañas."""

    summary = summarize_session_guides(dataset)
    current_keys = set(summary["keys"])
    previous_keys = set(st.session_state.get("guides_home_keys") or [])
    new_keys = current_keys - previous_keys
    if previous_keys and new_keys:
        st.warning(f"🔔 Aviso rápido: tienes {len(new_keys)} guía(s) nueva(s).")
    if summary["total"] > 0:
        preview = format_clients_preview(summary["clientes"])
        clients = f" Clientes: {preview}." if preview else ""
        st.info(f"📦 Aviso: tienes {summary['total']} pedido(s) con guía cargada.{clients}")
    st.session_state["guides_home_keys"] = sorted(current_keys)


def render_request_feedback() -> None:
    message = st.session_state.get("guides_request_success")
    if not message:
        return
    if st.session_state.pop("guides_request_celebrate", False):
        st.toast(message, icon="✅")
        st.balloons()
    st.success(message)
    if st.button("✅ Aceptar y limpiar mensaje", key="guides_request_clear"):
        st.session_state.pop("guides_request_success", None)
        rerun_active_tab()


def render_request_tab() -> None:
    """Formulario "Nuevo Pedido" de ARTTD JIMENA: sólo Solicitudes de Guía."""

    st.subheader("📝 Nuevo Pedido")
    st.caption(
        f"Registra la solicitud en {SHEET_PEDIDOS_OPERATIVOS} como {GUIDE_VENDOR_NAME}, igual que en app_v. "
        "Cuando almacén cargue la guía aparecerá en 📦 Guías Cargadas."
    )
    render_request_feedback()
    version = st.session_state.get("guides_form_version", 0)
    today = app_today()

    def field(label: str, key: str, *, area: bool = False, container=st, **kwargs) -> str:
        widget = container.text_area if area else container.text_input
        return widget(label, key=f"guides_{key}_{version}", **kwargs)

    with st.form(f"guides_request_form_{version}"):
        section_title("🧾 Información Básica del Cliente y Pedido")
        first = st.columns(3)
        first[0].selectbox("📦 Tipo de Envío", [TIPO_ENVIO_GUIA], disabled=True,
                           key=f"guides_tipo_envio_{version}")
        first[1].selectbox("👤 Vendedor", [GUIDE_VENDOR_NAME], disabled=True,
                           key=f"guides_vendedor_{version}")
        folio = field("📄 Folio de Factura (opcional)", "folio", container=first[2])
        comentario = field("💬 Comentario / Descripción Detallada", "comentario", area=True,
                           help="Agrega indicaciones o comentarios para la solicitud de guía.")

        with st.expander("📬 Dirección guía Manual  (Obligatorio al Solicitar Guia)", expanded=True):
            st.info("Completa la dirección para solicitar la guía DHL.")
            section_title("CAMPOS OBLIGATORIOS DHL (MÉXICO)")
            left, right = st.columns(2)
            address = {
                "nombre": field("Nombre *", "nombre", container=left),
                "calle_numero": field("Calle y número *", "calle_numero", container=left),
                "colonia": field("Colonia *", "colonia", container=left),
                "codigo_postal": field("Código Postal *", "codigo_postal", container=left),
                "ciudad": field("Ciudad *", "ciudad", container=right),
                "estado": field("Estado *", "estado", container=right),
                "pais": field("País *", "pais", container=right, value="México"),
                "telefono": field("Teléfono *", "telefono", container=right),
            }
            section_title("CAMPOS OPCIONALES / RECOMENDADOS")
            optional = st.columns(3)
            address["interior"] = field("Interior / Depto", "interior", container=optional[0])
            address["municipio"] = field("Municipio / Alcaldía", "municipio", container=optional[1])
            address["correo"] = field("Correo electrónico", "correo", container=optional[2])
            address["referencias"] = field("Referencias de dirección", "referencias", area=True)

        st.info(f"✅ Tipo de envío seleccionado: {TIPO_ENVIO_GUIA} | "
                f"Fecha de Entrega Seleccionada: {today:%d/%m/%Y}")
        submitted = st.form_submit_button("✅ Registrar Pedido", type="primary")

    if not submitted:
        return
    missing = get_missing_dhl_mexico_required_fields(address)
    if missing:
        st.warning("⚠️ El pedido no se subió. Completa los campos obligatorios para solicitar guía manual.")
        st.write("Faltan: " + ", ".join(missing))
        return
    request = {"folio_factura": folio, "comentario": comentario, "direccion": address, "fecha_entrega": today}
    try:
        with st.spinner("Registrando pedido en Google Sheets..."):
            reference = register_guide_request(request)
    except Exception as exc:
        st.error(f"❌ Falla al subir el pedido.\n\n🔍 Detalle: {exc}")
        st.info("Si fue un error de conexión, vuelve a presionar Registrar Pedido: "
                "la misma solicitud no se duplica.")
        return
    st.session_state["guides_request_success"] = (
        f"✅ El pedido {reference} (ID vendedor: {GUIDE_USER_ID}) fue subido correctamente."
    )
    st.session_state["guides_request_celebrate"] = True
    st.session_state["guides_form_version"] = version + 1
    rerun_active_tab()


def refresh_guides() -> None:
    last = st.session_state.get("guides_last_refresh", 0.0)
    if time.time() - last < GUIDES_REFRESH_COOLDOWN_SECONDS:
        st.session_state["guides_refresh_wait"] = True
        return
    st.session_state["guides_last_refresh"] = time.time()
    st.session_state[GUIDES_REFRESH_TOKEN_KEY] = time.time()


def render_tab_guides_notice(guides: pd.DataFrame) -> None:
    """Aviso por ID de vendedor antes de aplicar filtros visuales, como app_v."""

    summary = summarize_session_guides(guides)
    rows = {guide_key(row): str(row.get("Cliente", "")).strip() or "Cliente sin nombre"
            for _, row in guides.iterrows()}
    current_keys = set(summary["keys"])
    previous_keys = set(st.session_state.get("guides_tab_keys") or [])
    new_keys = sorted(current_keys - previous_keys)
    if previous_keys and new_keys:
        clients = list(dict.fromkeys(rows.get(key, "Cliente sin nombre") for key in new_keys))
        detail = ", ".join(clients[:3]) + (f" y {len(clients) - 3} más" if len(clients) > 3 else "")
        st.success(f"🔔 Se cargaron {len(new_keys)} guía(s) nueva(s) para tus pedidos (ID vendedor: {GUIDE_USER_ID}).")
        st.info(f"👤 Clientes con nuevas guías: {detail}.")
        st.toast(f"🔔 Nuevas guías detectadas: {len(new_keys)}", icon="📦")
    elif current_keys:
        st.info(f"📦 Tienes {len(current_keys)} pedido(s) con guía cargada pendiente (ID vendedor: {GUIDE_USER_ID}).")
    else:
        st.caption(f"Sin nuevas guías detectadas aún para el ID vendedor {GUIDE_USER_ID}.")
    st.session_state["guides_tab_keys"] = sorted(current_keys)


def render_guide_links(row: pd.Series) -> None:
    urls = split_urls(row.get("URLs_Guia", ""))
    last_guide = clean_cell(row.get("Ultima_Guia", ""))
    if row.get("Fuente") == SHEET_CASOS_ESPECIALES:
        section_title("📎 Guías Subidas")
        links = urls or ([last_guide] if last_guide else [])
    else:
        section_title("📎 Última Guía Subida")
        links = [last_guide] if last_guide else []
    if not links:
        st.warning("⚠️ No se encontró una URL válida para la guía.")
        return
    for index, url in enumerate(links):
        st.link_button(f"📄 {file_name_from_url(url)}", guide_browser_url(url),
                       key=f"guides_link_{index}_{hashlib.sha256(url.encode()).hexdigest()[:10]}")


def render_loaded_tab(dataset: pd.DataFrame) -> None:
    st.subheader("📦 Pedidos con Guías Subidas desde Almacén y Casos Especiales")
    toolbar = st.columns([1.2, 4])
    toolbar[0].button("🔄 Actualizar guías", on_click=refresh_guides, width="stretch")
    if st.session_state.pop("guides_refresh_wait", False):
        toolbar[1].caption("⏳ Espera unos segundos antes de volver a actualizar.")

    guides = visible_vendor_guides(recent_guides(dataset))
    if guides.empty:
        st.info("No hay pedidos o casos especiales con guías subidas.")
        return
    counts = guides["Fuente"].value_counts().sort_index()
    st.markdown(
        '<div class="guides-summary">'
        + "".join(f"<span>{source}: {count}</span>" for source, count in counts.items())
        + "</div>",
        unsafe_allow_html=True,
    )
    render_tab_guides_notice(guides)

    section_title("🔍 Filtros")
    st.caption(f"⚡ Por velocidad, esta pestaña solo carga guías de los últimos {GUIDES_HISTORY_DAYS} días.")
    today = app_today()
    minimum = today - timedelta(days=GUIDES_HISTORY_DAYS)
    # Los defaults viven en session_state (no en value=) para que Streamlit no
    # advierta al ajustarlos; una fecha fuera del último mes se sube al mínimo.
    defaults = {"guides_filter_7d": True, "guides_filter_start": today - timedelta(days=7),
                "guides_filter_end": today, "guides_filter_day": today}
    for key, default in defaults.items():
        st.session_state.setdefault(key, default)
        value = st.session_state[key]
        if isinstance(value, date) and value < minimum:
            st.session_state[key] = minimum
    vendor_column, date_column = st.columns(2)
    vendors = ["Todos", *GUIDE_VENDORS]
    if st.session_state.get("guides_filter_vendor") not in vendors:
        st.session_state["guides_filter_vendor"] = GUIDE_VENDOR_NAME
    vendor = vendor_column.selectbox("Filtrar por Vendedor", vendors, key="guides_filter_vendor")
    with date_column:
        use_range = st.checkbox("🔁 Activar búsqueda por rango de fechas", key="guides_filter_range")
        if use_range and st.session_state.get("guides_filter_7d"):
            st.session_state["guides_filter_7d"] = False
        last_7 = st.checkbox("Mostrar últimos 7 días", key="guides_filter_7d", disabled=use_range)
        start = end = day = None
        if use_range:
            range_columns = st.columns(2)
            start = range_columns[0].date_input("📅 Fecha inicial:", min_value=minimum,
                                                key="guides_filter_start", format="DD/MM/YYYY")
            end = range_columns[1].date_input("📅 Fecha final:", min_value=minimum,
                                              key="guides_filter_end", format="DD/MM/YYYY")
            if start and end and start > end:
                st.warning("⚠️ La fecha inicial no puede ser mayor que la fecha final.")
        else:
            day = st.date_input("📅 Filtrar por Fecha de Registro:", min_value=minimum,
                                key="guides_filter_day", disabled=last_7, format="DD/MM/YYYY")
    date_mode = "range" if use_range else ("7d" if last_7 else "day")
    guides = apply_guides_filters(guides, vendor=vendor, date_mode=date_mode, start=start, end=end,
                                  day=day, today=today).reset_index(drop=True)
    guides["display_label"] = guides.apply(guide_display_label, axis=1)

    table = guides[TABLE_COLUMNS].copy()
    table["Fecha_Entrega"] = guides["Fecha_Entrega_dt"].dt.strftime("%d/%m/%y").fillna("")
    event = st.dataframe(table, width="stretch", hide_index=True, key="guides_table",
                         on_select="rerun", selection_mode="single-row")
    selected_rows = event.selection.rows
    if selected_rows and 0 <= selected_rows[0] < len(guides):
        label = guides.iloc[selected_rows[0]]["display_label"]
        # Sólo sincroniza al cambiar de fila para que el selector se pueda mover después.
        if st.session_state.get("guides_last_table_row") != label:
            st.session_state["guides_selected_order"] = label
            st.session_state["guides_last_table_row"] = label

    section_title("📥 Selecciona un Pedido para Ver la Última Guía Subida")
    options = guides["display_label"].tolist()
    if not options:
        st.info("No hay guías con los filtros seleccionados.")
        return
    if st.session_state.get("guides_selected_order") not in options:
        st.session_state["guides_selected_order"] = options[0]
    selected = st.selectbox("📦 Pedido/Caso con Guía", options, key="guides_selected_order")
    render_guide_links(guides[guides["display_label"] == selected].iloc[0])


@st.fragment(key="lab_guides_tab_content")
def render_guides_tabs(current_user: str) -> None:
    """Ejecuta únicamente la pestaña visible, como las demás vistas del laboratorio."""

    dataset = load_guides_dataset()
    render_guides_notice(dataset)
    tabs = st.tabs(GUIDES_TAB_LABELS, key=f"guides_primary_tabs_{current_user}", on_change="rerun")
    for label, tab in zip(GUIDES_TAB_LABELS, tabs):
        if not tab.open:
            continue
        with tab:
            if label == GUIDES_TAB_REQUEST:
                render_request_tab()
            else:
                render_loaded_tab(dataset)
        break


def render_embedded_workspace(current_user: str) -> None:
    """Renderiza Solicitudes de guía dentro de lab_pg usando la sesión compartida."""

    render_guides_tabs(current_user)
