"""Aplicación de seguimiento para ``Control ALINEADORES``.

La hoja ``PROCESOS POR PRODUCTO`` es la fuente de verdad del flujo y de los
plazos. Los estados marcados como pausa se ofrecen de forma opcional y no
forman parte de la secuencia obligatoria.
"""

from __future__ import annotations

import hashlib
import hmac
import html
import io
import json
import math
import re
import time
import unicodedata
import threading
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from contextlib import contextmanager
from typing import Any, Callable, Iterable, Iterator
from zoneinfo import ZoneInfo

import gspread
import pandas as pd
import streamlit as st
import forms_notices
import order_detail
import case_invoices
import case_search
from user_permissions import (APP_USERS, VIEW_AND_CREATE_USERS, can_change_status, can_create_orders,
                              can_edit_orders, status_only)
from grid_interactions import response_frame
import dropdown_fields
from order_identifiers import identifier_resolver
from copy import deepcopy
from google.auth.transport.requests import AuthorizedSession
from google.oauth2.service_account import Credentials
from gspread.cell import Cell
from gspread.utils import rowcol_to_a1
from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import (
    HRFlowable,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)
from streamlit.errors import StreamlitAPIException

from aligners_board import render_board_html
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
SHEET_POLANCO = "POLANCO"
SHEET_POLANCO_TIMES = "TIEMPOS_POLANCO"
SHEET_AERIS = "AERIS"
SHEET_AERIS_TIMES = "TIEMPOS_AERIS"
POLANCO_TAB_LABEL = "🏢 Polanco"
AERIS_TAB_LABEL = "🌬️ AERIS"
ORDER_TIME_SHEETS = {
    SHEET_ORDERS: SHEET_TIMES,
    SHEET_POLANCO: SHEET_POLANCO_TIMES,
    SHEET_AERIS: SHEET_AERIS_TIMES,
}
TRACKING_TAB_SHEETS = {
    "📋 Seguimiento": SHEET_ORDERS,
    POLANCO_TAB_LABEL: SHEET_POLANCO,
    AERIS_TAB_LABEL: SHEET_AERIS,
}
# Seguimiento General: las órdenes de las tres hojas en una sola tabla. Cada
# orden se sigue leyendo y guardando en su hoja de origen, así Polanco y AERIS
# conservan su registro (y su contabilidad) aparte.
GENERAL_TAB_LABEL = "🗂️ Seguimiento General"
GENERAL_ORIGINS = (
    ("📋 Seguimiento", SHEET_ORDERS),
    ("🏢 Polanco", SHEET_POLANCO),
    ("🌬️ AERIS", SHEET_AERIS),
)
ORIGIN_COLUMN = "ORIGEN"
GENERAL_OPEN_COLUMN = "ABRIR"
GENERAL_ROW_KEY_COLUMN = "_ORIGEN_ORDEN"
GENERAL_KEY_PREFIX = "general_"
GENERAL_ALL_ORIGINS = "Todos los orígenes"
GENERAL_EDITOR_KEY_STATE = "general_aligners_editor_key"
GENERAL_EDITOR_BASELINE_STATE = "general_aligners_editor_baseline"
GENERAL_REVISION_KEY = "general_aligners_revision"
# Ocultar las pestañas por hoja: se recuerda en la sesión y en el enlace.
GENERAL_HIDE_STATE_KEY = "aligners_hide_individual_tracking"
GENERAL_HIDE_PARAM = "seguimiento"
GENERAL_HIDE_PARAM_VALUE = "general"
GENERAL_HIDE_TOGGLE_KEY = f"{GENERAL_KEY_PREFIX}hide_individual_toggle"
CASE_ORDER_SHEETS = {
    "alineadores": SHEET_ORDERS,
    "polanco": SHEET_POLANCO,
    "aeris": SHEET_AERIS,
}
ORDER_HEADER_ROW = 2

ID_COLUMN = "No. Orden"
STATUS_COLUMN = "STATUS"
PRODUCT_COLUMN = "PRODUCTO"
APP_TIMEZONE_NAME = "America/Mexico_City"
APP_TIMEZONE = ZoneInfo(APP_TIMEZONE_NAME)

PAUSE_STATUSES = (
    "BORRADOR TITAN",
    "SOLICITUD DE CAMBIOS",
    "IMPRESIÓN EN PAUSA",
    "FALTA DE PAGO DE PLANEACIÓN",
    "FALTA DE PAGO DE IMPRESIÓN",
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

# ==============================
# Google Forms – Recepción de prescripciones
# ==============================
DEFAULT_FORMS_WORKSHEET = "Respuestas de formulario 1"
FORMS_WORKSHEET_FALLBACKS = ["Respuestas de formulario 1", "Form_Responses"]
# Preguntas de subida de archivos. Cada vez que se edita un formulario, Google
# agrega una columna nueva y deja la vieja: Marca Blanca tiene "Favor de subir
# archivos en formato ZIP", "Favor de subir archivos: - Escaneos STL..." y
# "Por favor sube el archivo de la imagen de tu logo". Todas cuentan como
# adjuntos de la respuesta.
FORMS_FILE_COLUMN_HINTS = (
    "subir archivo",
    "sube el archivo",
    "sube los archivos",
    "adjuntar archivo",
    "archivos en formato ZIP",
)
# Google Forms guarda los archivos subidos como links "drive.google.com/open?id=..."
# separados por coma; sirve para reconocer la pregunta aunque cambie su título.
_FORMS_UPLOAD_LINK = r"https://drive\.google\.com/open\?id=[\w-]+"
FORMS_UPLOAD_ANSWER_RE = re.compile(rf"{_FORMS_UPLOAD_LINK}(?:\s*,\s*{_FORMS_UPLOAD_LINK})*")
FORMS_EXCLUDED_COLUMN_HINTS = [
    "Acepto que he leído los Términos y Condiciones",
    "Estoy de acuerdo con el resguardo de mis datos personales",
    "El llenado de esta prescripción es importante",
    "El plan de tratamiento esta basado",
    "El resultado del tratamiento es responsabilidad",
    "Al aceptar el médico tratante ha comprendido",
]
FORMS_API_URL = "https://forms.googleapis.com/v1/forms/{form_id}"
FORMS_FORM_STRUCTURE_TTL_SECONDS = 120
# Columnas que Google agrega al Sheet sin ser preguntas del formulario.
FORMS_METADATA_HEADERS = {
    "MARCA TEMPORAL",
    "TIMESTAMP",
    "DIRECCION DE CORREO ELECTRONICO",
    "EMAIL ADDRESS",
    "PUNTUACION",
    "SCORE",
}
FORMS_FIRST_SECTION_TITLE = "Datos capturados"
FORMS_TAB_LABEL = "📥 Recibidos de Forms"
# Tabla única de "Recibidos de Forms": todas las respuestas de los formularios.
FORMS_INBOX_TABLE_KEY = "aligners_forms_table"
FORMS_INBOX_SELECTOR_KEY = "aligners_forms_selector"
FORMS_INBOX_RECENT_ROWS = 40
# Columnas internas de la tabla única; no se muestran.
FORMS_INBOX_ID_COLUMN = "_respuesta"
FORMS_INBOX_FORM_COLUMN = "_formulario"
FORMS_INBOX_KEY_COLUMN = "_clave"
FORMS_INBOX_TABLE_COLUMNS = [
    "Nueva", "Formulario", "Respuesta #", "Marca temporal", "Paciente", "Doctor", "Asesor",
    "Archivos",
]
FORMS_INBOX_PATIENT_HINTS = ("nombre completo del paciente", "nombre del paciente")
FORMS_INBOX_DOCTOR_HINTS = ("medico tratante", "doctor tratante", "nombre del doctor")
FORMS_INBOX_ADVISOR_HINTS = ("nombre del asesor",)
FORMS_OTHER_ANSWERS_TITLE = "Otras respuestas (preguntas que ya no están en el formulario)"

# Cada formulario apunta al Google Sheet de respuestas ya compartido con la
# misma cuenta de servicio de [gsheets].google_credentials. El sheet_id/worksheet
# se puede sobreescribir en secrets bajo [google_forms_alineadores.<key>], pero
# no es obligatorio: si no hay override, se usa el valor por defecto de aquí.
# sheet_id verificados contra el título real de cada Google Sheet de
# respuestas (metadata de Drive): "Alineadores (Respuestas)" es el formulario
# TD, "MARCA BLANCA (Respuestas)" es marca blanca y "Otros Productos
# (Respuestas)" es el formulario "Otros Productos" (antes iban cruzados).
# form_id es el Google Form que llena esas respuestas ("Alineadores", "MARCA
# BLANCA " y "Otros Productos" en Drive). Se lee con Google Forms API para que
# el PDF siga el orden y las secciones actuales del formulario: Google agrega
# al final del Sheet cada pregunta nueva, así que el orden de columnas deja de
# coincidir con el formulario en cuanto se edita. También admite override en
# [google_forms_alineadores.<key>].form_id.
ALIGNERS_FORMS: tuple[dict[str, str], ...] = (
    {
        "key": "td",
        "label": "🔵 Prescripción Alineadores TD",
        "default_sheet_id": "1_wBpRLzN9p87sJq9961J_qtHEdpLvYSDEPqNQrqqlZ0",
        "default_form_id": "1-31c4NAll-3NqsOecSBmVOU-AM0OdckMnvtk84uSBzE",
    },
    {
        "key": "marca_blanca",
        "label": "🟣 Prescripción Marca Blanca",
        "default_sheet_id": "1OYwJI_IaqGYOXR4aTffYmLK_38hd0p51_3C7vAjFXEU",
        "default_form_id": "1alX8OXw_iSvrBnYzPLI1Sy5CDg0XiMMIhm-bMJ0G_lI",
    },
    {
        "key": "otros_productos",
        "label": "📦 Otros Productos",
        "default_sheet_id": "1FFjO3RTMRZQ4OBfoL4thy_8GpIxgdlASkWggN_RMlRU",
        "default_form_id": "1iMRjp1eaHDEvqbUA165fax6_3wlJJVXwY3wDiq87ZFs",
    },
    {
        "key": "aeris",
        "label": AERIS_TAB_LABEL,
        # Los archivos privados nuevos se conectan desde Streamlit Secrets.
        "default_sheet_id": "",
        "default_form_id": "",
    },
)

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

MULTI_SELECT_COLUMNS = {"ARCHIVOS RECIBIDOS"}

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
    "FALTA DE PAGO DE PLANEACION": "⏸️",
    "FALTA DE PAGO DE IMPRESION": "⏸️",
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
    "FALTA DE PAGO DE PLANEACION": ("#F4CCCC", "#8A1C1C"),
    "FALTA DE PAGO DE IMPRESION": ("#F4CCCC", "#8A1C1C"),
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
ALIGNERS_SIGNAL_FILTER_KEY = "aligners_signal_filter"

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


def is_sent_status(value: Any) -> bool:
    return status_key(value) == "ENVIADO"


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


# Igual que en Aparatos: Admin, Jime y Lesly pueden corregir la etapa eligiendo
# cualquiera del flujo con «Ver todas las etapas…» dentro de STATUS.
MANUAL_STAGE_USERS = ("Admin", "Jime", "Lesly")
MANUAL_STAGE_COMMENT = "Cambio manual de etapa desde STATUS (todas las etapas)."


def get_manual_stage_statuses(
    product: Any,
    current_status: Any,
    definitions: dict[str, ProcessDefinition],
    current_user: str,
) -> list[str]:
    """Etapas del producto para una corrección explícita de un pedido activo.

    Incluye todo el flujo normal, las pausas y Cancelado. Un pedido enviado o
    cancelado se corrige desde el histórico, no desde aquí.
    """

    definition = get_process_definition(product, definitions)
    current = canonical_status(current_status, configured_statuses(definitions))
    if current_user not in MANUAL_STAGE_USERS or definition is None or is_terminal_status(current):
        return [current]
    return list(dict.fromkeys([current, *definition.normal_statuses, *definition.pauses, "CANCELADO"]))


def get_reactivation_statuses(
    product: Any,
    current_status: Any,
    definitions: dict[str, ProcessDefinition],
) -> list[str]:
    """Etapas a las que puede regresar un pedido enviado para volver a la mesa activa.

    Se ofrece cualquier etapa normal o pausa del producto, salvo Enviado y
    Cancelado. Un pedido que no está enviado sólo conserva su etapa actual.
    """

    definition = get_process_definition(product, definitions)
    current = canonical_status(current_status, configured_statuses(definitions))
    if definition is None or not is_sent_status(current):
        return [current]
    targets = [
        status
        for status in (*definition.normal_statuses, *definition.pauses)
        if not is_terminal_status(status)
    ]
    return list(dict.fromkeys([current, *targets]))


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
    if column in MULTI_SELECT_COLUMNS:
        return set(dropdown_fields.split_values(old_text)) == set(dropdown_fields.split_values(new_text))
    if column == STATUS_COLUMN:
        return status_key(old_text) == status_key(new_text)
    if column == PRODUCT_COLUMN:
        return product_key(old_text) == product_key(new_text)
    if column == "VENDEDOR":
        return dropdown_fields.canonical_vendor(old_value) == dropdown_fields.canonical_vendor(new_value)
    if column in DATE_COLUMNS or column.startswith("FECHA"):
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


def signal_detail(detail: Any) -> str:
    """Motivo del semáforo para la ficha: la pausa ya se ve en el chip de la etapa."""
    return " · ".join(part for part in clean_cell(detail).strip().split(" · ")
                      if part.strip() and not part.startswith("Pausa: "))


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
    return hmac.compare_digest(password.encode("utf-8"), stored_value.encode("utf-8"))


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
    for namespace in ("aligners", "polanco_aligners", "aeris_aligners"):
        order_detail.clear_drafts(namespace)
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

    configured_passwords = get_user_passwords()
    missing = missing_user_passwords(configured_passwords)
    if missing:
        st.warning("Falta configurar la contraseña de: " + ", ".join(missing) + ".")
        st.info(
            "Usa los mismos hashes de la app de aparatos en Streamlit Secrets, "
            "dentro de [auth.passwords]."
        )
        if not configured_passwords:
            return None

    st.subheader("🔐 Acceso requerido")
    st.caption(
        "Selecciona tu usuario e ingresa la misma contraseña de Control de aparatos. "
        "Después de entrar quedará recordado en el link."
    )
    with st.form("aligners_login_form"):
        username = st.selectbox("Usuario", list(configured_passwords))
        password = st.text_input("Contraseña", type="password")
        submitted = st.form_submit_button("🔓 Entrar y guardar usuario en link")
    if submitted:
        if login_user(username, password):
            st.rerun()
        st.session_state["aligners_login_error"] = "Usuario o contraseña incorrectos."
    if st.session_state.get("aligners_login_error"):
        st.error(st.session_state["aligners_login_error"])
    return None


def _get_service_account_credentials() -> Credentials:
    credentials_text = st.secrets["gsheets"]["google_credentials"]
    credentials_info = json.loads(credentials_text)
    return Credentials.from_service_account_info(credentials_info, scopes=SCOPE)


def _get_gs_client():
    return gspread.authorize(_get_service_account_credentials())


def get_service_account_email() -> str:
    """Correo de la cuenta de servicio con la que se comparten Sheets y Forms."""

    try:
        return clean_cell(
            json.loads(st.secrets["gsheets"]["google_credentials"]).get("client_email", "")
        ).strip() or "la cuenta de servicio"
    except Exception:
        return "la cuenta de servicio"


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
def get_spreadsheet_by_id(sheet_id: str):
    """Abre cualquier Google Sheet compartido con el mismo service account."""

    client = _get_gs_client()
    return run_gsheets_request(lambda: client.open_by_key(sheet_id))


@st.cache_resource
def get_worksheet(sheet_name: str):
    spreadsheet = get_spreadsheet()
    try:
        return run_gsheets_request(lambda: spreadsheet.worksheet(sheet_name))
    except gspread.WorksheetNotFound:
        if sheet_name not in ORDER_TIME_SHEETS.values():
            raise
        return run_gsheets_request(
            lambda: spreadsheet.add_worksheet(
                title=sheet_name,
                rows="2000",
                cols=str(len(TIMES_HEADERS)),
            )
        )


def current_order_sheet() -> str:
    """El destino pertenece a la sesión; nunca cambia globals compartidos."""
    value = st.session_state.get("aligners_order_sheet", SHEET_ORDERS)
    return value if value in ORDER_TIME_SHEETS else SHEET_ORDERS


@contextmanager
def order_sheet_scope(sheet_name: str) -> Iterator[None]:
    """Trabaja temporalmente sobre otra hoja (lecturas, ficha, altas y guardados).

    La vista general lo usa para que cada orden se lea y se guarde en su hoja
    de origen con las mismas funciones que su pestaña individual.
    """
    previous = st.session_state.get("aligners_order_sheet")
    st.session_state["aligners_order_sheet"] = sheet_name
    try:
        yield
    finally:
        if previous is None:
            st.session_state.pop("aligners_order_sheet", None)
        else:
            st.session_state["aligners_order_sheet"] = previous


def current_times_sheet() -> str:
    return ORDER_TIME_SHEETS[current_order_sheet()]


def can_change_status_here(current_user: str) -> bool:
    """Juan puede cambiar la etapa en Polanco y AERIS; el resto sigue ``can_edit_orders``."""
    return can_change_status(current_user, current_order_sheet())


def status_only_here(current_user: str) -> bool:
    return status_only(current_user, current_order_sheet())


def tracking_key(key: str) -> str:
    """Aísla tablas, filtros, borradores y altas de cada seguimiento."""
    prefix = {SHEET_POLANCO: "polanco_", SHEET_AERIS: "aeris_"}.get(current_order_sheet(), "")
    if prefix and not key.startswith(prefix):
        return prefix + key
    return key


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
def read_order_values(sheet_name: str = SHEET_ORDERS) -> list[list[str]]:
    return run_gsheets_request(lambda: get_worksheet(sheet_name).get_all_values())


@st.cache_data(ttl=30)
def read_process_values() -> list[list[str]]:
    return run_gsheets_request(lambda: get_worksheet(SHEET_PROCESSES).get_all_values())


@st.cache_data(ttl=30)
def read_times_values(sheet_name: str = SHEET_TIMES) -> list[list[str]]:
    return run_gsheets_request(lambda: get_worksheet(sheet_name).get_all_values())


def clear_sheet_data_cache() -> None:
    """Invalida sólo lecturas de Sheets y conserva validaciones estables."""
    read_order_values.clear(current_order_sheet())
    read_process_values.clear()
    read_times_values.clear(current_times_sheet())
    read_forms_responses_df.clear()
    read_form_sections.clear()


def get_aligners_forms_secret_value(form_key: str, key: str, default: Any = "") -> Any:
    """Lee un override opcional de un formulario de alineadores desde st.secrets.

    Soporta [google_forms_alineadores.<form_key>].<key> como tabla anidada.
    """

    if not hasattr(st, "secrets"):
        return default

    forms_config = st.secrets.get("google_forms_alineadores", {})
    if hasattr(forms_config, "get"):
        form_section = forms_config.get(form_key, {})
        if hasattr(form_section, "get"):
            value = form_section.get(key, "")
            if clean_cell(value).strip():
                return value

    return default


def get_aligners_form_config(form: dict[str, str]) -> dict[str, str]:
    """Combina el sheet_id/worksheet por defecto con overrides opcionales en secrets."""

    sheet_id = clean_cell(
        get_aligners_forms_secret_value(form["key"], "sheet_id", form["default_sheet_id"])
    ).strip() or form["default_sheet_id"]
    worksheet_name = clean_cell(
        get_aligners_forms_secret_value(form["key"], "worksheet", DEFAULT_FORMS_WORKSHEET)
    ).strip() or DEFAULT_FORMS_WORKSHEET
    form_id = clean_cell(
        get_aligners_forms_secret_value(form["key"], "form_id", form.get("default_form_id", ""))
    ).strip()
    return {
        "key": form["key"],
        "label": form["label"],
        "sheet_id": sheet_id,
        "worksheet": worksheet_name,
        "form_id": form_id,
    }


@st.cache_data(ttl=30)
def read_forms_responses_df(sheet_id: str, worksheet_name: str) -> pd.DataFrame:
    """Lee la hoja de respuestas de un Google Form como DataFrame."""

    if not sheet_id:
        return pd.DataFrame()
    spreadsheet = get_spreadsheet_by_id(sheet_id)
    candidate_names = [worksheet_name, *FORMS_WORKSHEET_FALLBACKS]
    worksheet = None
    tried_names: list[str] = []
    for candidate_name in dict.fromkeys(
        name for name in candidate_names if clean_cell(name).strip()
    ):
        tried_names.append(candidate_name)
        try:
            worksheet = run_gsheets_request(lambda: spreadsheet.worksheet(candidate_name))
            break
        except gspread.WorksheetNotFound:
            continue
    if worksheet is None:
        worksheets = run_gsheets_request(lambda: spreadsheet.worksheets())
        if not worksheets:
            return pd.DataFrame()
        worksheet = worksheets[0]
        st.info(
            "No encontré las pestañas de Forms esperadas "
            f"({', '.join(tried_names)}). Usaré la primera pestaña: {worksheet.title}."
        )

    values = run_gsheets_request(lambda: worksheet.get_all_values())
    if not values:
        return pd.DataFrame()

    headers = ensure_unique_column_names(values[0])
    width = len(headers)
    rows = [row[:width] + [""] * max(width - len(row), 0) for row in values[1:]]
    df = pd.DataFrame(rows, columns=headers)
    if df.empty:
        return df

    non_empty_rows = df.apply(
        lambda row: any(clean_cell(value).strip() for value in row),
        axis=1,
    )
    return df[non_empty_rows].reset_index(drop=True)


def forms_title_key(value: Any) -> str:
    """Compara títulos de pregunta sin importar acentos, mayúsculas ni saltos de línea."""

    return re.sub(r"\s+", " ", normalize_text(value)).strip()


def parse_form_sections(form: dict[str, Any]) -> list[dict[str, Any]]:
    """Convierte la respuesta de Forms API en secciones con sus preguntas en orden.

    Las cuadrículas llegan al Sheet como una columna por fila con el formato
    ``Pregunta [Fila]``, así que se expanden igual.
    """

    sections: list[dict[str, Any]] = [{"title": FORMS_FIRST_SECTION_TITLE, "questions": []}]
    for item in form.get("items", []) or []:
        title = clean_cell(item.get("title", "")).strip()
        if "pageBreakItem" in item:
            sections.append({"title": title or f"Sección {len(sections) + 1}", "questions": []})
        elif "questionItem" in item:
            sections[-1]["questions"].append(title)
        elif "questionGroupItem" in item:
            for row in item["questionGroupItem"].get("questions", []) or []:
                row_title = clean_cell(row.get("rowQuestion", {}).get("title", "")).strip()
                sections[-1]["questions"].append(f"{title} [{row_title}]")
    return [section for section in sections if section["questions"]]


@st.cache_data(ttl=FORMS_FORM_STRUCTURE_TTL_SECONDS, show_spinner=False)
def read_form_sections(form_id: str) -> tuple[list[dict[str, Any]], str]:
    """Lee las secciones y preguntas actuales del Google Form.

    Devuelve ``(secciones, error)``. El error también se cachea para no repetir
    la llamada en cada rerun mientras el formulario no esté compartido.
    """

    if not form_id:
        return [], "No hay form_id configurado."
    try:
        session = AuthorizedSession(_get_service_account_credentials())
        response = session.get(FORMS_API_URL.format(form_id=form_id), timeout=20)
    except Exception as exc:
        return [], str(exc)
    if not response.ok:
        try:
            message = response.json().get("error", {}).get("message", "")
        except ValueError:
            message = ""
        return [], f"{response.status_code} {message or response.reason}".strip()
    return parse_form_sections(response.json()), ""


def map_forms_columns(
    columns: list[str], sections: list[dict[str, Any]]
) -> dict[str, tuple[int, int]]:
    """Ubica cada columna del Sheet en la (sección, pregunta) actual del formulario.

    Las columnas sin pregunta equivalente (preguntas borradas, metadatos) no
    aparecen en el resultado. Si un título se repite, ``ensure_unique_column_names``
    le agregó ``_2``, ``_3``...; esas columnas se reparten en orden entre las
    preguntas con ese título y las sobrantes van a la última.
    """

    positions: dict[str, list[tuple[int, int]]] = {}
    for section_index, section in enumerate(sections):
        for question_index, title in enumerate(section["questions"]):
            positions.setdefault(forms_title_key(title), []).append(
                (section_index, question_index)
            )

    column_set = set(columns)
    used: dict[str, int] = {}
    mapping: dict[str, tuple[int, int]] = {}
    for column in columns:
        key = forms_title_key(column)
        if key not in positions:
            base = re.sub(r"_\d+$", "", column)
            if base != column and base in column_set:
                key = forms_title_key(base)
        candidates = positions.get(key)
        if not candidates:
            continue
        index = used.get(key, 0)
        mapping[column] = candidates[min(index, len(candidates) - 1)]
        used[key] = index + 1
    return mapping


def order_forms_columns(
    columns: list[str], mapping: dict[str, tuple[int, int]]
) -> list[str]:
    """Ordena las columnas de la tabla como el formulario actual."""

    leading = [
        column
        for column in columns
        if column not in mapping
        and (column == "Respuesta #" or forms_title_key(column) in FORMS_METADATA_HEADERS)
    ]
    placed = sorted((column for column in columns if column in mapping), key=mapping.__getitem__)
    others = [column for column in columns if column not in mapping and column not in leading]
    return leading + placed + others


def group_forms_details(
    details: list[tuple[str, str]],
    sections: list[dict[str, Any]],
    mapping: dict[str, tuple[int, int]],
) -> list[tuple[str, list[tuple[str, str]]]]:
    """Agrupa las respuestas por sección con el título actual de cada pregunta.

    Sin estructura del formulario se conserva el orden de columnas del Sheet.
    """

    if not details:
        return []
    if not sections:
        return [(FORMS_FIRST_SECTION_TITLE, list(details))]

    leading: list[tuple[str, str]] = []
    others: list[tuple[str, str]] = []
    placed: dict[int, list[tuple[int, int, tuple[str, str]]]] = {}
    for order, (column, value) in enumerate(details):
        position = mapping.get(column)
        if position is not None:
            section_index, question_index = position
            label = sections[section_index]["questions"][question_index]
            placed.setdefault(section_index, []).append((question_index, order, (label, value)))
        elif forms_title_key(column) in FORMS_METADATA_HEADERS:
            leading.append((column, value))
        else:
            others.append((column, value))

    groups = [
        (section["title"], [entry for _, _, entry in sorted(placed[section_index])])
        for section_index, section in enumerate(sections)
        if section_index in placed
    ]
    if leading:
        if groups:
            groups[0] = (groups[0][0], leading + groups[0][1])
        else:
            groups.append((FORMS_FIRST_SECTION_TITLE, leading))
    if others:
        groups.append((FORMS_OTHER_ANSWERS_TITLE, others))
    return groups


def should_hide_forms_column(column: str) -> bool:
    """Oculta columnas legales/informativas que no aportan a la revisión técnica."""

    normalized_column = normalize_text(column)
    return any(
        normalize_text(hint) in normalized_column for hint in FORMS_EXCLUDED_COLUMN_HINTS
    )


def find_column_by_hint(columns: list[str], *hints: str) -> str:
    """Encuentra una columna comparando texto normalizado por fragmentos."""

    normalized_columns = [(column, normalize_text(column)) for column in columns]
    for hint in hints:
        normalized_hint = normalize_text(hint)
        for column, normalized_column in normalized_columns:
            if normalized_hint and normalized_hint in normalized_column:
                return column
    return ""


def build_forms_review_df(forms_df: pd.DataFrame) -> pd.DataFrame:
    """Muestra todas las respuestas útiles de Forms, excluyendo solo textos legales."""

    if forms_df.empty:
        return forms_df

    visible_columns = [
        column for column in forms_df.columns if not should_hide_forms_column(column)
    ]
    review_df = forms_df[visible_columns].copy()
    review_df.insert(0, "Respuesta #", [index + 2 for index in range(len(review_df))])
    return review_df


def is_forms_file_column(column: str, values: pd.Series) -> bool:
    """Dice si la columna es una pregunta de subida de archivos de Google Forms.

    Se reconoce por el título o, si el título no coincide, porque todas sus
    respuestas son solo links de archivos subidos a Forms.
    """

    normalized_column = normalize_text(column)
    if any(normalize_text(hint) in normalized_column for hint in FORMS_FILE_COLUMN_HINTS):
        return True
    answers = [answer for value in values if (answer := clean_cell(value).strip())]
    return bool(answers) and all(FORMS_UPLOAD_ANSWER_RE.fullmatch(answer) for answer in answers)


def get_forms_file_columns(review_df: pd.DataFrame) -> list[str]:
    """Columnas con archivos adjuntos, en el orden del Sheet."""

    return [
        column
        for column in review_df.columns
        if column != "Respuesta #" and is_forms_file_column(column, review_df[column])
    ]


def get_forms_row_file_links(row: pd.Series, file_columns: list[str]) -> list[str]:
    """Junta, sin repetir, los links de todas las columnas de archivos de una respuesta."""

    links: list[str] = []
    for column in file_columns:
        for link in get_forms_file_links(row.get(column, "")):
            if link not in links:
                links.append(link)
    return links


def get_forms_file_links(value: Any) -> list[str]:
    """Separa y valida los enlaces de archivos enviados por Google Forms."""

    raw_value = clean_cell(value).strip()
    if not raw_value:
        return []

    links = []
    for candidate in re.split(r"(?:\r?\n|\\n|[,;])\s*", raw_value):
        link = candidate.strip()
        if link.startswith(("https://", "http://")) and link not in links:
            links.append(link)
    return links


def get_forms_field_icon(column: str) -> str:
    """Devuelve un icono específico para que la ficha de Forms no se vea repetitiva."""

    normalized = normalize_text(column)
    icon_rules = [
        (("MARCA", "FECHA", "HORA", "TEMPORAL"), "🕒"),
        (("DOCTOR", "TRATANTE", "MEDICO"), "🩺"),
        (("WHATSAPP", "TELEFONO", "CONTACTO"), "💬"),
        (("PACIENTE",), "🙂"),
        (("APARATO", "PRODUCTO", "SOLICITAR", "OPCIONES"), "🦷"),
        (("ARCHIVO", "STL", "DICOM", "DRIVE", "ADJUNTAR"), "📎"),
        (("OBSERV", "COMENT", "DETALLE", "INDICACION"), "📝"),
        (("DIRECCION", "ENVIO", "DOMICILIO"), "📍"),
        (("CORREO", "EMAIL", "MAIL"), "✉️"),
    ]
    for keywords, icon in icon_rules:
        if any(keyword in normalized for keyword in keywords):
            return icon
    return "🔹"


def apply_single_row_selection_to_selectbox(
    event: Any, df: pd.DataFrame, id_column: str, selectbox_key: str
) -> None:
    """Sincroniza una fila seleccionada en una tabla con su selectbox asociado."""

    selected_rows = getattr(getattr(event, "selection", None), "rows", [])
    if not selected_rows:
        return
    selected_position = selected_rows[0]
    if selected_position >= len(df.index):
        return
    raw_selected_id = df.iloc[selected_position].get(id_column, "")
    selected_id = clean_cell(raw_selected_id).strip()
    if selected_id:
        st.session_state[selectbox_key] = raw_selected_id


def build_form_response_pdf(
    *,
    form_label: str,
    response_number: Any,
    sections: list[tuple[str, list[tuple[str, str]]]],
    links: list[str],
) -> bytes:
    """Genera un PDF legible con todos los datos de una respuesta de Forms.

    ``sections`` llega ya agrupado y ordenado como el formulario actual.
    """

    # Helvetica no tiene emojis: el del nombre de la pestaña salía como un cuadro.
    form_label = strip_visual_prefix(form_label) or form_label
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=letter,
        topMargin=1.6 * cm,
        bottomMargin=1.6 * cm,
        leftMargin=1.8 * cm,
        rightMargin=1.8 * cm,
        title=f"{form_label} - Respuesta {response_number}",
    )

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "AlignersPdfTitle",
        parent=styles["Title"],
        alignment=TA_LEFT,
        textColor=colors.HexColor("#123B2C"),
        fontSize=18,
        spaceAfter=2,
    )
    subtitle_style = ParagraphStyle(
        "AlignersPdfSubtitle",
        parent=styles["Normal"],
        textColor=colors.HexColor("#5C7568"),
        fontSize=10,
        spaceAfter=14,
    )
    section_style = ParagraphStyle(
        "AlignersPdfSection",
        parent=styles["Heading2"],
        textColor=colors.white,
        backColor=colors.HexColor("#12875E"),
        fontSize=12,
        leading=18,
        spaceBefore=16,
        spaceAfter=8,
        leftIndent=6,
        borderPadding=(4, 6, 4, 6),
    )
    label_style = ParagraphStyle(
        "AlignersPdfLabel",
        parent=styles["Normal"],
        fontSize=8.5,
        textColor=colors.HexColor("#3F6B54"),
        fontName="Helvetica-Bold",
    )
    value_style = ParagraphStyle(
        "AlignersPdfValue",
        parent=styles["Normal"],
        fontSize=10,
        textColor=colors.HexColor("#1B4B36"),
        leading=13,
    )
    link_style = ParagraphStyle(
        "AlignersPdfLink",
        parent=styles["Normal"],
        fontSize=9.5,
        textColor=colors.HexColor("#0E8358"),
        leading=14,
    )

    story = [
        Paragraph(html.escape(form_label), title_style),
        Paragraph(
            f"Respuesta #{html.escape(str(response_number))} · ARTTDLAB / Control de alineadores",
            subtitle_style,
        ),
        HRFlowable(width="100%", thickness=1, color=colors.HexColor("#BFE3CC"), spaceAfter=6),
    ]

    for section_title, details in sections:
        story.append(Paragraph(html.escape(str(section_title)), section_style))
        rows = [
            [
                Paragraph(html.escape(str(field_label)), label_style),
                Paragraph(html.escape(str(field_value)).replace("\n", "<br/>"), value_style),
            ]
            for field_label, field_value in details
        ]
        table = Table(rows, colWidths=[4.5 * cm, 11.2 * cm], repeatRows=0)
        table.setStyle(
            TableStyle(
                [
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("TOPPADDING", (0, 0), (-1, -1), 7),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
                    ("LINEBELOW", (0, 0), (-1, -1), 0.5, colors.HexColor("#D7EDDD")),
                    ("ROWBACKGROUNDS", (0, 0), (-1, -1), [colors.white, colors.HexColor("#F7FCF9")]),
                ]
            )
        )
        story.append(table)
    if not sections:
        story.append(Paragraph(FORMS_FIRST_SECTION_TITLE, section_style))
        story.append(Paragraph("Esta respuesta no tiene datos adicionales visibles.", value_style))

    story.append(Paragraph("Archivos adjuntos", section_style))
    if links:
        for index, link in enumerate(links, start=1):
            safe_link = html.escape(link, quote=True)
            story.append(
                Paragraph(
                    f'{index}. <a href="{safe_link}" color="#0E8358">{safe_link}</a>',
                    link_style,
                )
            )
            story.append(Spacer(1, 3))
    else:
        story.append(Paragraph("Esta respuesta no trae archivos adjuntos detectados.", value_style))

    doc.build(story)
    return buffer.getvalue()


def first_forms_answer(row: pd.Series, columns: list[str], hints: tuple[str, ...]) -> str:
    """Primera respuesta no vacía entre las columnas cuyo título coincide con algún hint.

    Un formulario puede repetir la pregunta en varias secciones (Marca Blanca
    pide el paciente en la sección de cada paquete), así que se revisan todas.
    """

    normalized_hints = [normalize_text(hint) for hint in hints]
    for column in columns:
        normalized_column = normalize_text(column)
        if any(hint in normalized_column for hint in normalized_hints):
            value = clean_cell(row.get(column, "")).strip()
            if value:
                return value
    return ""


def load_forms_inbox_sources(
    forms: list[dict[str, str]],
) -> tuple[list[dict[str, Any]], list[str]]:
    """Lee las respuestas de cada formulario.

    Devuelve ``(fuentes, problemas)``: un formulario sin configurar o que no se
    pudo leer queda fuera con su motivo, sin tapar las respuestas de los demás.
    """

    sources: list[dict[str, Any]] = []
    problems: list[str] = []
    for form in forms:
        if not form["sheet_id"]:
            problems.append(
                f"{form['label']}: configura su `sheet_id` en "
                f"`[google_forms_alineadores.{form['key']}]` en secrets."
            )
            continue
        try:
            review_df = build_forms_review_df(
                read_forms_responses_df(form["sheet_id"], form["worksheet"])
            )
        except Exception as exc:
            problems.append(
                f"{form['label']}: no pude leer su Google Sheet de respuestas. Verifica que "
                f"`{get_service_account_email()}` tenga acceso. Detalle: {exc}"
            )
            continue
        sources.append({
            "form": form,
            "review_df": review_df,
            "file_columns": get_forms_file_columns(review_df),
        })
    return sources, problems


def build_forms_inbox_df(sources: list[dict[str, Any]]) -> pd.DataFrame:
    """Una fila por respuesta de todos los formularios, de la más nueva a la más vieja."""

    rows = []
    for source in sources:
        form, review_df = source["form"], source["review_df"]
        if review_df.empty:
            continue
        columns = list(review_df.columns)
        stamp = forms_notices.timestamp_column(columns)
        new_numbers = forms_notices.new_response_numbers(review_df, form["key"])
        for _, row in review_df.iterrows():
            number = int(row["Respuesta #"])
            timestamp = clean_cell(row.get(stamp, "")).strip() if stamp else ""
            rows.append({
                FORMS_INBOX_ID_COLUMN: f"{form['key']}:{number}",
                FORMS_INBOX_FORM_COLUMN: form["key"],
                FORMS_INBOX_KEY_COLUMN: forms_notices.response_key(timestamp),
                "Nueva": "🆕" if number in new_numbers else "",
                "Formulario": form["label"],
                "Respuesta #": number,
                "Marca temporal": timestamp,
                "Paciente": first_forms_answer(row, columns, FORMS_INBOX_PATIENT_HINTS),
                "Doctor": first_forms_answer(row, columns, FORMS_INBOX_DOCTOR_HINTS),
                "Asesor": first_forms_answer(row, columns, FORMS_INBOX_ADVISOR_HINTS),
                "Archivos": len(get_forms_row_file_links(row, source["file_columns"])),
            })
    if not rows:
        return pd.DataFrame()
    # Sin marca temporal reconocible ("") quedan al final.
    return (
        pd.DataFrame(rows)
        .sort_values([FORMS_INBOX_KEY_COLUMN, "Respuesta #"], ascending=False, kind="stable")
        .reset_index(drop=True)
    )


def focus_forms_inbox(inbox_df: pd.DataFrame, form_keys: set[str]) -> pd.DataFrame:
    """Filas visibles de la tabla única y respuesta elegida en el selector.

    - Recupera la respuesta que el usuario tenía elegida antes de cambiar de pestaña.
    - Deja seleccionada la que pidió un aviso de la campana, buscándola por
      formulario y marca temporal (no por número de fila).
    - Muestra las más recientes y, al final, las nuevas o la elegida que no
      entren entre ellas: así siempre se pueden abrir.
    """

    selector_key = FORMS_INBOX_SELECTOR_KEY
    remembered = st.session_state.get(forms_notices.SELECTIONS_KEY, {}).get(selector_key)
    if selector_key not in st.session_state and remembered is not None:
        st.session_state[selector_key] = remembered
    focus = st.session_state.get(forms_notices.FOCUS_KEY, {})
    for form_key in sorted(form_keys & set(focus if isinstance(focus, dict) else ())):
        key = focus.pop(form_key)
        matches = inbox_df[
            (inbox_df[FORMS_INBOX_FORM_COLUMN] == form_key)
            & (inbox_df[FORMS_INBOX_KEY_COLUMN] == key)
        ] if key else inbox_df.iloc[0:0]
        if not matches.empty:
            st.session_state[selector_key] = matches.iloc[-1][FORMS_INBOX_ID_COLUMN]
            versions = st.session_state.setdefault(forms_notices.TABLE_VERSIONS_KEY, {})
            versions[FORMS_INBOX_TABLE_KEY] = versions.get(FORMS_INBOX_TABLE_KEY, 0) + 1

    wanted = set(inbox_df.loc[inbox_df["Nueva"] != "", FORMS_INBOX_ID_COLUMN])
    if st.session_state.get(selector_key) is not None:
        wanted.add(st.session_state[selector_key])
    recent = inbox_df.head(FORMS_INBOX_RECENT_ROWS)
    missing = wanted - set(recent[FORMS_INBOX_ID_COLUMN])
    extra = inbox_df.iloc[FORMS_INBOX_RECENT_ROWS:]
    extra = extra[extra[FORMS_INBOX_ID_COLUMN].isin(missing)]
    return pd.concat([recent, extra], ignore_index=True)


def render_forms_response_detail(source: dict[str, Any], selected_row: pd.Series) -> None:
    """Ficha de una respuesta: resumen, PDF, datos capturados y archivos de Drive."""

    form, review_df, file_columns = source["form"], source["review_df"], source["file_columns"]
    response_number = int(selected_row["Respuesta #"])
    forms_notices.record_viewed(
        form["key"], selected_row.get(forms_notices.timestamp_column(review_df.columns), "")
    )
    form_sections, form_error = read_form_sections(form.get("form_id", ""))
    column_positions = map_forms_columns(list(review_df.columns), form_sections)
    if form_error:
        st.caption(
            "ℹ️ No pude leer el formulario actual en Google Forms, así que el PDF y la "
            "ficha siguen el orden de columnas del Sheet. Comparte el formulario como "
            f"editor con `{get_service_account_email()}` y habilita Google Forms API "
            f"en su proyecto de Google Cloud. Detalle: {form_error}"
        )

    selected_links = get_forms_row_file_links(selected_row, file_columns)
    visible_details = []
    for column in order_forms_columns(list(review_df.columns), column_positions):
        if column == "Respuesta #" or column in file_columns:
            continue
        value = clean_cell(selected_row.get(column, "")).strip()
        if value:
            visible_details.append((column, value))
    detail_groups = group_forms_details(visible_details, form_sections, column_positions)
    visible_details = [detail for _, details in detail_groups for detail in details]

    link_pill_class = "align-forms-pill-link" if selected_links else "align-forms-pill-missing"
    link_pill_text = (
        f"✅ {len(selected_links)} archivo{'s' if len(selected_links) != 1 else ''} listo"
        f"{'s' if len(selected_links) != 1 else ''}"
        if selected_links
        else "⚠️ Sin link detectado"
    )
    st.markdown(
        f"<div class='align-forms-hero'>"
        f"<div class='align-forms-hero-title'>📌 Resumen rápido de la respuesta seleccionada</div>"
        f"<div style='margin-top:10px;'>"
        f"<span class='align-forms-pill align-forms-pill-form'>{html.escape(form['label'])}</span>"
        f"<span class='align-forms-pill align-forms-pill-id'>🧾 Respuesta #{response_number}</span>"
        f"<span class='align-forms-pill {link_pill_class}'>{link_pill_text}</span>"
        f"</div></div>",
        unsafe_allow_html=True,
    )

    pdf_bytes = build_form_response_pdf(
        form_label=form["label"],
        response_number=response_number,
        sections=detail_groups,
        links=selected_links,
    )
    with st.container(key=f"align_forms_pdf_{form['key']}"):
        st.download_button(
            "📄 Descargar PDF de esta respuesta",
            data=pdf_bytes,
            file_name=f"{form['key']}_respuesta_{response_number}.pdf",
            mime="application/pdf",
            key=f"aligners_forms_pdf_button_{form['key']}_{response_number}",
        )

    if visible_details:
        cols = st.columns(2)
        for idx, (column, value) in enumerate(visible_details[:12]):
            icon = get_forms_field_icon(column)
            safe_column = html.escape(str(column))
            safe_value = html.escape(str(value))
            with cols[idx % 2]:
                st.markdown(
                    f"<div class='align-forms-detail'><div class='align-forms-label'>{icon} {safe_column}</div>"
                    f"<div class='align-forms-value'>{safe_value}</div></div>",
                    unsafe_allow_html=True,
                )
    else:
        st.info("La respuesta seleccionada no tiene detalles adicionales visibles.")

    if selected_links:
        st.markdown("**🔗 Archivos de Drive detectados en Forms:**")
        links_html = "".join(
            f"<li><a href='{html.escape(link, quote=True)}' target='_blank' "
            f"rel='noopener noreferrer'>Archivo {index}</a>"
            f"<div class='align-forms-link-url'>{html.escape(link)}</div></li>"
            for index, link in enumerate(selected_links, start=1)
        )
        st.markdown(
            "<ol class='align-forms-links-list'>" + links_html + "</ol>",
            unsafe_allow_html=True,
        )
    else:
        st.warning(
            "Esta respuesta no trae archivos en ninguna pregunta de subida de archivos."
        )


def render_aligners_forms_inbox(forms: list[dict[str, str]]) -> None:
    """Tabla única con las respuestas de todos los formularios y la ficha de la elegida."""

    sources, problems = load_forms_inbox_sources(forms)
    for problem in problems:
        st.caption(f"ℹ️ {problem}")
    inbox_df = build_forms_inbox_df(sources)
    if inbox_df.empty:
        st.info("No hay respuestas de Google Forms para mostrar todavía.")
        return

    display_df = focus_forms_inbox(inbox_df, {source["form"]["key"] for source in sources})
    new_rows = display_df[display_df["Nueva"] != ""]
    if not new_rows.empty:
        plural = "s" if len(new_rows) != 1 else ""
        st.info(
            f"**{len(new_rows)} respuesta{plural} nueva{plural} que todavía no abres** "
            "(marcadas con 🆕 en la tabla): "
            + ", ".join(
                f"{row['Formulario']} #{row['Respuesta #']}" for _, row in new_rows.iterrows()
            ),
            icon="🆕",
        )
    st.caption(
        "Elige una respuesta para ver su ficha con los datos capturados y los "
        "archivos de Drive adjuntos."
    )
    table_event = st.dataframe(
        display_df,
        width="stretch",
        hide_index=True,
        column_order=FORMS_INBOX_TABLE_COLUMNS,
        column_config={
            "Nueva": st.column_config.TextColumn("🆕", width="small"),
            "Respuesta #": st.column_config.NumberColumn("Respuesta #", format="%d"),
            "Archivos": st.column_config.NumberColumn("📎 Archivos", format="%d"),
        },
        on_select="rerun",
        selection_mode="single-row",
        key=forms_notices.table_widget_key(FORMS_INBOX_TABLE_KEY),
    )
    apply_single_row_selection_to_selectbox(
        table_event, display_df, FORMS_INBOX_ID_COLUMN, FORMS_INBOX_SELECTOR_KEY
    )

    rows_by_id = {row[FORMS_INBOX_ID_COLUMN]: row for _, row in display_df.iterrows()}
    response_options = list(rows_by_id)
    if st.session_state.get(FORMS_INBOX_SELECTOR_KEY) not in rows_by_id:
        st.session_state[FORMS_INBOX_SELECTOR_KEY] = response_options[0]
    # Sin 🆕 en el texto: si cambia entre reruns, Streamlit pierde la opción elegida.
    selected_id = st.selectbox(
        "📌 Selecciona una respuesta",
        options=response_options,
        format_func=lambda option: " · ".join(
            part for part in (
                rows_by_id[option]["Formulario"],
                f"#{rows_by_id[option]['Respuesta #']}",
                rows_by_id[option]["Paciente"],
            ) if part
        ),
        key=FORMS_INBOX_SELECTOR_KEY,
    )
    forms_notices.remember_selection(FORMS_INBOX_SELECTOR_KEY, selected_id)
    inbox_row = rows_by_id[selected_id]
    source = next(
        source for source in sources
        if source["form"]["key"] == inbox_row[FORMS_INBOX_FORM_COLUMN]
    )
    review_df = source["review_df"]
    selected_rows = review_df[review_df["Respuesta #"] == inbox_row["Respuesta #"]]
    if not selected_rows.empty:
        render_forms_response_detail(source, selected_rows.iloc[0])


def render_aligners_forms_tab() -> None:
    """Respuestas de todos los formularios de prescripción en una sola tabla."""

    st.subheader(FORMS_TAB_LABEL)
    st.caption(
        f"Respuestas y archivos de los {len(ALIGNERS_FORMS)} formularios de prescripción, leídos "
        "directamente de sus Google Sheets con la cuenta de servicio compartida. La columna "
        "**Formulario** dice de cuál viene cada respuesta."
    )
    render_aligners_forms_inbox([get_aligners_form_config(form) for form in ALIGNERS_FORMS])


def rerun_active_tab() -> None:
    """Recalcula la pestaña activa; fuera de un fragmento recarga la app completa."""
    try:
        st.rerun(scope="fragment")
    except StreamlitAPIException:
        st.rerun()


def read_orders_df(sheet_name: str | None = None) -> pd.DataFrame:
    """Pedidos de la hoja activa, o de ``sheet_name`` (tablero y buscador)."""
    values = read_order_values(sheet_name or current_order_sheet())
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


def read_times_df(sheet_name: str | None = None) -> pd.DataFrame:
    values = read_times_values(sheet_name or current_times_sheet())
    if not values:
        return pd.DataFrame(columns=TIMES_HEADERS)
    headers = [canonical_column_name(item) for item in ensure_unique_column_names(values[0])]
    rows = normalize_rows(values[1:], headers)
    frame = pd.DataFrame(rows, columns=headers)
    times_sheet = sheet_name or current_times_sheet()
    order_sheet = next((orders for orders, times in ORDER_TIME_SHEETS.items()
                        if times == times_sheet), SHEET_ORDERS)
    return canonical_times_df(frame, read_orders_df(order_sheet).get(ID_COLUMN, []))


def read_process_definitions() -> dict[str, ProcessDefinition]:
    return parse_process_matrix(read_process_values())


def ensure_times_headers() -> None:
    _ensure_times_headers(current_times_sheet())


@st.cache_data(ttl=3600)
def _ensure_times_headers(sheet_name: str) -> None:
    worksheet = get_worksheet(sheet_name)
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

    worksheet = get_worksheet(current_times_sheet())
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
    resolve_id = identifier_resolver(read_orders_df().get(ID_COLUMN, []))
    for row_number, row in enumerate(values[1:], start=2):
        id_position = positions[ID_COLUMN]
        end_position = positions["FECHA_FIN"]
        row_identifier = row[id_position - 1] if id_position and id_position <= len(row) else ""
        date_finished = row[end_position - 1] if end_position and end_position <= len(row) else ""
        if resolve_id(row_identifier) != clean_cell(identifier).strip() or clean_cell(date_finished).strip():
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
    """Cierra la etapa anterior y abre la medición de la hoja activa."""

    if (current_user in VIEW_AND_CREATE_USERS and clean_cell(previous_status).strip()
            and not can_change_status_here(current_user)):
        raise ValueError("Tu usuario no tiene permiso para modificar pedidos existentes.")
    ensure_times_headers()
    configured = configured_statuses(definitions)
    previous = canonical_status(previous_status, configured)
    new = canonical_status(new_status, configured)
    previous_resume = close_active_times(identifier, new)
    clear_sheet_data_cache()

    worksheet = get_worksheet(current_times_sheet())
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
    # Do not retry an ambiguous append: it could create two active timers.
    worksheet.append_row(
        [prepare_sheet_value(row.get(canonical_column_name(header), "")) for header in headers],
        value_input_option="RAW", insert_data_option="INSERT_ROWS",
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
    worksheet = get_worksheet(current_order_sheet())
    values = run_gsheets_request(lambda: worksheet.get_all_values())
    if len(values) < ORDER_HEADER_ROW:
        result["error"] = f"No encontré la fila 2 de encabezados en {current_order_sheet()}."
        return result
    headers = ensure_unique_column_names(values[ORDER_HEADER_ROW - 1])
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

    formula_totals = [column for column in changes if shipment_field_kind(column) == "total"]
    if formula_totals:
        # La app calcula los Totales de envíos; si la hoja ya los calcula con
        # fórmula, se conserva la fórmula en vez de escribir el número encima.
        try:
            formulas = run_gsheets_request(
                lambda: worksheet.row_values(row_number, value_render_option="FORMULA")
            )
        except Exception:
            formulas = []
        for column in formula_totals:
            position = get_header_position(headers, column)
            if position and position <= len(formulas) and str(formulas[position - 1] or "").startswith("="):
                changes = {key: value for key, value in changes.items() if key != column}

    updates: list[Cell] = []
    for column, value in changes.items():
        position = get_header_position(headers, column)
        if position is None:
            result["skipped_columns"].append(column)
            continue
        updates.append(Cell(row_number, position, prepare_sheet_value(value)))
        result["updated_columns"].append(column)
    if result["skipped_columns"]:
        result["error"] = "Faltan columnas en la hoja: " + ", ".join(result["skipped_columns"])
        return result
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
# Envíos y alineadores
# ==============================
# La hoja trae el plan de tratamiento (TEMPLATE SUP … Total) y ocho envíos
# repetidos (TEMP. SUP … FECHA ENVÍO). Los encabezados repetidos llegan como
# "NO. ALIN SUP_2", "Total_3"…, así que los bloques se arman por su orden.
SHIPMENT_QUANTITY_KINDS = ("temp_sup", "temp_inf", "alin_sup", "alin_inf")
SHIPMENT_FIELD_LABELS = {
    "temp_sup": "🧩 Template sup.",
    "temp_inf": "🧩 Template inf.",
    "alin_sup": "🦷 Alineadores sup.",
    "alin_inf": "🦷 Alineadores inf.",
    "total": "Σ Total",
    "pago": "💳 Pago impresión",
    "envio": "🚚 Fecha envío",
}
# Valores de texto que Sheets ya usa en las fechas de pago.
PAYMENT_TEXT_VALUES = {"CORTESIA": "🎁 Cortesía", "PENDIENTE": "⏳ Pendiente"}


@dataclass(frozen=True)
class ShipmentBlock:
    """Plan de tratamiento (``number`` 0) o un envío, con su columna por tipo."""

    number: int
    columns: dict[str, str]

    @property
    def title(self) -> str:
        return f"Envío {self.number}" if self.number else "Plan de tratamiento"


def shipment_field_kind(column: Any) -> str:
    base = re.sub(r"_\d+$", "", clean_cell(column)).strip()
    words = re.sub(r"[^A-Z0-9]+", " ", normalize_text(base)).split()
    side = "sup" if "SUP" in words else "inf" if "INF" in words else ""
    if words and words[0].startswith("TEMP") and side:
        return f"temp_{side}"
    if side and any(word.startswith("ALIN") for word in words):
        return f"alin_{side}"
    if words == ["TOTAL"]:
        return "total"
    if words[:3] == ["FECHA", "PAGO", "IMPRESION"]:
        return "pago"
    if words == ["FECHA", "ENVIO"]:
        return "envio"
    return ""


def shipment_layout(columns: Iterable[Any]) -> tuple[ShipmentBlock | None, list[ShipmentBlock]]:
    """Plan de tratamiento y envíos, en el orden de las columnas de la hoja."""
    groups: list[dict[str, str]] = []
    current: dict[str, str] = {}
    for column in columns:
        kind = shipment_field_kind(column)
        if not kind:
            continue
        # Cada bloque abre con su template superior; un tipo repetido también lo cierra.
        if current and (kind in current or kind == "temp_sup"):
            groups.append(current)
            current = {}
        current[kind] = str(column)
    if current:
        groups.append(current)
    groups = [group for group in groups if {"alin_sup", "alin_inf"} & set(group)]
    plan = None
    if groups and not {"pago", "envio"} & set(groups[0]):
        plan = ShipmentBlock(0, groups.pop(0))
    return plan, [ShipmentBlock(number, group) for number, group in enumerate(groups, start=1)]


def block_columns(plan: ShipmentBlock | None, shipments: list[ShipmentBlock]) -> list[str]:
    return [column for block in [plan, *shipments] if block for column in block.columns.values()]


def parse_shipment_quantity(value: Any) -> int | None:
    """Piezas de un campo: un número (7) o un rango de alineadores (1-7 son 7)."""
    text = clean_cell(value).strip()
    if not text:
        return 0
    if re.fullmatch(r"\d+(?:\.0+)?", text):
        return int(float(text))
    match = re.fullmatch(r"(\d+)\s*-\s*(\d+)", text)
    if match:
        return abs(int(match.group(2)) - int(match.group(1))) + 1
    return None


def shipment_total(values: dict[str, Any]) -> str | None:
    """Templates + alineadores; vacío sin datos y ``None`` si hay texto no numérico."""
    texts = [clean_cell(values.get(kind, "")).strip() for kind in SHIPMENT_QUANTITY_KINDS]
    if not any(texts):
        return ""
    quantities = [parse_shipment_quantity(text) for text in texts]
    if any(quantity is None for quantity in quantities):
        return None
    return str(sum(quantities))


def shipment_pieces(block: ShipmentBlock, value_of: Callable[[str], Any]) -> int | None:
    """Piezas del bloque; si hay texto libre se respeta el Total escrito en la hoja."""
    total = shipment_total({kind: value_of(column) for kind, column in block.columns.items()})
    if total is None:
        total = clean_cell(value_of(block.columns.get("total", ""))).strip()
    quantity = parse_shipment_quantity(total)
    return quantity if total else None


def shipment_has_data(block: ShipmentBlock, value_of: Callable[[str], Any]) -> bool:
    return any(clean_cell(value_of(column)).strip() for column in block.columns.values())


def last_registered_shipment(shipments: list[ShipmentBlock], value_of: Callable[[str], Any]) -> int:
    return max((block.number for block in shipments if shipment_has_data(block, value_of)), default=0)


def is_payment_text(column: str, value: Any) -> bool:
    return "PAGO" in normalize_text(column) and normalize_text(value) in PAYMENT_TEXT_VALUES


def shipment_value_errors(identifier: str, columns: Iterable[Any], delta: dict[str, Any]) -> list[str]:
    plan, shipments = shipment_layout(columns)
    return [
        f"{identifier}: {block.title} · {SHIPMENT_FIELD_LABELS[kind].split(' ', 1)[1]} debe ser un número "
        "o un rango como 1-7."
        for block in [plan, *shipments] if block
        for kind, column in block.columns.items()
        if kind in SHIPMENT_QUANTITY_KINDS and column in delta
        and parse_shipment_quantity(delta[column]) is None
    ]


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


def orders_with_status(
    frame: pd.DataFrame,
    definitions: dict[str, ProcessDefinition],
    keep_status: Callable[[str], bool],
) -> pd.DataFrame:
    """Pedidos con folio y datos reales cuyo status cumple ``keep_status``."""
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
        & result[STATUS_COLUMN].map(keep_status).astype(bool)
        & result[meaningful].apply(
            lambda row: any(clean_cell(value).strip() for value in row), axis=1
        )
    )
    return result[mask].reset_index(drop=True)


def active_orders(
    frame: pd.DataFrame, definitions: dict[str, ProcessDefinition]
) -> pd.DataFrame:
    return orders_with_status(
        frame, definitions, lambda status: not is_terminal_status(status)
    )


def sent_orders(
    frame: pd.DataFrame, definitions: dict[str, ProcessDefinition]
) -> pd.DataFrame:
    """Histórico de enviados; se consulta aparte de la mesa activa."""
    return orders_with_status(frame, definitions, is_sent_status)


def canonical_times_df(frame: pd.DataFrame, order_ids: Iterable[Any] = ()) -> pd.DataFrame:
    result = frame.copy()
    result.columns = [canonical_column_name(column) for column in result.columns]
    result = result.loc[:, ~result.columns.duplicated()].copy()
    for column in result:
        result[column] = result[column].map(clean_cell)
    if ID_COLUMN in result:
        result[ID_COLUMN] = result[ID_COLUMN].map(identifier_resolver(order_ids))
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
    logs = canonical_times_df(times_df, orders_df.get(ID_COLUMN, []))
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
    if column == "VENDEDOR":
        return dropdown_fields.canonical_vendor(value)
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
    *,
    reactivate: bool = False,
    manual_stage: bool = False,
    current_user: str = "",
) -> list[str]:
    """``manual_stage`` valida contra todas las etapas del producto (ver get_manual_stage_statuses)."""
    identifier = clean_cell(row.get(ID_COLUMN, "")).strip()
    if current_user in VIEW_AND_CREATE_USERS:
        if not status_only_here(current_user) or reactivate:
            return ["Tu usuario no tiene permiso para editar pedidos."]
        if set(delta) - {STATUS_COLUMN}:
            return [f"{identifier}: tu usuario sólo puede cambiar la etapa (STATUS)."]
    errors: list[str] = []
    if manual_stage and reactivate:
        return [f"{identifier}: usa el histórico de Enviados para reactivar este pedido."]
    if manual_stage and current_user not in MANUAL_STAGE_USERS:
        return [f"{identifier}: tu usuario no puede cambiar etapas manualmente."]
    invalid_columns = set(delta) - aligners_editable_columns(row.index)
    if invalid_columns:
        errors.append(f"{identifier}: no se puede editar {', '.join(sorted(invalid_columns))}.")
    if (current_order_sheet() == SHEET_AERIS and PRODUCT_COLUMN in delta
            and product_key(delta[PRODUCT_COLUMN]) != product_key(SHEET_AERIS)):
        errors.append(f"{identifier}: el seguimiento AERIS sólo admite el producto AERIS.")
    if reactivate and set(delta) - {STATUS_COLUMN}:
        errors.append(f"{identifier}: en Enviados sólo se puede cambiar la etapa.")
    for column in {column for column in delta if column in DATE_COLUMNS or column.startswith("FECHA")}:
        value = clean_cell(delta[column]).strip()
        if value and parse_simple_date(value) is None and not is_payment_text(column, value):
            errors.append(f"{identifier}: la fecha de {column} no es válida.")
    errors.extend(shipment_value_errors(identifier, row.index, delta))
    if STATUS_COLUMN in delta:
        previous = canonical_status(row.get(STATUS_COLUMN, ""), configured_statuses(definitions))
        product = delta.get(PRODUCT_COLUMN, row.get(PRODUCT_COLUMN, ""))
        if manual_stage:
            allowed = get_manual_stage_statuses(product, previous, definitions, current_user)
        else:
            allowed_for = get_reactivation_statuses if reactivate else get_allowed_next_statuses
            allowed = allowed_for(product, previous, definitions)
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
    *,
    reactivate: bool = False,
    select_catalog: dict | None = None,
) -> tuple[list[str], list[str]]:
    """Guarda por número de orden; ``reactivate`` saca pedidos del histórico de Enviados."""
    try:
        changes = grid_changes(baseline, edited, definitions)
    except ValueError as exc:
        return [], [str(exc)]
    if not changes:
        return [], []
    # Filas cuya etapa se eligió desde «Ver todas las etapas…» (tabla o ficha).
    manual_targets = {clean_cell(key).strip(): value
                      for key, value in edited.attrs.get("manualStageTargets", {}).items()}
    manual_ids = {
        identifier for identifier, delta in changes
        if STATUS_COLUMN in delta and identifier in manual_targets
        and status_key(grid_cell_value(STATUS_COLUMN, manual_targets[identifier], definitions))
        == status_key(delta[STATUS_COLUMN])
    }
    multiple = dropdown_fields.multiple_columns(select_catalog or {}, source, MULTI_SELECT_COLUMNS)
    order_detail.restore_catalog_values(changes, select_catalog, multiple)

    if not can_edit_orders(current_user) and (reactivate or not can_change_status_here(current_user)):
        return [], ["Tu usuario no tiene permiso para editar pedidos."]
    if source[ID_COLUMN].duplicated().any():
        return [], ["Hay números de orden duplicados; corrige la hoja antes de guardar."]

    source_by_id = source.set_index(ID_COLUMN, drop=False)
    saved: list[str] = []
    errors: list[str] = []
    # Prevalidar todo el lote antes del primer write evita aplicar sólo la parte válida.
    for identifier, delta in changes:
        if identifier not in source_by_id.index:
            errors.append(f"{identifier}: el pedido ya no está en la vista actual.")
        else:
            errors.extend(validate_delta(source_by_id.loc[identifier], delta, definitions, reactivate=reactivate,
                                         manual_stage=identifier in manual_ids, current_user=current_user))
            for column, value in delta.items():
                if column in (select_catalog or {}) and column != STATUS_COLUMN and not dropdown_fields.valid_value(
                    value, select_catalog[column], multiple=column in multiple,
                    previous=source_by_id.loc[identifier].get(column, "")):
                    errors.append(f"{identifier}: valor no permitido para {column}.")
    if errors:
        return [], errors
    for identifier, delta in changes:
        if identifier not in source_by_id.index:
            errors.append(f"{identifier}: el pedido ya no está en la vista actual.")
            break
        row = source_by_id.loc[identifier]
        validation_errors = validate_delta(row, delta, definitions, reactivate=reactivate,
                                           manual_stage=identifier in manual_ids, current_user=current_user)
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
                    product=clean_cell(delta.get(PRODUCT_COLUMN, row.get(PRODUCT_COLUMN, ""))).strip(),
                    previous_status=previous_status,
                    new_status=delta[STATUS_COLUMN],
                    current_user=current_user,
                    definitions=definitions,
                    comment=(
                        "Reactivado desde el histórico de Enviados."
                        if reactivate
                        else " ".join(filter(None, [
                            MANUAL_STAGE_COMMENT if identifier in manual_ids else "",
                            delta.get("DETALLE COMENTARIOS", ""),
                        ]))
                    ),
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
    if not can_edit_orders(current_user):
        raise ValueError("Tu usuario no tiene permiso para reparar la medición de pedidos.")
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
    row: pd.Series,
    definitions: dict[str, ProcessDefinition],
    *,
    reactivate: bool = False,
) -> list[str]:
    allowed_for = get_reactivation_statuses if reactivate else get_allowed_next_statuses
    options = allowed_for(
        row.get(PRODUCT_COLUMN, ""), row.get(STATUS_COLUMN, ""), definitions
    )
    return [status_display_value(option, definitions) for option in options]


def grid_configuration_parts(
    grid: pd.DataFrame,
    source: pd.DataFrame,
    definitions: dict[str, ProcessDefinition],
    *,
    reactivate: bool = False,
    select_catalog: dict | None = None,
    current_user: str = "",
) -> dict[str, Any]:
    """Opciones por pedido (etapas, listas, fechas, colores) y columnas editables."""
    duplicates = set(source.loc[source[ID_COLUMN].duplicated(keep=False), ID_COLUMN])
    manual_stages = {} if reactivate else {
        row[ID_COLUMN]: [status_display_value(option, definitions) for option in get_manual_stage_statuses(
            row.get(PRODUCT_COLUMN, ""), row.get(STATUS_COLUMN, ""), definitions, current_user)]
        for _, row in source.iterrows() if row[ID_COLUMN] not in duplicates
    }
    stages = {
        row[ID_COLUMN]: (
            grid_stage_options(row, definitions, reactivate=reactivate)
            if row[ID_COLUMN] not in duplicates
            else [status_display_value(row[STATUS_COLUMN], definitions)]
        )
        for _, row in source.iterrows()
    }
    selections: dict[str, list[str]] = dict(select_catalog or {})
    multiple = dropdown_fields.multiple_columns(selections, source, MULTI_SELECT_COLUMNS)
    for column in SELECT_COLUMNS & set(grid) - set(selections):
        values = [clean_cell(value).strip() for value in source[column]]
        selections[column] = list(dict.fromkeys(["", *[value for value in values if value]]))
    dates: dict[str, dict[str, str]] = {}
    for column in {col for col in grid if col in DATE_COLUMNS or col.startswith("FECHA")}:
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
    editable_columns = ({STATUS_COLUMN} & set(grid.columns)
                        if reactivate else aligners_editable_columns(grid.columns))
    if current_user in VIEW_AND_CREATE_USERS:
        editable_columns = ({STATUS_COLUMN} & editable_columns
                            if status_only_here(current_user) and not reactivate else set())
    return {"stages": stages, "manual_stages": manual_stages, "selections": selections,
            "multiple": multiple, "dates": dates, "palettes": palettes, "editable": editable_columns}


def decorate_order_grid(options: dict, selections: dict, grid: pd.DataFrame, multiple) -> dict:
    return dropdown_fields.decorate_grid(options, selections, grid, multiple=multiple,
        labeler=new_order_option_label, palettes=order_dropdown_palettes(selections),
        canonicalize=lambda col, value: strip_visual_prefix(value) if col == PRODUCT_COLUMN else str(value))


def build_grid_configuration(
    grid: pd.DataFrame,
    source: pd.DataFrame,
    definitions: dict[str, ProcessDefinition],
    *,
    hidden_columns: set[str] | None = None,
    reactivate: bool = False,
    select_catalog: dict | None = None,
    current_user: str = "",
) -> dict:
    """En modo ``reactivate`` (histórico de Enviados) sólo se edita la etapa.

    Fuera de él, ``current_user`` decide si STATUS ofrece «Ver todas las etapas…».
    """
    parts = grid_configuration_parts(grid, source, definitions, reactivate=reactivate,
                                     select_catalog=select_catalog, current_user=current_user)
    options = build_aligners_grid_options(
        grid,
        editable=parts["editable"],
        automatic=AUTOMATIC_COLUMNS,
        stage_options=parts["stages"],
        select_options=parts["selections"],
        date_values=parts["dates"],
        palettes=parts["palettes"],
        time_zone=APP_TIMEZONE_NAME,
        hidden_columns=hidden_columns,
        manual_stage_options=parts["manual_stages"],
    )
    return decorate_order_grid(options, parts["selections"], grid, parts["multiple"])


def tracking_columns(available_columns: Iterable[str]) -> list[str]:
    columns = list(available_columns)
    visible = [column for column in GRID_COLUMNS if column in columns]
    # La hoja puede incorporar más campos propios sin cambiar la app.
    if current_order_sheet() in (SHEET_POLANCO, SHEET_AERIS):
        extras = [column for column in columns
                  if column not in visible and not column.startswith("_")]
        position = next((i for i, column in enumerate(visible)
                         if column in COMPUTED_COLUMNS[1:]), len(visible))
        visible[position:position] = extras
    return visible


NEW_ORDER_SELECT_COLUMNS = (
    "PRODUCTO", "TIPO", "ETAPA SOLICITUD", "VENDEDOR", "SERVICIO",
    "PAQUETE MARCA BLANCA", "ARCHIVOS RECIBIDOS",
)
NEW_ORDER_FIELD_ICONS = {
    "PRODUCTO": "🦷", "TIPO": "🏷️", "ETAPA SOLICITUD": "📋",
    "VENDEDOR": "🤝", "SERVICIO": "🛠️", "PAQUETE MARCA BLANCA": "📦",
    "ARCHIVOS RECIBIDOS": "📁",
}
# Indicadores visuales de la app. Sheets expone los valores de los chips, pero
# no sus colores individuales. Conservamos los tonos observados y añadimos
# iconos semánticos, sin modificar el texto que se guarda en Drive.
NEW_ORDER_OPTION_ICONS = {
    "PRODUCTO": {"CONVENC.": "🔵", "GRAPHY": "🟠", "RETENEDORES": "🟢",
                 "MODELOS": "🟤", "SETUP": "🟣", "GUARDA": "🌙", "GUIA": "🧭"},
    "TIPO": {"ARTTDLAB": "🔵", "MARCA BLANCA": "⚪"},
    "ETAPA SOLICITUD": {"NUEVO": "🟢", "REFIN": "🔵"},
    "SERVICIO": {"PLANEACION": "🟤", "PLAN+CONFEC.": "🟡",
                 "CONFECCION": "🟣", "IMPRESION": "🖨️"},
    "PAQUETE MARCA BLANCA": {"LITE": "🟢", "STANDARD": "🔵",
                            "LIMITLESS": "🟣", "LIMITLESS+": "🟠", "NO APLICA": "⚪"},
    "ARCHIVOS RECIBIDOS": {"STLS": "📁", "TOMO": "🩻", "RX LATERAL": "🩻",
                          "RX PANO": "🩻", "FOTO INTRA": "📸", "FOTO EXTRA": "📷"},
}


def parse_order_form_catalogs(metadata: dict[str, Any]) -> dict[str, dict[str, list[str]]]:
    """Lee opciones nativas de tabla, incluso las que todavía no tienen pedidos."""
    return {title: catalog for title, catalog in
            order_detail.parse_sheet_catalogs(metadata, canonical_column_name).items()
            if title in ORDER_TIME_SHEETS}


@st.cache_data(ttl=3600)
def read_order_form_catalogs(spreadsheet_id: str) -> dict[str, dict[str, list[str]]]:
    spreadsheet = get_spreadsheet_by_id(spreadsheet_id)
    # No incluir rangos de una hoja ausente: Sheets rechaza toda la consulta.
    available = run_gsheets_request(lambda: spreadsheet.fetch_sheet_metadata(params={
        "fields": "sheets(properties(title))",
    }))
    titles = {sheet.get("properties", {}).get("title") for sheet in available.get("sheets", [])}
    ranges = [f"'{name}'!A2:CB12" for name in ORDER_TIME_SHEETS if name in titles]
    if not ranges:
        return {}
    metadata = run_gsheets_request(lambda: spreadsheet.fetch_sheet_metadata(params={
        "includeGridData": "true",
        "ranges": ranges,
        "fields": "sheets(properties(title),tables(range,columnProperties(columnName,dataValidationRule)),data(startRow,rowData(values(formattedValue,dataValidation))))",
    }))
    return parse_order_form_catalogs(metadata)


def get_order_form_catalog() -> dict[str, list[str]]:
    catalogs = st.session_state.get("aligners_form_catalogs")
    if catalogs is None:
        catalogs = read_order_form_catalogs(configured_spreadsheet_id())
        st.session_state["aligners_form_catalogs"] = catalogs
    catalog = dropdown_fields.with_app_vendors(catalogs.get(current_order_sheet(), {}))
    if current_order_sheet() == SHEET_AERIS:
        # Esta hoja sólo atiende AERIS, incluso si copió una lista de Polanco.
        catalog[PRODUCT_COLUMN] = [value for value in catalog.get(PRODUCT_COLUMN, [])
                                   if product_key(value) == product_key(SHEET_AERIS)] or [SHEET_AERIS]
    return catalog


def tracking_form_catalog() -> dict:
    try:
        return get_order_form_catalog()
    except Exception:
        # Evita repetir solicitudes fallidas en cada clic. Actualizar datos
        # descarta esta caché y vuelve a consultar los catálogos.
        st.session_state["aligners_form_catalogs"] = {}
        st.warning("No se pudieron cargar las listas de Sheets. Pulsa Actualizar datos para reintentarlo.")
        return {}


def order_dropdown_palettes(catalog):
    tones = {"🔵": ("#DBEAFE", "#1E40AF"), "🟢": ("#DCFCE7", "#166534"),
             "🟣": ("#F3E8FF", "#6B21A8"), "🟠": ("#FFEDD5", "#9A3412"),
             "🟤": ("#EFEBE9", "#5D4037"), "🟡": ("#FEF3C7", "#92400E"),
             "⚪": ("#F1F5F9", "#475569")}
    return {column: {value: tones.get(NEW_ORDER_OPTION_ICONS.get(column, {}).get(normalize_text(value)),
                                    dropdown_fields.option_colors(column, value))
                     for value in values} for column, values in catalog.items()}


def new_order_option_label(column: str, value: str) -> str:
    text = (dropdown_fields.vendor_display_name(value) if column == "VENDEDOR"
            else clean_cell(value).strip())
    icon = NEW_ORDER_OPTION_ICONS.get(column, {}).get(normalize_text(text))
    if not icon:
        icon = "👤" if column == "VENDEDOR" else NEW_ORDER_FIELD_ICONS.get(column, "🔹")
    return f"{icon} {text}"


def generate_order_identifier(values: list[list[Any]], headers: list[str],
                              reserved_ids: Iterable[Any] = ()) -> str:
    """Mismo folio diario DDMMAAAA-NNN de Aparatos, dentro de la hoja activa."""
    position = headers.index(ID_COLUMN)
    existing = {clean_cell(row[position]).strip() for row in values[ORDER_HEADER_ROW:]
                if len(row) > position}
    existing.update(clean_cell(value).strip() for value in reserved_ids)
    prefix = app_today().strftime("%d%m%Y")
    sequence = 1
    while f"{prefix}-{sequence:03d}" in existing:
        sequence += 1
    return f"{prefix}-{sequence:03d}"


@st.cache_resource
def order_creation_lock():
    # Serializa lectura del folio + append entre sesiones de esta instancia.
    return threading.RLock()


def create_order(values: dict[str, Any], current_user: str,
                 definitions: dict[str, ProcessDefinition], *,
                 catalog: dict[str, list[str]] | None = None) -> tuple[str, str]:
    with order_creation_lock():
        return _create_order(values, current_user, definitions, catalog=catalog)


def _create_order(values: dict[str, Any], current_user: str,
                  definitions: dict[str, ProcessDefinition], *,
                  catalog: dict[str, list[str]] | None = None) -> tuple[str, str]:
    """Añade una orden en la hoja activa y reporta por separado un fallo de bitácora."""
    if not can_create_orders(current_user):
        raise ValueError("Tu usuario no tiene permiso para agregar pedidos.")
    row = {key: prepare_sheet_value(value).strip() for key, value in values.items()}
    if catalog is not None:
        for column in NEW_ORDER_SELECT_COLUMNS:
            selected = values.get(column)
            if selected and (catalog.get(column) or current_order_sheet() != SHEET_AERIS):
                if not dropdown_fields.valid_value(selected, catalog.get(column, []), multiple=column in MULTI_SELECT_COLUMNS):
                    raise ValueError(f"La opción de {column} no pertenece al catálogo de Sheets.")
                row[column] = (dropdown_fields.join_values(dropdown_fields.split_values(selected, catalog[column]))
                               if column in MULTI_SELECT_COLUMNS else selected)

    for column in (PRODUCT_COLUMN, "NOMBRE DOCTOR", "NOMBRE PACIENTE"):
        if not row.get(column):
            raise ValueError(f"Completa {column}.")
    if current_order_sheet() == SHEET_AERIS and product_key(row[PRODUCT_COLUMN]) != product_key(SHEET_AERIS):
        raise ValueError("El seguimiento AERIS sólo admite el producto AERIS.")
    definition = get_process_definition(row[PRODUCT_COLUMN], definitions)
    if not definition or not definition.normal_statuses:
        raise ValueError("El producto no tiene un flujo configurado en PROCESOS POR PRODUCTO.")
    sheet_name = current_order_sheet()
    worksheet = get_worksheet(sheet_name)
    existing = run_gsheets_request(lambda: worksheet.get_all_values())
    if len(existing) < ORDER_HEADER_ROW:
        raise ValueError(f"No encontré los encabezados de {sheet_name} en la fila 2.")
    headers = [canonical_column_name(item) for item in
               ensure_unique_column_names(existing[ORDER_HEADER_ROW - 1])]
    for column in (ID_COLUMN, STATUS_COLUMN, PRODUCT_COLUMN, "NOMBRE DOCTOR", "NOMBRE PACIENTE"):
        if column not in headers:
            raise ValueError(f"Falta la columna {column} en {sheet_name}.")
    # A missing order may still have history. Never recycle its folio.
    log_values = run_gsheets_request(lambda: get_worksheet(current_times_sheet()).get_all_values())
    log_id_position = get_header_position(log_values[0], ID_COLUMN) if log_values else None
    reserved = [item[log_id_position - 1] for item in log_values[1:]
                if log_id_position and len(item) >= log_id_position]
    identifier = generate_order_identifier(existing, headers, reserved)
    row.update({ID_COLUMN: identifier, STATUS_COLUMN: definition.normal_statuses[0],
                "FECHA DE RECEPCIÓN": app_today().isoformat()})
    # values.append detects logical tables and can insert above later cases.
    # appendCells uses the last populated row of the whole sheet, atomically.
    # Explicit string values preserve folios and literal user-entered text.
    # No se reintenta una inserción: un timeout ambiguo podría duplicar la orden.
    worksheet.spreadsheet.batch_update({"requests": [{"appendCells": {
        "sheetId": worksheet.id,
        "rows": [{"values": [
            {"userEnteredValue": {"stringValue": row.get(header, "")}}
            for header in headers
        ]}],
        "fields": "userEnteredValue",
    }}]})
    clear_sheet_data_cache()
    reset_workbench()
    confirmed = run_gsheets_request(lambda: worksheet.get_all_values())
    id_index = headers.index(ID_COLUMN)
    matches = [item for item in confirmed[ORDER_HEADER_ROW:]
               if len(item) > id_index and clean_cell(item[id_index]).strip() == identifier]
    if len(matches) != 1 or any(
        len(matches[0]) <= headers.index(column)
        or clean_cell(matches[0][headers.index(column)]).strip() != row[column]
        for column in (PRODUCT_COLUMN, "NOMBRE DOCTOR", "NOMBRE PACIENTE")
    ):
        raise ValueError(f"No se pudo verificar la orden {identifier}. Actualiza la tabla antes de reintentar.")
    warning = ""
    try:
        register_status_change(identifier=identifier, product=row[PRODUCT_COLUMN],
                               previous_status="", new_status=row[STATUS_COLUMN],
                               current_user=current_user, definitions=definitions,
                               comment="Registro inicial desde la app.")
    except Exception as exc:
        warning = (f"La orden {identifier} sí se guardó, pero falta iniciar su medición. "
                   f"Usa Reparar medición en el pedido. Detalle: {exc}")
    clear_sheet_data_cache()
    reset_workbench()
    return identifier, warning


def render_new_order(current_user: str, snapshot: dict[str, Any]) -> None:
    definitions = snapshot["definitions"]
    try:
        catalog = get_order_form_catalog()
    except Exception as exc:
        st.error(f"No se pudieron cargar las opciones de Sheets: {exc}")
        return
    # AERIS tiene su propio esquema, sin TIPO ni ETAPA SOLICITUD.
    select_columns = list(NEW_ORDER_SELECT_COLUMNS)
    if current_order_sheet() == SHEET_AERIS:
        orders = snapshot.get("orders", pd.DataFrame())
        select_columns = [column for column in select_columns if column in orders.columns]
    missing = [column for column in select_columns if not catalog.get(column)
               and current_order_sheet() != SHEET_AERIS]
    if missing:
        st.warning("Faltan listas en la tabla de Sheets: " + ", ".join(missing) +
                   ". Configúralas y pulsa Actualizar datos.")
        return
    st.subheader("➕ Nueva orden")
    st.caption(f"Captura los datos principales en {current_order_sheet()}.")
    version = st.session_state.get(tracking_key("aligners_new_version"), 0)
    with st.form(tracking_key(f"aligners_new_form_{version}")):
        st.info(f"🔢 Folio automático: DDMMAAAA-NNN · 📥 Recepción: {app_today():%d/%m/%Y} · "
                "📋 Etapa inicial automática según el producto.")
        row: dict[str, Any] = {}

        def select_field(container, column):
            if not catalog.get(column):
                return container.text_input(f"{NEW_ORDER_FIELD_ICONS[column]} {column}",
                    key=tracking_key(f"aligners_new_{column}_{version}"))
            if column in MULTI_SELECT_COLUMNS:
                values = container.multiselect(f"{NEW_ORDER_FIELD_ICONS[column]} {column}",
                    catalog[column], placeholder="Elige una o varias opciones",
                    format_func=lambda value: new_order_option_label(column, value),
                    key=tracking_key(f"aligners_new_{column}_{version}"))
                return dropdown_fields.join_values(values)
            return container.selectbox(
                f"{NEW_ORDER_FIELD_ICONS[column]} {column}" + (" *" if column == PRODUCT_COLUMN else ""),
                catalog[column], index=0 if current_order_sheet() == SHEET_AERIS and column == PRODUCT_COLUMN else None,
                placeholder="Selecciona una opción",
                format_func=lambda value: new_order_option_label(column, value),
                key=tracking_key(f"aligners_new_{column}_{version}"),
            )

        first = st.columns(3)
        row[PRODUCT_COLUMN] = select_field(first[0], PRODUCT_COLUMN)
        row["NOMBRE DOCTOR"] = first[1].text_input("👩‍⚕️ NOMBRE DOCTOR *")
        row["NOMBRE PACIENTE"] = first[2].text_input("👤 NOMBRE PACIENTE *")
        for group in (("TIPO", "ETAPA SOLICITUD", "VENDEDOR"),
                      ("SERVICIO", "PAQUETE MARCA BLANCA", "ARCHIVOS RECIBIDOS")):
            fields = [column for column in group if column in select_columns]
            columns = st.columns(len(fields)) if fields else []
            for container, column in zip(columns, fields):
                row[column] = select_field(container, column)
        if current_order_sheet() == SHEET_POLANCO:
            comments, balance = st.columns([2, 1])
            row["DETALLE COMENTARIOS"] = comments.text_area("📝 DETALLE COMENTARIOS", height=80)
            row["ADEUDO"] = balance.number_input("💰 ADEUDO", min_value=0.0, value=None,
                                                 step=100.0, format="%.2f", placeholder="Opcional")
        else:
            row["DETALLE COMENTARIOS"] = st.text_area("📝 DETALLE COMENTARIOS", height=80)
        submitted = st.form_submit_button("💾 Guardar nueva orden")
    if submitted:
        try:
            identifier, warning = create_order(row, current_user, definitions, catalog=catalog)
        except Exception as exc:
            st.error(f"No se pudo confirmar el guardado: {exc}")
            st.info("Si hubo un error de conexión, actualiza la tabla y verifica la orden antes de reintentar.")
            return
        st.session_state[tracking_key("aligners_new_version")] = version + 1
        st.session_state[tracking_key("aligners_feedback")] = (
            [identifier], [warning] if warning else [], f"Orden {identifier} creada en {current_order_sheet()}.")
        rerun_active_tab()


def guard_tracking_tab_change(current_user: str) -> None:
    """Impide que Streamlit elimine el editor de una pestaña con cambios pendientes."""
    previous = st.session_state.get("aligners_active_tab", "📋 Seguimiento")
    key = f"aligners_primary_tabs_{current_user}"
    # La vista general edita en las tres hojas; cada pestaña individual, sólo en la suya.
    sheets = ([sheet for _, sheet in GENERAL_ORIGINS] if previous == GENERAL_TAB_LABEL
              else [current_order_sheet()])
    pending = previous == GENERAL_TAB_LABEL and general_grid_change_count() > 0
    for sheet in sheets:
        with order_sheet_scope(sheet):
            snapshot = st.session_state.get(tracking_key("aligners_snapshot")) or {}
            pending = pending or bool(pending_change_count(snapshot.get("definitions", {})))
    if st.session_state.get(key) != previous and pending:
        st.session_state[key] = previous
        st.session_state["aligners_tab_warning"] = True


def reset_workbench() -> None:
    st.session_state.pop(tracking_key("aligners_snapshot"), None)
    for prefix in ("aligners", "aligners_sent"):
        st.session_state.pop(tracking_key(f"{prefix}_editor_key"), None)
        st.session_state.pop(tracking_key(f"{prefix}_editor_baseline"), None)
    st.session_state[tracking_key("aligners_revision")] = st.session_state.get(tracking_key("aligners_revision"), 0) + 1


def discard_sent_changes() -> None:
    """Descarta sólo el histórico de Enviados; la mesa activa conserva sus ediciones."""
    st.session_state.pop(tracking_key("aligners_sent_editor_key"), None)
    st.session_state.pop(tracking_key("aligners_sent_editor_baseline"), None)
    revision_key = tracking_key("aligners_sent_revision")
    st.session_state[revision_key] = st.session_state.get(revision_key, 0) + 1


def without_repeated_orders(
    baseline: pd.DataFrame, edited: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Los folios repetidos se muestran en Enviados, pero no se pueden reactivar."""
    if ID_COLUMN not in baseline or ID_COLUMN not in edited:
        return baseline, edited
    repeated = set(baseline.loc[baseline[ID_COLUMN].duplicated(keep=False), ID_COLUMN])
    return (
        baseline[~baseline[ID_COLUMN].isin(repeated)],
        edited[~edited[ID_COLUMN].isin(repeated)],
    )


def editor_change_count(
    prefix: str,
    definitions: dict[str, ProcessDefinition],
    *,
    skip_repeated: bool = False,
) -> int:
    key = st.session_state.get(tracking_key(f"{prefix}_editor_key"), "")
    if not key:
        return 0
    state = st.session_state.get(key) or {}
    baseline = st.session_state.get(tracking_key(f"{prefix}_editor_baseline"))
    if baseline is None or not state:
        return 0
    edited = response_frame(baseline, state, ID_COLUMN)
    if skip_repeated:
        baseline, edited = without_repeated_orders(baseline, edited)
    try:
        return len(grid_changes(baseline, edited, definitions))
    except ValueError:
        return 1


def sent_change_count(definitions: dict[str, ProcessDefinition]) -> int:
    return editor_change_count("aligners_sent", definitions, skip_repeated=True)


def pending_change_count(definitions: dict[str, ProcessDefinition]) -> int:
    """Suma la mesa activa y el histórico de Enviados: refrescar perdería ambos."""
    return (editor_change_count("aligners", definitions) + sent_change_count(definitions)
            + order_detail.pending_count(tracking_key("aligners")))


# ==============================
# Interfaz
# ==============================
def get_snapshot(current_user: str) -> dict[str, Any]:
    snapshot = st.session_state.get(tracking_key("aligners_snapshot"))
    if snapshot is not None and snapshot.get("user") != current_user:
        order_detail.clear_drafts(tracking_key("aligners"))
    # Las sesiones abiertas antes del histórico de Enviados no traen "sent".
    if snapshot is None or snapshot.get("user") != current_user or "sent" not in snapshot:
        definitions = read_process_definitions()
        orders = read_orders_df()
        times = read_times_df()
        snapshot = {
            "user": current_user,
            "definitions": definitions,
            "orders": orders,
            "times": times,
            "table": build_tracking_table(orders, times, definitions),
            "sent": sent_orders(orders, definitions),
            "at": app_now(),
        }
        st.session_state[tracking_key("aligners_snapshot")] = snapshot
    return snapshot


def signal_filter_key(key_prefix: str | None = None) -> str:
    """Sin prefijo, cada seguimiento guarda su filtro; la vista general usa el suyo."""
    if key_prefix is None:
        return tracking_key(ALIGNERS_SIGNAL_FILTER_KEY)
    return key_prefix + ALIGNERS_SIGNAL_FILTER_KEY


def set_aligners_signal_filter(signal: str, key_prefix: str | None = None) -> None:
    """Actualiza el filtro de semáforo desde una tarjeta."""

    st.session_state[signal_filter_key(key_prefix)] = (
        signal if signal in {"Todos", *ALERT_COLORS} else "Todos"
    )


def current_aligners_signal_filter(key_prefix: str | None = None) -> str:
    key = signal_filter_key(key_prefix)
    signal = st.session_state.get(key, "Todos")
    if signal not in {"Todos", *ALERT_COLORS}:
        signal = "Todos"
        st.session_state[key] = signal
    return signal


def render_metrics(
    table: pd.DataFrame,
    *,
    namespace: str = "",
    interactive: bool = False,
    disabled: bool = False,
    key_prefix: str | None = None,
) -> str:
    """Muestra métricas; en Seguimiento también funcionan como filtros."""

    labels = [
        ("total", "🦷 Pedidos activos", "Todos"),
        ("red", "🔴 Atrasado", "🔴 Atrasado"),
        ("amber", "🟡 Por vencer", "🟡 Por vencer"),
        ("pause", "🟣 En pausa", "🟣 En pausa"),
        ("green", "🟢 En tiempo", "🟢 En tiempo"),
        ("gray", "⚪ Sin medición", "⚪ Sin medición"),
    ]
    selected = current_aligners_signal_filter(key_prefix)
    metrics = st.columns(len(labels))
    for container, (tone, label, signal) in zip(metrics, labels):
        value = len(table) if signal == "Todos" else int(table["SEMÁFORO"].eq(signal).sum())
        if interactive:
            with container.container(key=f"align_filter_{tone}"):
                st.button(
                    f"{label}\n\n**{value}**",
                    key=(tracking_key(f"aligners_signal_card_{tone}") if key_prefix is None
                         else f"{key_prefix}aligners_signal_card_{tone}"),
                    type="primary" if selected == signal else "secondary",
                    disabled=disabled,
                    use_container_width=True,
                    on_click=set_aligners_signal_filter,
                    args=(signal, key_prefix),
                    help="Mostrar todos los pedidos" if signal == "Todos" else f"Filtrar por {label}",
                )
        else:
            key = f"align_{tone}_{namespace}" if namespace else f"align_{tone}"
            with container.container(key=key):
                st.metric(label, value)
    return selected


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


def aligners_batch_stages(selected: pd.DataFrame, definitions: dict) -> list[str]:
    return order_detail.common_stages(selected,
        lambda row: get_allowed_next_statuses(row.get(PRODUCT_COLUMN, ""), row.get(STATUS_COLUMN, ""), definitions),
        STATUS_COLUMN, status_key)


PAYMENT_MODES = {"📅 Fecha": "", "🎁 Cortesía": "CORTESIA", "⏳ Pendiente": "PENDIENTE"}


def _order_draft(namespace: str, identifier: str) -> dict[str, Any]:
    return st.session_state[order_detail.draft_key(namespace)][identifier]


def shipment_state_key(kind: str, namespace: str, identifier: str) -> str:
    return f"shipment_{kind}_{namespace}_{identifier}"


def refresh_shipment_total(namespace: str, identifier: str, block: ShipmentBlock) -> None:
    """El Total sólo se reescribe cuando cambian las cantidades de su bloque."""
    column = block.columns.get("total")
    if not column:
        return
    draft = _order_draft(namespace, identifier)
    total = shipment_total({kind: order_detail.draft_value(draft, item) for kind, item in block.columns.items()})
    changed = any(block.columns.get(kind) in draft["changes"] for kind in SHIPMENT_QUANTITY_KINDS)
    if not changed or total is None:
        draft["changes"].pop(column, None)
    else:
        order_detail.set_draft_value(namespace, identifier, column, total, values_equivalent)


def remember_shipment_field(namespace: str, identifier: str, block: ShipmentBlock,
                            column: str, key: str, convert: Callable[[Any], str]) -> None:
    order_detail.set_draft_value(namespace, identifier, column, convert(st.session_state.get(key)),
                                 values_equivalent)
    refresh_shipment_total(namespace, identifier, block)


def remember_payment_mode(namespace: str, identifier: str, column: str, key: str) -> None:
    value = PAYMENT_MODES.get(st.session_state.get(key), "")
    if not value:
        # Volver a "Fecha" recupera la fecha original, si la había.
        original = _order_draft(namespace, identifier)["baseline"].get(column, "")
        value = clean_cell(original).strip() if parse_simple_date(original) else ""
    order_detail.set_draft_value(namespace, identifier, column, value, values_equivalent)


def start_new_shipment(namespace: str, identifier: str, number: int) -> None:
    st.session_state[shipment_state_key("target", namespace, identifier)] = number


def cancel_new_shipment(namespace: str, identifier: str, block: ShipmentBlock) -> None:
    draft = _order_draft(namespace, identifier)
    for column in block.columns.values():
        draft["changes"].pop(column, None)
    st.session_state[shipment_state_key("target", namespace, identifier)] = block.number - 1


def copy_shipment_quantities(namespace: str, identifier: str,
                             previous: ShipmentBlock, block: ShipmentBlock) -> None:
    draft = _order_draft(namespace, identifier)
    for kind in SHIPMENT_QUANTITY_KINDS:
        if kind in previous.columns and kind in block.columns:
            value = clean_cell(order_detail.draft_value(draft, previous.columns[kind])).strip()
            order_detail.set_draft_value(namespace, identifier, block.columns[kind], value, values_equivalent)
    refresh_shipment_total(namespace, identifier, block)
    # Claves nuevas: los campos ya dibujados deben mostrar lo copiado.
    nonce = shipment_state_key("nonce", namespace, identifier)
    st.session_state[nonce] = st.session_state.get(nonce, 0) + 1


def shipment_display_value(kind: str, value: Any) -> str:
    text = clean_cell(value).strip()
    if kind in {"pago", "envio"} and text:
        parsed = parse_simple_date(text)
        if parsed is not None:
            return f"{parsed:%d/%m/%Y}"
        return PAYMENT_TEXT_VALUES.get(normalize_text(text), text)
    return text or "—"


def payment_mode(value: Any) -> str | None:
    key = normalize_text(value)
    if key.startswith("CORTES"):
        return "🎁 Cortesía"
    if key.startswith(("PEND", "PEDN")):
        return "⏳ Pendiente"
    if not key or parse_simple_date(value) is not None:
        return "📅 Fecha"
    return None


def render_shipment_field(namespace: str, identifier: str, block: ShipmentBlock, kind: str,
                          token: str, *, allow_range: bool) -> None:
    """Un campo del bloque; conserva textos que no encajan en el control numérico."""
    draft = _order_draft(namespace, identifier)
    column = block.columns[kind]
    text = clean_cell(order_detail.draft_value(draft, column)).strip()
    key = f"shipment_{token}_{column}"
    label = SHIPMENT_FIELD_LABELS[kind]
    field_args = (namespace, identifier, block, column, key)
    text_value = lambda value: clean_cell(value).strip()
    if kind == "total":
        total = shipment_total({item: order_detail.draft_value(draft, name) for item, name in block.columns.items()})
        value = text if total is None else total
        st.markdown(
            f'<div style="font-size:.875rem;margin-bottom:.35rem">{label}</div>'
            '<div style="border:1px solid rgba(128,128,128,.35);border-radius:.5rem;padding:.42rem .75rem;'
            f'font-weight:700;background:rgba(34,160,90,.10)">{html.escape(value or "—")}</div>',
            unsafe_allow_html=True,
        )
        st.caption("Se calcula solo")
    elif kind in SHIPMENT_QUANTITY_KINDS and allow_range and kind.startswith("alin"):
        st.text_input(label, value=text, placeholder="Ej. 7 ó 1-7", key=key,
                      help="Cantidad de alineadores (7) o del alineador al alineador (1-7).",
                      on_change=remember_shipment_field, args=(*field_args, text_value))
        if parse_shipment_quantity(text) is None:
            st.caption(":red[Usa un número (7) o un rango (1-7).]")
    elif kind in SHIPMENT_QUANTITY_KINDS and re.fullmatch(r"\d*", text):
        st.number_input(label, min_value=0, step=1, value=int(text) if text else None, placeholder="0",
                        key=key, on_change=remember_shipment_field,
                        args=(*field_args, lambda value: "" if value is None else str(int(value))))
    elif kind == "pago" and payment_mode(text) is not None:
        mode = payment_mode(text)
        st.radio(label, list(PAYMENT_MODES), index=list(PAYMENT_MODES).index(mode), horizontal=True,
                 key=key + "_mode", on_change=remember_payment_mode,
                 args=(namespace, identifier, column, key + "_mode"))
        if mode == "📅 Fecha":
            st.date_input("Fecha de pago", value=parse_simple_date(text) if text else None,
                          format="DD/MM/YYYY", key=key, label_visibility="collapsed",
                          on_change=remember_shipment_field,
                          args=(*field_args, lambda value: value.isoformat() if value else ""))
    elif kind == "envio" and (not text or parse_simple_date(text) is not None):
        st.date_input(label, value=parse_simple_date(text) if text else None, format="DD/MM/YYYY",
                      key=key, on_change=remember_shipment_field,
                      args=(*field_args, lambda value: value.isoformat() if value else ""))
    else:
        st.text_input(label, value=text, key=key, on_change=remember_shipment_field,
                      args=(*field_args, text_value))
        st.caption("Se conserva el texto de la hoja; puedes corregirlo aquí.")


def render_shipment_fields(namespace: str, identifier: str, block: ShipmentBlock, token: str,
                           previous: ShipmentBlock | None, *, allow_range: bool) -> None:
    draft = _order_draft(namespace, identifier)
    rows = [kinds for kinds in ((*SHIPMENT_QUANTITY_KINDS, "total"), ("pago", "envio"))
            if any(kind in block.columns for kind in kinds)]
    for kinds in rows:
        for container, kind in zip(st.columns(5 if len(kinds) > 2 else 2), kinds):
            if kind not in block.columns:
                continue
            with container:
                render_shipment_field(namespace, identifier, block, kind, token, allow_range=allow_range)
                if previous is not None and kind in previous.columns:
                    if kind == "total":
                        pieces = shipment_pieces(previous, lambda column: order_detail.draft_value(draft, column))
                        shown = "—" if pieces is None else str(pieces)
                    else:
                        shown = shipment_display_value(kind, order_detail.draft_value(draft, previous.columns[kind]))
                    st.caption(f"↳ {previous.title}: **{shown}**")


def render_shipments_section(namespace: str, identifier: str, plan: ShipmentBlock | None,
                             shipments: list[ShipmentBlock]) -> None:
    """Plan de tratamiento y un envío a la vez; el envío anterior queda como referencia."""
    draft = _order_draft(namespace, identifier)
    baseline = draft["baseline"]
    current = lambda column: order_detail.draft_value(draft, column)
    last = last_registered_shipment(shipments, lambda column: baseline.get(column, ""))
    target_key = shipment_state_key("target", namespace, identifier)
    target = st.session_state.get(target_key)
    new_block = shipments[last] if 1 <= last < len(shipments) else None
    if new_block is not None and any(column in draft["changes"] for column in new_block.columns.values()):
        target = new_block.number  # un envío nuevo con datos capturados no se esconde
    if target not in {max(last, 1), *([new_block.number] if new_block else [])}:
        target = max(last, 1)
    st.session_state[target_key] = target
    block = shipments[target - 1]
    previous = shipments[target - 2] if target >= 2 else None
    is_new = target > last

    version = st.session_state.get(f"order_detail_version_{namespace}", 0)
    nonce = st.session_state.get(shipment_state_key("nonce", namespace, identifier), 0)
    watched = block_columns(plan, shipments)
    token = hashlib.sha256(json.dumps(
        [namespace, identifier, version, nonce, target, [baseline.get(column, "") for column in watched]],
        default=str, ensure_ascii=False).encode()).hexdigest()[:16]

    summary = (f"{last} envío registrado" if last == 1 else f"{last} envíos registrados" if last
               else "sin envíos registrados")
    with st.expander(f"🚚 Envíos y alineadores · {summary}", expanded=False,
                     key=shipment_state_key("expander", namespace, identifier)):
        if plan is not None:
            st.markdown("**🦷 Plan de tratamiento**")
            st.caption("Piezas de todo el caso. El total se calcula solo.")
            render_shipment_fields(namespace, identifier, plan, token, None, allow_range=False)

        sent = [pieces for item in shipments if shipment_has_data(item, current)
                and (pieces := shipment_pieces(item, current)) is not None]
        planned = shipment_pieces(plan, current) if plan is not None else None
        if sent:
            progress = f"📦 Enviado: **{sum(sent)}**"
            if planned:
                remaining = planned - sum(sent)
                progress += f" de {planned} piezas del plan · " + (
                    f"faltan {remaining}" if remaining > 0 else "plan completo ✅" if remaining == 0
                    else f"{-remaining} de más ⚠️")
            else:
                progress += " piezas"
            st.markdown(progress)

        heading, action = st.columns([3, 2], vertical_alignment="center")
        heading.markdown(f"##### 📦 {block.title}" + (" · 🆕 nuevo" if is_new and last else ""))
        if new_block is not None and target == last:
            action.button(f"➕ Registrar Envío {new_block.number}", use_container_width=True,
                          key=f"shipment_new_{token}", on_click=start_new_shipment,
                          args=(namespace, identifier, new_block.number))
        elif is_new and last:
            action.button("✖️ Cancelar nuevo envío", use_container_width=True,
                          key=f"shipment_cancel_{token}", on_click=cancel_new_shipment,
                          args=(namespace, identifier, block))
        elif last and last >= len(shipments):
            action.caption(f"Ya se usaron los {len(shipments)} envíos de la hoja.")

        if not last:
            st.caption("Captura el primer envío. Cuando ya exista uno podrás registrar el siguiente.")
        elif is_new:
            st.caption(f"Debajo de cada campo ves lo del {previous.title}: escribe lo nuevo o lo mismo.")
        else:
            st.caption(f"Último envío registrado; corrígelo aquí o pulsa Registrar Envío {last + 1} "
                       "para capturar el siguiente." if new_block else "Último envío registrado.")
        earlier = [item for item in shipments[: target - 1] if shipment_has_data(item, current)]
        if len(earlier) > 1:
            st.caption("🗂️ Anteriores: " + " · ".join(
                f"{item.title}: {shipment_pieces(item, current) or '—'} pzas"
                + (f" ({shipment_display_value('envio', current(item.columns['envio']))})"
                   if clean_cell(current(item.columns.get("envio", ""))).strip() else "")
                for item in earlier))
        if is_new and previous is not None and any(
                clean_cell(current(previous.columns[kind])).strip()
                for kind in SHIPMENT_QUANTITY_KINDS if kind in previous.columns):
            st.button(f"📋 Usar las mismas cantidades del {previous.title}", key=f"shipment_copy_{token}",
                      on_click=copy_shipment_quantities, args=(namespace, identifier, previous, block))
        render_shipment_fields(namespace, identifier, block, token, previous, allow_range=True)


def invoice_case_type(sheet_name: str | None = None) -> str:
    selected_sheet = sheet_name or current_order_sheet()
    return next((case_type for case_type, order_sheet in CASE_ORDER_SHEETS.items()
                 if order_sheet == selected_sheet), "alineadores")


def verify_invoice_case(case_type: str, identifier: str) -> None:
    if case_type not in CASE_ORDER_SHEETS:
        raise ValueError("Selecciona un caso de Alineadores válido.")
    sheet_name = CASE_ORDER_SHEETS[case_type]
    read_order_values.clear(sheet_name)
    case_invoices.require_unique_case(read_orders_df(sheet_name), identifier, ID_COLUMN)


def render_case_search(current_user: str) -> None:
    def load_cases():
        frames = []
        for sheet_name in ORDER_TIME_SHEETS:
            try:
                orders = read_orders_df(sheet_name)
            except gspread.WorksheetNotFound:
                continue
            frames.append(case_search.source_cases(orders, invoice_case_type(sheet_name), ID_COLUMN))
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    case_search.render(current_user, namespace="alineadores", load_cases=load_cases,
                       verify_case=verify_invoice_case, rerun=rerun_active_tab)


def render_aligner_order_editor(row: pd.Series, current_user: str, definitions: dict) -> None:
    from aligners_grid import BUSINESS_ORDER

    view_only = not can_edit_orders(current_user)
    namespace = tracking_key("aligners")
    try:
        catalog = dict(get_order_form_catalog())
    except Exception:
        st.warning("No se pudieron cargar las listas de Sheets. Pulsa Actualizar datos para volver a intentarlo.")
        return
    candidate = order_detail.stage_source(row, namespace, ID_COLUMN, STATUS_COLUMN)
    catalog[STATUS_COLUMN] = list(dict.fromkeys([candidate.get(STATUS_COLUMN, ""),
        *get_allowed_next_statuses(candidate.get(PRODUCT_COLUMN, ""), candidate.get(STATUS_COLUMN, ""), definitions)]))
    manual_options = get_manual_stage_statuses(candidate.get(PRODUCT_COLUMN, ""), candidate.get(STATUS_COLUMN, ""),
                                               definitions, current_user)
    fields = order_detail.detail_columns(row.index if view_only else tracking_columns(row.index),
        aligners_editable_columns(row.index), BUSINESS_ORDER)
    # Plan y envíos no van en la tabla ni como campos sueltos: tienen su desplegable.
    plan, shipments = shipment_layout(row.index)
    in_shipments = set(block_columns(plan, shipments))
    if not view_only:
        fields = [column for column in fields if column not in in_shipments]
    identifier = str(row[ID_COLUMN])
    case_type = invoice_case_type()
    status = clean_cell(row.get(STATUS_COLUMN, "")).strip()
    labels = {column: f"{icon} {column}" for column, icon in NEW_ORDER_FIELD_ICONS.items()}
    labels.update({STATUS_COLUMN: "🚦 STATUS", "NOMBRE DOCTOR": "👩‍⚕️ NOMBRE DOCTOR",
                   "NOMBRE PACIENTE": "🙂 NOMBRE PACIENTE", "DETALLE COMENTARIOS": "📝 DETALLE COMENTARIOS"})
    labels.update({column: "📅 " + column.replace("_", " · ") for column in fields if "FECHA" in column})
    # La etapa se pinta como en la tabla y el encabezado de la ficha.
    palettes = {**order_dropdown_palettes(catalog),
                STATUS_COLUMN: {value: status_palette_value(value)
                                for value in dict.fromkeys([*catalog[STATUS_COLUMN], *manual_options])}}
    result = order_detail.render_editor(row, namespace=namespace, id_column=ID_COLUMN,
        columns=fields, catalog=catalog, labels=labels,
        option_label=lambda column, value: status_display_value(value, definitions) if column == STATUS_COLUMN else new_order_option_label(column, value),
        primary=BUSINESS_ORDER, required=(STATUS_COLUMN,), constrained=(STATUS_COLUMN,),
        expanded_catalog={STATUS_COLUMN: manual_options}, view_only=view_only,
        palettes=palettes, multiple=dropdown_fields.multiple_columns(catalog, pd.DataFrame([row]), MULTI_SELECT_COLUMNS),
        equivalent=values_equivalent, parse_date=parse_simple_date,
        format_date=lambda value: value.isoformat(), now=app_now,
        extra=(lambda: render_shipments_section(namespace, identifier, plan, shipments))
        if shipments and can_edit_orders(current_user) else None,
        section_extras={case_invoices.SERVICE_SECTION: lambda: case_invoices.render_section(
            case_type, identifier, current_user,
            verify_case=lambda: verify_invoice_case(case_type, identifier), rerun=rerun_active_tab)},
        hints={STATUS_COLUMN: f"El pedido está en pausa por **{status}**. Al resolverla podrás elegir la "
                              "etapa real a la que debe regresar."} if is_pause_status(status) else None)
    if result:
        baseline, delta = result
        source = pd.DataFrame([baseline])
        edited = pd.DataFrame([{**baseline, **delta}])
        target = (st.session_state.get(order_detail.draft_key(namespace), {}).get(identifier, {})
                  .get("manual_targets", {}).get(STATUS_COLUMN))
        if target and delta.get(STATUS_COLUMN) == target:
            edited.attrs["manualStageTargets"] = {identifier: target}
        saved, errors = save_workbench_changes(source, source, edited, current_user, definitions,
                                             select_catalog=catalog)
        if saved:
            order_detail.clear_drafts(namespace)
            st.session_state[tracking_key("aligners_selected_ids")] = list(dict.fromkeys(
                [*st.session_state.get(tracking_key("aligners_selected_ids"), []), *saved]))
            st.session_state[tracking_key("aligners_feedback")] = (saved, errors, f"{order_detail.order_title(row)} guardado.")
            rerun_active_tab()
        for error in errors:
            st.error(error)
    if view_only and status_only_here(current_user):
        render_status_only_change(row, current_user, definitions, catalog[STATUS_COLUMN])


def render_status_only_change(row: pd.Series, current_user: str, definitions: dict, options: list[str]) -> None:
    """Ficha de sólo lectura con un único cambio permitido: la etapa (Juan en Polanco/AERIS)."""
    identifier = clean_cell(row.get(ID_COLUMN, "")).strip()
    current = clean_cell(row.get(STATUS_COLUMN, "")).strip()
    choices = [option for option in options if status_key(option) != status_key(current)]
    if not choices:
        return
    target = st.selectbox("🚦 Cambiar etapa", choices, index=None, placeholder="Selecciona la nueva etapa",
                          format_func=lambda value: status_display_value(value, definitions),
                          key=tracking_key(f"aligners_status_only_{identifier}"))
    if st.button("Guardar etapa", type="primary", disabled=not target,
                 key=tracking_key(f"aligners_status_only_save_{identifier}")):
        source = pd.DataFrame([row])
        edited = source.copy()
        edited[STATUS_COLUMN] = target
        saved, errors = save_workbench_changes(source, source, edited, current_user, definitions)
        if saved:
            st.session_state[tracking_key("aligners_feedback")] = (saved, errors, f"{order_detail.order_title(row)} guardado.")
            rerun_active_tab()
        for error in errors:
            st.error(error)


def render_case_actions(
    selected: pd.DataFrame,
    times_df: pd.DataFrame,
    current_user: str,
    definitions: dict[str, ProcessDefinition],
    pending: bool,
) -> None:
    namespace = tracking_key("aligners")
    detail_rows = order_detail.rows_with_drafts(selected, namespace, ID_COLUMN)
    if detail_rows.empty:
        st.caption("☑️ Marca un pedido para editar su ficha; marca varios para aplicar acciones en lote.")
        return
    if pending:
        st.info("Guarda o descarta los cambios de la tabla antes de abrir acciones del pedido.")
        return
    if len(selected) > 1:
        st.markdown(f"### Pedidos seleccionados · {len(selected)}")
    form_pending = bool(order_detail.pending_count(namespace))
    if len(selected) > 1 and not form_pending and can_change_status_here(current_user):
        with st.expander("⚡ Cambiar etapa de los seleccionados", expanded=True):
            options = aligners_batch_stages(selected, definitions)
            target = st.selectbox("Etapa para todos", options, index=None,
                placeholder="Selecciona una etapa común", format_func=lambda value: status_display_value(value, definitions),
                key=tracking_key("aligners_bulk_stage"))
            st.caption("Sólo aparecen etapas y pausas permitidas para todos los pedidos marcados.")
            if st.button(f"Aplicar a {len(selected)} pedidos", disabled=not target,
                         key=tracking_key("aligners_bulk_apply")):
                edited = selected.copy()
                edited[STATUS_COLUMN] = target
                saved, errors = save_workbench_changes(selected, selected, edited, current_user, definitions)
                st.session_state[tracking_key("aligners_feedback")] = (saved, errors, "")
                rerun_active_tab()
    row = order_detail.choose_order(detail_rows, namespace, ID_COLUMN)
    if row is None:
        return
    identifier = clean_cell(row.get(ID_COLUMN, "")).strip()
    status = clean_cell(row.get(STATUS_COLUMN, "")).strip()
    product = clean_cell(row.get(PRODUCT_COLUMN, "")).strip()
    signal = clean_cell(row.get("SEMÁFORO", "")).strip()
    with st.container(border=True):
        order_detail.render_header(namespace, order_detail.order_title(row), [
            (status_display_value(status, definitions), status_palette_value(status)),
            (signal, ALERT_COLORS.get(signal, ALERT_COLORS["⚪ Sin medición"])),
        ], signal_detail(row.get("DETALLE SEMÁFORO", "")))
        render_aligner_order_editor(row, current_user, definitions)
        with st.expander("🛠️ Historial y opciones avanzadas", expanded=False):
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
            if can_edit_orders(current_user) and not form_pending and repairable and get_process_definition(product, definitions) is not None:
                st.caption("Úsalo si falta el inicio o hay registros inconsistentes. Mantiene la etapa actual, "
                           "cierra las mediciones abiertas e inicia una nueva desde ahora. "
                           "No recupera el tiempo transcurrido anteriormente; la acción queda en el historial.")
                if st.button(
                    "⏱️ Iniciar conteo de esta etapa desde ahora",
                    key=tracking_key(f"repair_aligner_{identifier}"),
                    help="No cambia el status; cierra mediciones incoherentes y comienza a contar desde ahora.",
                ):
                    try:
                        initialize_order_timer(row, current_user, definitions)
                        reset_workbench()
                        st.session_state[tracking_key("aligners_feedback")] = (
                            [identifier],
                            [],
                            "Medición iniciada en la etapa actual.",
                        )
                        rerun_active_tab()
                    except Exception as exc:
                        st.error(f"No se pudo iniciar la medición: {exc}")

            st.markdown("#### 🕘 Historial del pedido")
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
                st.info(f"Este pedido todavía no tiene historial en {current_times_sheet()}.")
            else:
                st.dataframe(
                    history[history_columns].iloc[::-1],
                    hide_index=True,
                    use_container_width=True,
                )

            st.markdown("#### 📋 Todos los datos de la hoja")
            excluded = {*COMPUTED_COLUMNS, "_SHEET_ROW"}
            details = [
                {"Campo": column, "Valor": clean_cell(value)}
                for column, value in row.items()
                if column not in excluded and clean_cell(value).strip()
            ]
            st.dataframe(pd.DataFrame(details), hide_index=True, use_container_width=True)


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
            key=tracking_key("aligners_refresh"),
        ):
            st.session_state.pop("aligners_form_catalogs", None)
            read_order_form_catalogs.clear(configured_spreadsheet_id())
            clear_sheet_data_cache()
            reset_workbench()
            rerun_active_tab()

    feedback = st.session_state.pop(tracking_key("aligners_feedback"), None)
    if feedback:
        saved, errors, message = feedback
        if saved:
            st.toast(message or "Guardado: " + ", ".join(saved), icon="✅")
        for error in errors:
            st.error(error)

    if status_only_here(current_user):
        st.caption(STATUS_ONLY_CAPTION)
    elif not can_edit_orders(current_user):
        st.caption("👁️ Puedes consultar los pedidos y agregar nuevos; los pedidos existentes son de sólo lectura.")
    with st.expander("➕ Nueva orden", expanded=False,
                     key=tracking_key("aligners_new_expander"), on_change="rerun") as new_order:
        if new_order.open:
            if pending_before_grid:
                st.info("Guarda o descarta los cambios de la tabla antes de crear una orden.")
            else:
                render_new_order(current_user, snapshot)

    signal = render_metrics(table, interactive=True, disabled=pending_before_grid)
    st.caption(
        "Selecciona una tarjeta para filtrar · Verde: menos del 80% del plazo · Amarillo: 80–99% · Rojo: plazo agotado · "
        "Morado: pausa opcional · Gris: falta iniciar o reparar la medición. "
        "Los tiempos cuentan de lunes a viernes."
    )
    render_active_orders(current_user, snapshot, signal, stored_pending)
    render_sent_archive(current_user, snapshot)


def render_active_orders(
    current_user: str,
    snapshot: dict[str, Any],
    signal: str,
    stored_pending: int,
) -> None:
    """Tabla editable de pedidos activos, con filtros y acciones por pedido."""
    definitions: dict[str, ProcessDefinition] = snapshot["definitions"]
    table: pd.DataFrame = snapshot["table"]
    pending_before_grid = stored_pending > 0
    if table.empty:
        st.info(
            "No hay pedidos activos. Los enviados se consultan abajo, en el histórico de "
            "Enviados; los cancelados permanecen archivados en la hoja."
        )
        return

    bar_text = (
        f"✏️ {stored_pending} pedidos con cambios · Guarda o descarta antes de filtrar."
        if pending_before_grid
        else "✨ Marca un pedido en Abrir para editarlo en una ficha amplia debajo de la tabla."
    )
    st.markdown(
        f'<div class="align-edit-bar {"is-pending" if pending_before_grid else "is-ready"}">{bar_text}</div>',
        unsafe_allow_html=True,
    )

    filters = st.columns([2.2, 1.5, 1.5, 1.3])
    search = filters[0].text_input(
        "Buscar pedido",
        placeholder="Orden, doctor, paciente o producto",
        disabled=pending_before_grid,
        key=tracking_key("aligners_search"),
    )
    product_options = sorted(
        {clean_cell(value).strip() for value in table.get(PRODUCT_COLUMN, []) if clean_cell(value).strip()}
    )
    products = filters[1].multiselect(
        "Producto",
        product_options,
        disabled=pending_before_grid,
        key=tracking_key("aligners_products"),
    )
    status_options = sorted(
        {clean_cell(value).strip() for value in table.get(STATUS_COLUMN, []) if clean_cell(value).strip()}
    )
    statuses = filters[2].multiselect(
        "Status",
        status_options,
        disabled=pending_before_grid,
        format_func=lambda value: status_display_value(value, definitions),
        key=tracking_key("aligners_statuses"),
    )
    order_mode = filters[3].selectbox(
        "Orden",
        ["Orden de la hoja", "Atender urgentes primero"],
        disabled=pending_before_grid,
        key=tracking_key("aligners_order_mode"),
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
        key=tracking_key("aligners_hide_automatic"),
    )
    legend = (
        '<div class="align-column-legend"><span class="readonly">🔒 Fijas: No. Orden, Semáforo y columnas principales</span>'
        '<span class="editable">✎ Editable manual</span>'
        '<span class="auto-editable">✎⚙ Editable automática</span>'
        '<span class="automatic">⚙ Cálculo automático</span></div>'
    )
    visibility[1].markdown(legend if can_edit_orders(current_user) else
        STATUS_ONLY_LEGEND if status_only_here(current_user) else '<div class="align-column-legend"><span class="readonly">🔒 Pedidos de sólo lectura</span></div>', unsafe_allow_html=True)
    st.caption(
        f"{len(filtered)} de {len(table)} pedidos · Cada fila sólo ofrece su siguiente etapa normal, "
        "las pausas aplicables y Cancelado. En una pausa se habilitan las etapas para reanudar."
        + (" Para corregir, «Ver todas las etapas…» dentro de STATUS ofrece todo el flujo."
           if current_user in MANUAL_STAGE_USERS else "")
    )
    if filtered.empty:
        st.info("No hay pedidos que coincidan con los filtros.")
        return

    visible_columns = tracking_columns(filtered.columns)
    source_grid = filtered[visible_columns].copy()
    grid = display_workbench_df(source_grid, definitions)
    grid.insert(0, "SELECCIONAR", grid[ID_COLUMN].isin(st.session_state.get(tracking_key("aligners_selected_ids"), [])))
    signature = hashlib.sha256(
        json.dumps(
            [
                current_user,
                st.session_state.get(tracking_key("aligners_revision"), 0),
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
    key = tracking_key(f"aligners_grid_{signature}")
    st.session_state[tracking_key("aligners_editor_key")] = key
    st.session_state[tracking_key("aligners_editor_baseline")] = grid.copy()

    hidden: set[str] = set()
    if hide_automatic:
        hidden |= AUTOMATIC_COLUMNS
    catalog = tracking_form_catalog()
    config_key = tracking_key("aligners_grid_configuration")
    cached = st.session_state.get(config_key)
    if not cached or cached[0] != key:
        cached = (key, build_grid_configuration(grid, source_grid, definitions,
            hidden_columns=hidden, select_catalog=catalog, current_user=current_user))
        st.session_state[config_key] = cached
    options = deepcopy(cached[1])
    form_pending = bool(order_detail.pending_count(tracking_key("aligners")))
    order_detail.lock_grid(options, form_pending)
    edited = render_aligners_grid(grid, options, key)
    try:
        changes = grid_changes(grid, edited, definitions)
    except ValueError as exc:
        st.error(str(exc))
        changes = []
    pending = bool(changes)
    # Guardar reconstruye ambas tablas; no debe borrar en silencio lo elegido en Enviados.
    sent_pending = sent_change_count(definitions) > 0

    save_column, discard_column, count_column, _ = st.columns([1.3, 1.3, 1.8, 4])
    if save_column.button(
        "Guardar cambios",
        type="primary",
        disabled=not can_change_status_here(current_user) or not changes or sent_pending or form_pending,
        use_container_width=True,
        key=tracking_key("aligners_save"),
        help=(
            "Primero guarda o descarta los cambios del histórico de Enviados."
            if sent_pending
            else None
        ),
    ):
        saved, errors = save_workbench_changes(
            filtered, grid, edited, current_user, definitions, select_catalog=catalog
        )
        st.session_state[tracking_key("aligners_feedback")] = (saved, errors, "")
        rerun_active_tab()
    if discard_column.button(
        "Descartar cambios",
        disabled=not pending_before_grid and not pending,
        use_container_width=True,
        key=tracking_key("aligners_discard"),
    ):
        order_detail.clear_drafts(tracking_key("aligners"))
        reset_workbench()
        rerun_active_tab()
    count_column.caption(f"{len(changes) + order_detail.pending_count(tracking_key('aligners'))} pedidos con cambios pendientes")

    selected_ids = edited.loc[edited["SELECCIONAR"].eq(True), ID_COLUMN]
    st.session_state[tracking_key("aligners_selected_ids")] = selected_ids.tolist()
    order_detail.note_selection(selected_ids.tolist(), tracking_key("aligners"), edited.attrs.get("activeId"))
    selected = filtered[filtered[ID_COLUMN].isin(selected_ids)]
    render_case_actions(
        selected,
        snapshot["times"],
        current_user,
        definitions,
        pending or sent_pending,
    )


def render_sent_archive(current_user: str, snapshot: dict[str, Any], title_suffix: str = "") -> None:
    """Histórico de enviados; cambiar la etapa devuelve el pedido a la mesa activa."""
    definitions: dict[str, ProcessDefinition] = snapshot["definitions"]
    sent: pd.DataFrame = snapshot.get("sent", pd.DataFrame())
    main_pending = (editor_change_count("aligners", definitions) > 0
                    or order_detail.pending_count(tracking_key("aligners")) > 0)
    sent_pending = sent_change_count(definitions) > 0
    expander_key = tracking_key("aligners_sent_expander")
    if sent_pending and not st.session_state.get(expander_key):
        # Sin esto, cerrar el expander desmontaría la grilla y las etapas elegidas
        # se perderían en silencio. El servidor lo mantiene abierto (el navegador
        # puede dibujarlo cerrado) hasta guardar o descartar.
        st.session_state[expander_key] = True
        st.toast("Guarda o descarta los cambios antes de cerrar Enviados.", icon="⚠️")
    # on_change="rerun": la grilla se monta con el expander ya abierto, no con ancho cero.
    with st.expander(
        f"🚚 Enviados{title_suffix} · {len(sent)} pedido(s)",
        expanded=False,
        key=expander_key,
        on_change="rerun",
    ) as archive:
        if not archive.open:
            return
        st.caption(
            "Histórico de pedidos enviados. Cambia la etapa de un pedido y pulsa "
            "Guardar y reactivar: regresará a los pedidos activos de arriba."
        )
        if sent.empty:
            st.info("Todavía no hay pedidos enviados.")
            return

        filters = st.columns([2.2, 1.5])
        search = filters[0].text_input(
            "Buscar pedido",
            placeholder="Orden, doctor, paciente o producto",
            disabled=sent_pending,
            key=tracking_key("aligners_sent_search"),
        )
        product_options = sorted(
            {clean_cell(value).strip() for value in sent.get(PRODUCT_COLUMN, []) if clean_cell(value).strip()}
        )
        products = filters[1].multiselect(
            "Producto",
            product_options,
            disabled=sent_pending,
            key=tracking_key("aligners_sent_products"),
        )
        filtered = filter_tracking_table(
            sent,
            search=search,
            signal="Todos",
            products=products,
            statuses=[],
            order_mode="Orden de la hoja",
        )
        st.caption(
            f"{len(filtered)} de {len(sent)} pedidos enviados · Sólo se edita la etapa; "
            "se ofrece cualquier etapa o pausa del producto excepto Enviado."
        )
        if filtered.empty:
            st.info("No hay pedidos enviados que coincidan con la búsqueda.")
            return

        source_grid = filtered[tracking_columns(filtered.columns)].copy()
        grid = display_workbench_df(source_grid, definitions)
        signature = hashlib.sha256(
            json.dumps(
                [
                    current_user,
                    st.session_state.get(tracking_key("aligners_revision"), 0),
                    st.session_state.get(tracking_key("aligners_sent_revision"), 0),
                    search,
                    products,
                ],
                ensure_ascii=False,
                sort_keys=True,
            ).encode()
        ).hexdigest()[:16]
        key = tracking_key(f"aligners_sent_grid_{signature}")
        st.session_state[tracking_key("aligners_sent_editor_key")] = key
        st.session_state[tracking_key("aligners_sent_editor_baseline")] = grid.copy()

        options = build_grid_configuration(grid, source_grid, definitions, reactivate=True, current_user=current_user)
        order_detail.lock_grid(options, bool(order_detail.pending_count(tracking_key("aligners"))))
        edited = render_aligners_grid(grid, options, key)
        baseline, reactivated = without_repeated_orders(grid, edited)
        try:
            changes = grid_changes(baseline, reactivated, definitions)
        except ValueError as exc:
            st.error(str(exc))
            changes = []

        save_column, discard_column, count_column = st.columns([1.5, 1.3, 3.2])
        if save_column.button(
            "Guardar y reactivar",
            type="primary",
            # Guardar reconstruye ambas tablas; no debe borrar ediciones de arriba.
            disabled=not can_edit_orders(current_user) or not changes or main_pending,
            use_container_width=True,
            key=tracking_key("aligners_sent_save"),
        ):
            saved, errors = save_workbench_changes(
                filtered, baseline, reactivated, current_user, definitions, reactivate=True
            )
            message = (
                f"Reactivado: {', '.join(saved)} · ya aparece en los pedidos activos."
                if saved
                else ""
            )
            st.session_state[tracking_key("aligners_feedback")] = (saved, errors, message)
            rerun_active_tab()
        if discard_column.button(
            "Descartar cambios",
            disabled=not changes and not sent_pending,
            use_container_width=True,
            key=tracking_key("aligners_sent_discard"),
        ):
            discard_sent_changes()
            rerun_active_tab()
        if main_pending:
            count_column.caption(
                "Guarda o descarta primero los cambios de la tabla de pedidos activos."
            )
        else:
            count_column.caption(f"{len(changes)} pedidos por reactivar")


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
        "Historial y opciones avanzadas → Iniciar conteo de esta etapa desde ahora. "
        "Desde ese momento la app contará el plazo de la etapa actual."
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


# ==============================
# Seguimiento General
# ==============================
def load_general_sources(current_user: str) -> tuple[list[dict[str, Any]], list[str]]:
    """Lee las tres hojas con el mismo snapshot que usan sus pestañas.

    Una hoja que falla queda fuera con su motivo, sin tapar a las demás.
    """

    sources: list[dict[str, Any]] = []
    problems: list[str] = []
    for origin, sheet in GENERAL_ORIGINS:
        with order_sheet_scope(sheet):
            try:
                ensure_times_headers()
                snapshot = get_snapshot(current_user)
            except gspread.WorksheetNotFound:
                problems.append(f"{origin}: no se encontró la hoja {sheet} en Control ALINEADORES.")
                continue
            except Exception as exc:
                problems.append(f"{origin}: no se pudo leer la hoja {sheet} ({exc}).")
                continue
            columns = tracking_columns(snapshot["table"].columns)
        sources.append({"origin": origin, "sheet": sheet, "snapshot": snapshot, "columns": columns})
    return sources, problems


def merge_column_orders(orders: Iterable[list[str]]) -> list[str]:
    """Une las columnas de cada hoja respetando su orden; las propias van junto a sus vecinas."""

    merged: list[str] = []
    for columns in orders:
        previous = None
        for column in columns:
            if column not in merged:
                position = merged.index(previous) + 1 if previous in merged else len(merged)
                merged.insert(position, column)
            previous = column
    return merged


def general_row_key(origin: str, identifier: Any) -> str:
    """El número de orden puede repetirse entre hojas; el origen lo hace único."""
    return f"{origin}|{clean_cell(identifier).strip()}"


def build_general_table(sources: list[dict[str, Any]], key: str = "table") -> pd.DataFrame:
    """Une las órdenes de las tres hojas con la columna ORIGEN."""

    frames = []
    for source in sources:
        table = source["snapshot"].get(key, pd.DataFrame())
        if table is None or table.empty:
            continue
        frame = table.copy()
        frame.insert(0, ORIGIN_COLUMN, source["origin"])
        frame[GENERAL_ROW_KEY_COLUMN] = [
            general_row_key(source["origin"], identifier) for identifier in frame[ID_COLUMN]
        ]
        frames.append(frame)
    if not frames:
        return pd.DataFrame()
    # Cada hoja tiene columnas propias (AERIS no usa TIPO, Polanco lleva ADEUDO).
    return pd.concat(frames, ignore_index=True, sort=False).fillna("")


def show_general_feedback(sources: list[dict[str, Any]]) -> None:
    """Los guardados de la ficha y de Nueva orden avisan en la hoja donde ocurrieron."""

    for source in sources:
        with order_sheet_scope(source["sheet"]):
            feedback = st.session_state.pop(tracking_key("aligners_feedback"), None)
        if not feedback:
            continue
        saved, errors, message = feedback
        if saved:
            st.toast(f"{source['origin']} · " + (message or "Guardado: " + ", ".join(saved)), icon="✅")
        for error in errors:
            st.error(f"{source['origin']} · {error}")


def general_pending_count(sources: list[dict[str, Any]]) -> int:
    """Cambios sin guardar de la tabla general, de las fichas y de Enviados de cada hoja."""
    total = general_grid_change_count()
    for source in sources:
        with order_sheet_scope(source["sheet"]):
            total += pending_change_count(source["snapshot"]["definitions"])
    return total


def refresh_general_sources(sources: list[dict[str, Any]]) -> None:
    st.session_state.pop("aligners_form_catalogs", None)
    read_order_form_catalogs.clear(configured_spreadsheet_id())
    for source in sources:
        with order_sheet_scope(source["sheet"]):
            clear_sheet_data_cache()
            reset_workbench()


def set_individual_tracking_hidden() -> None:
    """Guarda la elección del interruptor y la deja en el enlace para recargar igual."""

    hidden = bool(st.session_state.get(GENERAL_HIDE_TOGGLE_KEY))
    st.session_state[GENERAL_HIDE_STATE_KEY] = hidden
    if hidden:
        st.query_params[GENERAL_HIDE_PARAM] = GENERAL_HIDE_PARAM_VALUE
    elif GENERAL_HIDE_PARAM in st.query_params:
        del st.query_params[GENERAL_HIDE_PARAM]


def individual_tracking_hidden() -> bool:
    if GENERAL_HIDE_STATE_KEY not in st.session_state:
        st.session_state[GENERAL_HIDE_STATE_KEY] = (
            normalize_text(get_query_param_value(GENERAL_HIDE_PARAM))
            == normalize_text(GENERAL_HIDE_PARAM_VALUE)
        )
    return bool(st.session_state[GENERAL_HIDE_STATE_KEY])


def render_general_new_order(current_user: str, sources: list[dict[str, Any]], pending: int) -> None:
    """Nueva orden: primero se elige a qué seguimiento pertenece."""

    with st.expander("➕ Nueva orden", expanded=False,
                     key=f"{GENERAL_KEY_PREFIX}new_expander", on_change="rerun") as new_order:
        if not new_order.open:
            return
        if pending:
            st.info("Guarda o descarta los cambios de la tabla antes de crear una orden.")
            return
        origins = [source["origin"] for source in sources]
        origin = st.segmented_control(
            "¿A qué seguimiento pertenece la orden?",
            origins,
            key=f"{GENERAL_KEY_PREFIX}new_origin",
        )
        st.caption(
            "La orden se guarda en la hoja de ese origen: Polanco y AERIS conservan su "
            "registro aparte para su contabilidad y facturación."
        )
        if not origin:
            st.info("Elige el origen para capturar la orden.")
            return
        source = next(source for source in sources if source["origin"] == origin)
        with order_sheet_scope(source["sheet"]):
            render_new_order(current_user, source["snapshot"])


def general_grid_change_count() -> int:
    """Filas editadas en la tabla general que todavía no se guardan."""

    key = st.session_state.get(GENERAL_EDITOR_KEY_STATE, "")
    state = st.session_state.get(key) if key else None
    baseline = st.session_state.get(GENERAL_EDITOR_BASELINE_STATE)
    if baseline is None or not state:
        return 0
    edited = response_frame(baseline, state, GENERAL_ROW_KEY_COLUMN)
    if len(edited) != len(baseline):
        return 1
    columns = [column for column in baseline.columns if column != "SELECCIONAR"]
    before = baseline[columns].fillna("").astype(str).reset_index(drop=True)
    after = edited[columns].fillna("").astype(str).reset_index(drop=True)
    return int(before.ne(after).any(axis=1).sum())


def general_grid_options(
    grid: pd.DataFrame,
    filtered: pd.DataFrame,
    sources: list[dict[str, Any]],
    definitions: dict[str, ProcessDefinition],
    current_user: str,
    hidden: set[str],
) -> dict:
    """Mismo editor que cada pestaña; cada fila usa las reglas y columnas de su hoja."""

    stages: dict[str, list[str]] = {}
    manual_stages: dict[str, list[str]] = {}
    selections: dict[str, list[str]] = {}
    multiple: set[str] = set()
    dates: dict[str, dict[str, str]] = {}
    palettes: dict[str, dict[str, Any]] = {}
    editable: set[str] = set()
    row_editable: dict[str, list[str]] = {}
    for source in sources:
        mask = filtered[ORIGIN_COLUMN] == source["origin"]
        if not mask.any():
            continue
        own = [column for column in source["columns"] if column in grid.columns]
        with order_sheet_scope(source["sheet"]):
            parts = grid_configuration_parts(
                grid.loc[mask, own], filtered.loc[mask], definitions,
                select_catalog=tracking_form_catalog(), current_user=current_user,
            )
        for _, row in filtered.loc[mask].iterrows():
            row_key, identifier = row[GENERAL_ROW_KEY_COLUMN], row[ID_COLUMN]
            # STATUS se habilita por columna; sin permiso en esta hoja la fila no ofrece etapas.
            stages[row_key] = (parts["stages"].get(identifier, [])
                               if STATUS_COLUMN in parts["editable"] else [])
            manual_stages[row_key] = parts["manual_stages"].get(identifier, [])
            row_editable[row_key] = sorted(parts["editable"] & set(own))
        for column, values in parts["selections"].items():
            selections[column] = list(dict.fromkeys([*selections.get(column, []), *values]))
        multiple |= set(parts["multiple"])
        for column, values in parts["dates"].items():
            dates.setdefault(column, {}).update(values)
        for column, values in parts["palettes"].items():
            palettes.setdefault(column, {}).update(values)
        editable |= parts["editable"] & set(own)
    options = build_aligners_grid_options(
        grid,
        editable=editable,
        automatic=AUTOMATIC_COLUMNS,
        stage_options=stages,
        select_options=selections,
        date_values=dates,
        palettes=palettes,
        time_zone=APP_TIMEZONE_NAME,
        hidden_columns=hidden | {GENERAL_ROW_KEY_COLUMN},
        manual_stage_options=manual_stages,
        id_column=GENERAL_ROW_KEY_COLUMN,
        row_editable=row_editable,
    )
    return decorate_order_grid(options, selections, grid, multiple)


def split_general_changes(
    grid: pd.DataFrame,
    edited: pd.DataFrame,
    filtered: pd.DataFrame,
    sources: list[dict[str, Any]],
    definitions: dict[str, ProcessDefinition],
) -> tuple[list[dict[str, Any]], list[str]]:
    """Separa lo editado por hoja: cada origen se valida y guarda con su propio número de orden."""

    manual_targets = edited.attrs.get("manualStageTargets", {})
    batches: list[dict[str, Any]] = []
    errors: list[str] = []
    for source in sources:
        mask = grid[ORIGIN_COLUMN] == source["origin"]
        if not mask.any():
            continue
        own = [column for column in source["columns"] if column in grid.columns]
        baseline = grid.loc[mask, own].reset_index(drop=True)
        part = edited.loc[edited[ORIGIN_COLUMN] == source["origin"]]
        changed = part[own].reset_index(drop=True)
        keys = dict(zip(part[GENERAL_ROW_KEY_COLUMN].astype(str), part[ID_COLUMN]))
        changed.attrs["manualStageTargets"] = {
            keys[row_key]: target for row_key, target in manual_targets.items() if row_key in keys
        }
        try:
            changes = grid_changes(baseline, changed, definitions)
        except ValueError as exc:
            errors.append(f"{source['origin']} · {exc}")
            continue
        if changes:
            batches.append({"source": source, "rows": filtered.loc[mask], "baseline": baseline,
                            "edited": changed, "changes": changes})
    return batches, errors


def reset_general_grid() -> None:
    """Tabla nueva y limpia: lo editado en la anterior ya se guardó o se descartó."""
    st.session_state.pop(GENERAL_EDITOR_KEY_STATE, None)
    st.session_state.pop(GENERAL_EDITOR_BASELINE_STATE, None)
    st.session_state[GENERAL_REVISION_KEY] = st.session_state.get(GENERAL_REVISION_KEY, 0) + 1


def discard_general_changes(sources: list[dict[str, Any]]) -> None:
    for source in sources:
        with order_sheet_scope(source["sheet"]):
            order_detail.clear_drafts(tracking_key("aligners"))
            reset_workbench()
    reset_general_grid()


def any_status_access(current_user: str, sources: list[dict[str, Any]]) -> bool:
    return any(can_change_status(current_user, source["sheet"]) for source in sources)


STATUS_ONLY_LEGEND = ('<div class="align-column-legend"><span class="editable">✎ Sólo puedes cambiar STATUS'
                      '</span><span class="readonly">🔒 Lo demás es de sólo lectura</span></div>')
STATUS_ONLY_CAPTION = ("👁️ Puedes consultar los pedidos, agregar nuevos y cambiar la etapa (STATUS) de Polanco y "
                       "AERIS; lo demás es de sólo lectura.")


def render_general_orders(current_user: str, sources: list[dict[str, Any]], stored_pending: int) -> None:
    """Tabla única y editable con las órdenes activas; cada cambio va a la hoja de su origen."""

    definitions: dict[str, ProcessDefinition] = sources[0]["snapshot"]["definitions"]
    pending_before_grid = stored_pending > 0
    table = build_general_table(sources)
    signal = render_metrics(table, interactive=True, disabled=pending_before_grid,
                            key_prefix=GENERAL_KEY_PREFIX)
    st.caption(
        "Selecciona una tarjeta para filtrar · Verde: menos del 80% del plazo · Amarillo: 80–99% · "
        "Rojo: plazo agotado · Morado: pausa opcional · Gris: falta iniciar o reparar la medición. "
        "Los tiempos cuentan de lunes a viernes."
    )
    if table.empty:
        st.info(
            "No hay pedidos activos. Los enviados se consultan abajo, en el histórico de "
            "Enviados de cada origen."
        )
        return

    bar_text = (
        f"✏️ {stored_pending} pedidos con cambios · Guarda o descarta antes de filtrar."
        if pending_before_grid
        else "✨ Edita directo en la tabla o marca un pedido en Abrir para verlo en una ficha amplia debajo."
    )
    st.markdown(
        f'<div class="align-edit-bar {"is-pending" if pending_before_grid else "is-ready"}">{bar_text}</div>',
        unsafe_allow_html=True,
    )
    filters = st.columns([1.6, 2.2, 1.5, 1.5, 1.3])
    origin_filter = filters[0].selectbox(
        "Origen",
        [GENERAL_ALL_ORIGINS, *(source["origin"] for source in sources)],
        disabled=pending_before_grid,
        key=f"{GENERAL_KEY_PREFIX}origin_filter",
    )
    search = filters[1].text_input(
        "Buscar pedido",
        placeholder="Orden, doctor, paciente o producto",
        disabled=pending_before_grid,
        key=f"{GENERAL_KEY_PREFIX}search",
    )
    scoped = table if origin_filter == GENERAL_ALL_ORIGINS else table[table[ORIGIN_COLUMN] == origin_filter]
    product_options = sorted(
        {clean_cell(value).strip() for value in scoped.get(PRODUCT_COLUMN, []) if clean_cell(value).strip()}
    )
    products = filters[2].multiselect("Producto", product_options, disabled=pending_before_grid,
                                      key=f"{GENERAL_KEY_PREFIX}products")
    status_options = sorted(
        {clean_cell(value).strip() for value in scoped.get(STATUS_COLUMN, []) if clean_cell(value).strip()}
    )
    statuses = filters[3].multiselect(
        "Status",
        status_options,
        disabled=pending_before_grid,
        format_func=lambda value: status_display_value(value, definitions),
        key=f"{GENERAL_KEY_PREFIX}statuses",
    )
    order_mode = filters[4].selectbox(
        "Orden",
        ["Orden de la hoja", "Atender urgentes primero"],
        disabled=pending_before_grid,
        key=f"{GENERAL_KEY_PREFIX}order_mode",
    )
    filtered = filter_tracking_table(
        scoped, search=search, signal=signal, products=products, statuses=statuses, order_mode=order_mode,
    )

    visibility = st.columns([1.7, 6.7])
    hide_automatic = visibility[0].checkbox(
        "Ocultar automáticas",
        disabled=pending_before_grid,
        key=f"{GENERAL_KEY_PREFIX}hide_automatic",
    )
    visibility[1].markdown(
        '<div class="align-column-legend"><span class="readonly">🔒 Fijas: Origen, No. Orden, Semáforo y columnas principales</span>'
        '<span class="editable">✎ Editable manual</span>'
        '<span class="auto-editable">✎⚙ Editable automática</span>'
        '<span class="automatic">⚙ Cálculo automático</span></div>'
        if can_edit_orders(current_user) else STATUS_ONLY_LEGEND if any_status_access(current_user, sources) else
        '<div class="align-column-legend"><span class="readonly">🔒 Pedidos de sólo lectura</span></div>',
        unsafe_allow_html=True,
    )
    st.caption(
        f"{len(filtered)} de {len(table)} pedidos activos · Cada cambio se guarda en la hoja de su "
        "**Origen**. Cada fila sólo ofrece su siguiente etapa normal, las pausas aplicables y Cancelado."
        + (" Para corregir, «Ver todas las etapas…» dentro de STATUS ofrece todo el flujo."
           if current_user in MANUAL_STAGE_USERS else "")
    )
    if filtered.empty:
        st.info("No hay pedidos que coincidan con los filtros.")
        return

    # Una hoja sin pedidos activos no aporta filas, aunque tenga columnas propias.
    columns = [column for column in merge_column_orders(source["columns"] for source in sources)
               if column in table.columns]
    grid = display_workbench_df(filtered[[ORIGIN_COLUMN, *columns]], definitions)
    selected_state = f"{GENERAL_KEY_PREFIX}selected"
    known = set(table[GENERAL_ROW_KEY_COLUMN])
    selected_keys = [key for key in st.session_state.get(selected_state, []) if key in known]
    grid.insert(0, "SELECCIONAR", filtered[GENERAL_ROW_KEY_COLUMN].isin(selected_keys).to_numpy())
    grid[GENERAL_ROW_KEY_COLUMN] = filtered[GENERAL_ROW_KEY_COLUMN].to_numpy()
    revisions = [st.session_state.get(GENERAL_REVISION_KEY, 0)]
    for source in sources:
        with order_sheet_scope(source["sheet"]):
            revisions.append(st.session_state.get(tracking_key("aligners_revision"), 0))
    # Con datos o filtros nuevos la tabla se vuelve a dibujar con lo marcado guardado.
    signature = hashlib.sha256(json.dumps(
        [current_user, revisions, origin_filter, search, signal, products, statuses, order_mode,
         hide_automatic],
        ensure_ascii=False, sort_keys=True,
    ).encode()).hexdigest()[:16]
    key = f"{GENERAL_KEY_PREFIX}aligners_grid_{signature}"
    st.session_state[GENERAL_EDITOR_KEY_STATE] = key
    st.session_state[GENERAL_EDITOR_BASELINE_STATE] = grid.copy()

    config_key = f"{GENERAL_KEY_PREFIX}aligners_grid_configuration"
    cached = st.session_state.get(config_key)
    if not cached or cached[0] != key:
        cached = (key, general_grid_options(grid, filtered, sources, definitions, current_user,
                                            AUTOMATIC_COLUMNS if hide_automatic else set()))
        st.session_state[config_key] = cached
    options = deepcopy(cached[1])
    namespaces = []
    for source in sources:
        with order_sheet_scope(source["sheet"]):
            namespaces.append(tracking_key("aligners"))
    form_pending = any(order_detail.pending_count(namespace) for namespace in namespaces)
    order_detail.lock_grid(options, form_pending)
    edited = render_aligners_grid(grid, options, key, id_column=GENERAL_ROW_KEY_COLUMN)
    batches, change_errors = split_general_changes(grid, edited, filtered, sources, definitions)
    for error in change_errors:
        st.error(error)
    change_count = sum(len(batch["changes"]) for batch in batches)
    sent_pending = False
    for source in sources:
        with order_sheet_scope(source["sheet"]):
            sent_pending = sent_pending or sent_change_count(source["snapshot"]["definitions"]) > 0

    save_column, discard_column, count_column, _ = st.columns([1.3, 1.3, 1.8, 4])
    if save_column.button(
        "Guardar cambios",
        type="primary",
        disabled=not any_status_access(current_user, sources) or not batches or sent_pending or form_pending,
        width="stretch",
        key=f"{GENERAL_KEY_PREFIX}aligners_save",
        help="Primero guarda o descarta los cambios del histórico de Enviados." if sent_pending else None,
    ):
        for batch in batches:
            source = batch["source"]
            with order_sheet_scope(source["sheet"]):
                saved, errors = save_workbench_changes(
                    batch["rows"], batch["baseline"], batch["edited"], current_user,
                    source["snapshot"]["definitions"], select_catalog=tracking_form_catalog(),
                )
                st.session_state[tracking_key("aligners_feedback")] = (saved, errors, "")
        reset_general_grid()
        rerun_active_tab()
    if discard_column.button(
        "Descartar cambios",
        disabled=not pending_before_grid and not change_count,
        width="stretch",
        key=f"{GENERAL_KEY_PREFIX}aligners_discard",
    ):
        discard_general_changes(sources)
        rerun_active_tab()
    drafts = sum(order_detail.pending_count(namespace) for namespace in namespaces)
    count_column.caption(f"{change_count + drafts} pedidos con cambios pendientes")

    checked = edited.loc[edited["SELECCIONAR"].eq(True), GENERAL_ROW_KEY_COLUMN].astype(str).tolist()
    visible_keys = set(grid[GENERAL_ROW_KEY_COLUMN])
    # Lo marcado que quedó fuera por un filtro sigue abierto.
    selected_keys = [key for key in selected_keys if key not in visible_keys] + checked
    st.session_state[selected_state] = selected_keys
    active_key = edited.attrs.get("activeId")

    shown = 0
    for source, namespace in zip(sources, namespaces):
        snapshot = source["snapshot"]
        origin_rows = table[(table[ORIGIN_COLUMN] == source["origin"])
                            & table[GENERAL_ROW_KEY_COLUMN].isin(selected_keys)]
        with order_sheet_scope(source["sheet"]):
            if origin_rows.empty and not order_detail.pending_count(namespace):
                continue
            source_table = snapshot["table"]
            selected = source_table[source_table[ID_COLUMN].isin(origin_rows[ID_COLUMN])]
            active = origin_rows.loc[origin_rows[GENERAL_ROW_KEY_COLUMN] == active_key, ID_COLUMN]
            order_detail.note_selection(selected[ID_COLUMN].tolist(), namespace,
                                        active.iloc[0] if not active.empty else None)
            st.markdown(f"#### {source['origin']}")
            render_case_actions(
                selected,
                snapshot["times"],
                current_user,
                snapshot["definitions"],
                bool(batches) or sent_change_count(snapshot["definitions"]) > 0,
            )
        shown += 1
    if not shown:
        st.caption("☑️ Marca Abrir en un pedido para ver su ficha; marca varios del mismo origen para "
                   "cambiarles la etapa en lote.")


def render_general_workbench(current_user: str) -> None:
    """Pestaña Seguimiento General: Seguimiento, Polanco y AERIS en una sola vista."""

    heading, toggle = st.columns([5, 2])
    heading.caption(
        "Órdenes de Seguimiento, Polanco y AERIS en una sola tabla. La columna **Origen** dice "
        "de qué hoja viene cada una; al guardar, cada cambio va a su hoja original."
    )
    toggle.toggle(
        "Ocultar pestañas individuales",
        value=individual_tracking_hidden(),
        key=GENERAL_HIDE_TOGGLE_KEY,
        on_change=set_individual_tracking_hidden,
        help="Oculta Seguimiento, Polanco y AERIS y deja sólo esta vista general. "
             "Desactívalo aquí para mostrarlas otra vez.",
    )
    sources, problems = load_general_sources(current_user)
    for problem in problems:
        st.warning(problem)
    if not sources:
        return
    pending = general_pending_count(sources)

    info, refresh = st.columns([5, 1])
    info.caption(
        f"{current_user} · Datos consultados {min(source['snapshot']['at'] for source in sources):%d/%m/%Y %H:%M} · "
        "Hora de Ciudad de México"
    )
    if refresh.button("Actualizar datos", disabled=bool(pending), width="stretch",
                      key=f"{GENERAL_KEY_PREFIX}refresh"):
        refresh_general_sources(sources)
        rerun_active_tab()
    show_general_feedback(sources)
    if not can_edit_orders(current_user):
        st.caption(STATUS_ONLY_CAPTION if any_status_access(current_user, sources) else
                   "👁️ Puedes consultar los pedidos y agregar nuevos; los pedidos existentes son de sólo lectura.")

    render_general_new_order(current_user, sources, pending)
    render_general_orders(current_user, sources, pending)
    for source in sources:
        with order_sheet_scope(source["sheet"]):
            render_sent_archive(current_user, source["snapshot"], title_suffix=f" · {source['origin']}")


# ==============================
# Tablero para pantalla
# ==============================
BOARD_TAB_LABEL = "📺 Tablero"
BOARD_ALL_SOURCES = "Todas las hojas"
BOARD_SOURCES = (
    ("Alineadores", SHEET_ORDERS, SHEET_TIMES),
    ("Polanco", SHEET_POLANCO, SHEET_POLANCO_TIMES),
    ("AERIS", SHEET_AERIS, SHEET_AERIS_TIMES),
)
BOARD_REFRESH_SECONDS = 60
BOARD_CARDS_PER_STAGE = 5
# En modo pantalla cada columna recorta con CSS las que no quepan en la altura.
BOARD_SCREEN_CARDS_PER_STAGE = 14
BOARD_PRIORITY_ROWS = 10
BOARD_SCREEN_PARAM = "pantalla"
BOARD_SCREEN_KEY = "aligners_board_screen"
BOARD_SOURCE_KEY = "aligners_board_source"
BOARD_LAST_MODEL_KEY = "aligners_board_last_model"
BOARD_URL_APPLIED_KEY = "aligners_board_url_applied"
# Rangos de antigüedad en días hábiles; ``None`` deja abierto el último rango.
BOARD_AGE_BANDS = ((2, "0–2 d"), (5, "3–5 d"), (10, "6–10 d"), (20, "11–20 d"), (None, "21+ d"))
BOARD_SIGNALS = (
    ("late", "🔴 Atrasado"),
    ("due", "🟡 Por vencer"),
    ("pause", "🟣 En pausa"),
    ("ok", "🟢 En tiempo"),
    ("none", "⚪ Sin medición"),
)
SPANISH_WEEKDAYS = ("lunes", "martes", "miércoles", "jueves", "viernes", "sábado", "domingo")
SPANISH_MONTH_NAMES = (
    "enero", "febrero", "marzo", "abril", "mayo", "junio", "julio",
    "agosto", "septiembre", "octubre", "noviembre", "diciembre",
)


def business_days_between(start: date, end: date) -> int:
    """Días hábiles (lunes a viernes) transcurridos de ``start`` a ``end``."""
    if end <= start:
        return 0
    weeks, remainder = divmod((end - start).days, 7)
    extra = sum(
        1 for offset in range(remainder) if (start + timedelta(days=offset)).weekday() < 5
    )
    return weeks * 5 + extra


def board_age_band(days: Any) -> int | None:
    if days is None or pd.isna(days):
        return None
    return next(
        index
        for index, (upper, _) in enumerate(BOARD_AGE_BANDS)
        if upper is None or int(days) <= upper
    )


def first_log_dates(times_df: pd.DataFrame) -> dict[str, date]:
    """Primer registro de bitácora por pedido; respaldo si falta FECHA DE RECEPCIÓN."""
    logs = canonical_times_df(times_df)
    if not {ID_COLUMN, "FECHA_INICIO"}.issubset(logs.columns):
        return {}
    firsts: dict[str, date] = {}
    for identifier, started in zip(logs[ID_COLUMN], logs["FECHA_INICIO"]):
        key = clean_cell(identifier).strip()
        day = parse_simple_date(started)
        if key and day and (key not in firsts or day < firsts[key]):
            firsts[key] = day
    return firsts


def build_board_table(
    sources: Iterable[tuple[str, pd.DataFrame, pd.DataFrame]],
    definitions: dict[str, ProcessDefinition],
    *,
    now: datetime | None = None,
) -> pd.DataFrame:
    """Une los pedidos activos de cada hoja con su antigüedad, más antiguos primero.

    ``sources`` son tuplas ``(nombre de hoja, pedidos, bitácora)``. El semáforo es
    el mismo de Seguimiento; la antigüedad cuenta días hábiles desde la
    recepción y, sin ella, desde el primer registro de la bitácora.
    """

    current_time = now or app_now()
    frames: list[pd.DataFrame] = []
    for label, orders, times in sources:
        table = build_tracking_table(orders, times, definitions, now=current_time)
        if table.empty:
            continue
        first_logs = first_log_dates(times)
        received = table.get("FECHA DE RECEPCIÓN", pd.Series("", index=table.index))
        starts = [
            parse_simple_date(value) or first_logs.get(clean_cell(identifier).strip())
            for value, identifier in zip(received, table[ID_COLUMN])
        ]
        table = table.copy()
        for column in ("HORAS EN ETAPA", "PLAZO HORAS"):
            table[column] = pd.to_numeric(table[column], errors="coerce")
        table["HOJA"] = label
        table["INICIO PEDIDO"] = starts
        table["DÍAS ACTIVO"] = pd.array(
            [
                business_days_between(start, current_time.date()) if start else None
                for start in starts
            ],
            dtype="Int64",
        )
        frames.append(table)
    if not frames:
        return pd.DataFrame(
            columns=[ID_COLUMN, STATUS_COLUMN, PRODUCT_COLUMN, *COMPUTED_COLUMNS,
                     "HOJA", "INICIO PEDIDO", "DÍAS ACTIVO"]
        )
    board = pd.concat(frames, ignore_index=True, sort=False)
    board = board.assign(
        _age=pd.to_numeric(board["DÍAS ACTIVO"], errors="coerce").astype(float).fillna(-1),
        _signal=board["SEMÁFORO"].map(signal_rank),
        _hours=pd.to_numeric(board["HORAS EN ETAPA"], errors="coerce").astype(float).fillna(-1),
    ).sort_values(["_age", "_signal", "_hours"], ascending=[False, True, False], kind="stable")
    return board.drop(columns=["_age", "_signal", "_hours"]).reset_index(drop=True)


def board_stage_order(
    definitions: dict[str, ProcessDefinition],
) -> tuple[list[str], list[str]]:
    """Etapas normales en el orden común de todos los productos, y las pausas."""
    normal: list[str] = []
    for definition in definitions.values():
        position = -1
        for status in definition.normal_statuses:
            if is_terminal_status(status):
                continue
            keys = [status_key(item) for item in normal]
            if status_key(status) in keys:
                position = keys.index(status_key(status))
                continue
            position += 1
            normal.insert(position, status)
    pauses: dict[str, str] = {}
    for definition in definitions.values():
        for status in definition.pauses:
            pauses.setdefault(status_key(status), status)
    return normal, list(pauses.values())


def board_week_activity(
    sources: Iterable[tuple[str, pd.DataFrame, pd.DataFrame]],
    definitions: dict[str, ProcessDefinition],
    *,
    today: date,
) -> dict[str, int]:
    """Pedidos recibidos (según la hoja) y enviados (según la bitácora) esta semana."""
    week_start = today - timedelta(days=today.weekday())
    received = received_today = 0
    sent: set[tuple[str, str]] = set()
    sent_today: set[tuple[str, str]] = set()
    for label, orders, times in sources:
        frame = orders_with_status(orders, definitions, lambda status: True)
        for value in frame.get("FECHA DE RECEPCIÓN", []):
            day = parse_simple_date(value)
            if day and week_start <= day <= today:
                received += 1
                received_today += day == today
        logs = canonical_times_df(times)
        if not {ID_COLUMN, STATUS_COLUMN, "FECHA_INICIO"}.issubset(logs.columns):
            continue
        for identifier, status, started in zip(
            logs[ID_COLUMN], logs[STATUS_COLUMN], logs["FECHA_INICIO"]
        ):
            day = parse_simple_date(started)
            if not is_sent_status(status) or not day or not week_start <= day <= today:
                continue
            sent.add((label, clean_cell(identifier).strip()))
            if day == today:
                sent_today.add((label, clean_cell(identifier).strip()))
    return {
        "received": received,
        "received_today": received_today,
        "sent": len(sent),
        "sent_today": len(sent_today),
    }


def format_business_hours(hours: Any) -> str:
    value = pd.to_numeric(hours, errors="coerce")
    if value is None or pd.isna(value):
        return ""
    value = float(value)
    if value < 1:
        return "<1 h"
    if value < 24:
        return f"{value:.0f} h"
    days = value / 24
    return f"{days:.0f} d" if days >= 10 or days.is_integer() else f"{days:.1f} d"


def board_card(
    row: pd.Series,
    *,
    rank: int | None,
    show_source: bool,
    signal_keys: dict[str, str],
) -> dict[str, Any]:
    signal = clean_cell(row.get("SEMÁFORO", "")).strip()
    key = signal_keys.get(signal, "none")
    elapsed = pd.to_numeric(row.get("HORAS EN ETAPA"), errors="coerce")
    maximum = pd.to_numeric(row.get("PLAZO HORAS"), errors="coerce")
    status = clean_cell(row.get(STATUS_COLUMN, "")).strip()
    progress = None
    if key == "pause":
        time_label = f"{format_business_hours(elapsed)} en pausa" if pd.notna(elapsed) else "En pausa"
    elif pd.notna(elapsed) and pd.notna(maximum) and float(maximum) > 0:
        time_label = f"{format_business_hours(elapsed)} de {format_business_hours(maximum)}"
        progress = float(elapsed) / float(maximum)
    else:
        time_label = "Sin medición"
    days = row.get("DÍAS ACTIVO")
    days = None if days is None or pd.isna(days) else int(days)
    product = strip_visual_prefix(row.get(PRODUCT_COLUMN, ""))
    return {
        "folio": clean_cell(row.get(ID_COLUMN, "")).strip(),
        "patient": clean_cell(row.get("NOMBRE PACIENTE", "")).strip(),
        "doctor": clean_cell(row.get("NOMBRE DOCTOR", "")).strip(),
        "product": product,
        "product_emoji": PRODUCT_EMOJIS.get(product_key(product), "🦷"),
        "status": status or "Sin etapa",
        "status_emoji": STATUS_EMOJIS.get(status_key(status), "⚪"),
        "source": clean_cell(row.get("HOJA", "")).strip(),
        "show_source": show_source,
        "signal": key,
        "signal_emoji": signal.split(" ", 1)[0] if signal else "⚪",
        "stage_time_label": time_label,
        "progress": progress,
        "detail": clean_cell(row.get("DETALLE SEMÁFORO", "")).strip(),
        "age_days": days,
        "age_band": board_age_band(days),
        "rank": rank,
    }


def board_view_model(
    board: pd.DataFrame,
    definitions: dict[str, ProcessDefinition],
    activity: dict[str, int],
    *,
    now: datetime,
    source_filter: str = BOARD_ALL_SOURCES,
    screen: bool = False,
    cards_per_stage: int = BOARD_CARDS_PER_STAGE,
    priority_rows: int = BOARD_PRIORITY_ROWS,
) -> dict[str, Any]:
    """Traduce la tabla del tablero al modelo que dibuja ``aligners_board``."""

    signal_keys = {label: key for key, label in BOARD_SIGNALS}
    source_labels = [label for label, _, _ in BOARD_SOURCES]
    shown_sources = source_labels if source_filter == BOARD_ALL_SOURCES else [source_filter]
    cards = [
        board_card(
            row,
            rank=index + 1 if index < priority_rows else None,
            show_source=source_filter == BOARD_ALL_SOURCES and row.get("HOJA") in ("Polanco", "AERIS"),
            signal_keys=signal_keys,
        )
        for index, (_, row) in enumerate(board.iterrows())
    ]

    normal, pauses = board_stage_order(definitions)
    by_stage: dict[str, list[dict[str, Any]]] = {}
    known = {status_key(status) for status in [*normal, *pauses]}
    others: list[dict[str, Any]] = []
    for card in cards:
        stage = status_key(card["status"])
        if stage in known:
            by_stage.setdefault(stage, []).append(card)
        else:
            others.append({**card, "stage_time_label": card["status"]})

    def stage_model(name: str, stage_cards: list[dict[str, Any]], emoji: str | None = None) -> dict[str, Any]:
        counts = {key: 0 for key, _ in BOARD_SIGNALS}
        for card in stage_cards:
            counts[card["signal"]] += 1
        return {
            "name": name,
            "emoji": emoji or STATUS_EMOJIS.get(status_key(name), "⚪"),
            "accent": status_palette_value(name)[0],
            "count": len(stage_cards),
            "signals": counts,
            "cards": stage_cards[:cards_per_stage],
            "more": max(len(stage_cards) - cards_per_stage, 0),
        }

    groups = [
        {
            "label": "Flujo normal →",
            "kind": "normal",
            "stages": [stage_model(name, by_stage.get(status_key(name), [])) for name in normal],
        },
        {
            "label": "⏸️ Pausas",
            "kind": "pause",
            "stages": [stage_model(name, by_stage.get(status_key(name), [])) for name in pauses],
        },
    ]
    if others:
        groups.append(
            {
                "label": "Otras etapas",
                "kind": "other",
                "stages": [stage_model("Revisar etapa", others, "❔")],
            }
        )

    signals = []
    for key, label in BOARD_SIGNALS:
        emoji, text = label.split(" ", 1)
        signals.append(
            {"key": key, "emoji": emoji, "label": text, "count": sum(card["signal"] == key for card in cards)}
        )
    dated = [card for card in cards if card["age_days"] is not None]
    oldest = max(dated, key=lambda card: card["age_days"], default=None)
    ages = [
        {"label": label, "band": index, "count": sum(card["age_band"] == index for card in cards)}
        for index, (_, label) in enumerate(BOARD_AGE_BANDS)
    ]
    undated = len(cards) - len(dated)
    if undated:
        ages.append({"label": "Sin fecha", "band": None, "count": undated})

    return {
        "screen": screen,
        "title": "Tablero de alineadores",
        "subtitle": " + ".join(shown_sources) + " · pedidos activos por etapa · días hábiles (lun–vie)",
        "clock": f"{now:%H:%M}",
        "date_label": f"{SPANISH_WEEKDAYS[now.weekday()]} {now.day} de {SPANISH_MONTH_NAMES[now.month - 1]}",
        "updated": f"{now:%H:%M:%S}",
        "stale": "",
        "total": len(cards),
        "sources": [
            (label, sum(card["source"] == label for card in cards)) for label in shown_sources
        ],
        "signals": signals,
        "oldest": (
            {"days": oldest["age_days"], "folio": oldest["folio"], "patient": oldest["patient"]}
            if oldest
            else None
        ),
        "week": activity,
        "groups": groups,
        "priority": [card for card in cards if card["rank"]],
        "ages": ages,
        "footer": (
            f"Se actualiza solo cada {BOARD_REFRESH_SECONDS} s · "
            "Enviados según la bitácora de la app."
        ),
    }


def board_source_options() -> list[str]:
    return [BOARD_ALL_SOURCES, *(label for label, _, _ in BOARD_SOURCES)]


def read_board_sources(source_filter: str) -> list[tuple[str, pd.DataFrame, pd.DataFrame]]:
    """Lee cada hoja por nombre; no toca la hoja activa de Seguimiento."""
    sources = []
    for label, orders_sheet, times_sheet in BOARD_SOURCES:
        if source_filter not in (BOARD_ALL_SOURCES, label):
            continue
        try:
            sources.append((label, read_orders_df(orders_sheet), read_times_df(times_sheet)))
        except gspread.WorksheetNotFound:
            continue
    return sources


def load_board_model(source_filter: str, *, screen: bool) -> dict[str, Any]:
    now = app_now()
    definitions = read_process_definitions()
    sources = read_board_sources(source_filter)
    return board_view_model(
        build_board_table(sources, definitions, now=now),
        definitions,
        board_week_activity(sources, definitions, today=now.date()),
        now=now,
        source_filter=source_filter,
        screen=screen,
        cards_per_stage=BOARD_SCREEN_CARDS_PER_STAGE if screen else BOARD_CARDS_PER_STAGE,
    )


def clear_board_cache() -> None:
    """Fuerza una lectura nueva de las hojas del tablero sin tocar la mesa de edición."""
    for _, orders_sheet, times_sheet in BOARD_SOURCES:
        read_order_values.clear(orders_sheet)
        read_times_values.clear(times_sheet)
    read_process_values.clear()


def board_screen_requested() -> bool:
    return normalize_text(get_query_param_value(BOARD_SCREEN_PARAM)) in {"1", "SI", "TRUE"}


def apply_board_url(current_user: str) -> None:
    """Con ``?pantalla=1`` la primera carga abre el tablero en modo pantalla."""
    if st.session_state.get(BOARD_URL_APPLIED_KEY):
        return
    st.session_state[BOARD_URL_APPLIED_KEY] = True
    if board_screen_requested():
        st.session_state[f"aligners_primary_tabs_{current_user}"] = BOARD_TAB_LABEL
        st.session_state[BOARD_SCREEN_KEY] = True


def sync_board_screen_param() -> None:
    """Deja el modo pantalla en el enlace para reabrirlo igual tras recargar."""
    if st.session_state.get(BOARD_SCREEN_KEY):
        st.query_params[BOARD_SCREEN_PARAM] = "1"
    elif BOARD_SCREEN_PARAM in st.query_params:
        del st.query_params[BOARD_SCREEN_PARAM]


def apply_board_css(current_user: str, *, screen: bool) -> None:
    # La lectura automática no debe atenuar el tablero mientras se recalcula.
    css = '[data-stale="true"] {opacity:1 !important; transition:none !important;}'
    if screen:
        # El tablero se fija sobre toda la ventana; sólo el interruptor queda encima.
        css += f"""
        [data-testid="stHeader"], [data-testid="stSidebar"], [data-testid="stExpandSidebarButton"],
        [data-testid="stStatusWidget"], [data-testid="stToolbar"], [data-testid="stDecoration"],
        .st-key-lab_workspace_header, .align-hero,
        .st-key-aligners_primary_tabs_{current_user} [role="tablist"] {{display:none !important;}}
        .stApp {{background:#0D1813 !important;}}
        .st-key-aligners_board_screen_bar {{
            position:fixed; right:14px; bottom:8px; z-index:999999; width:auto !important;
            background:#F1FCF5; border-radius:10px; padding:2px 12px; opacity:.4;
            transition:opacity 150ms ease;
        }}
        .st-key-aligners_board_screen_bar:hover,
        .st-key-aligners_board_screen_bar:focus-within {{opacity:1;}}
        """
    st.html(f"<style>{css}</style>")


def render_board_toolbar() -> None:
    st.session_state.setdefault(BOARD_SOURCE_KEY, BOARD_ALL_SOURCES)
    with st.container(key="aligners_board_toolbar"):
        source, info, screen, refresh = st.columns(
            [2.3, 2.6, 1.3, 1], vertical_alignment="center"
        )
        with source:
            st.segmented_control(
                "Hoja",
                board_source_options(),
                key=BOARD_SOURCE_KEY,
                required=True,
                label_visibility="collapsed",
            )
        info.caption(
            f"🔄 Se actualiza solo cada {BOARD_REFRESH_SECONDS} s · sólo lectura · "
            "para la pantalla usa Modo pantalla y F11."
        )
        screen.toggle(
            "📺 Modo pantalla", key=BOARD_SCREEN_KEY, on_change=sync_board_screen_param
        )
        refresh.button(
            "Actualizar", key="aligners_board_refresh", width="stretch", on_click=clear_board_cache
        )


@st.fragment(run_every=BOARD_REFRESH_SECONDS)
def render_board_live(source_filter: str, screen: bool) -> None:
    """Se vuelve a dibujar solo; si Sheets falla conserva la última lectura."""
    cache = st.session_state.setdefault(BOARD_LAST_MODEL_KEY, {})
    try:
        model = load_board_model(source_filter, screen=screen)
        cache[source_filter] = model
    except Exception as exc:
        previous = cache.get(source_filter)
        if previous is None and not is_google_sheets_rate_limit_error(exc):
            raise
        reason = (
            "Google Sheets alcanzó el límite temporal de lecturas"
            if is_google_sheets_rate_limit_error(exc)
            else f"No se pudieron leer las hojas ({type(exc).__name__})"
        )
        if previous is None:
            st.warning(f"{reason}. El tablero se reintentará en {BOARD_REFRESH_SECONDS} s.")
            return
        model = {
            **previous,
            "screen": screen,
            "stale": f"{reason}; se muestra la lectura de las {previous['updated']}. "
            f"Se reintentará en {BOARD_REFRESH_SECONDS} s.",
        }
    st.html(render_board_html(model))


def render_board(current_user: str) -> None:
    """Tablero de sólo lectura de los seguimientos independientes."""
    screen = bool(st.session_state.get(BOARD_SCREEN_KEY, False))
    apply_board_css(current_user, screen=screen)
    if screen:
        # Conserva la hoja elegida aunque su selector no se dibuje en pantalla.
        st.session_state[BOARD_SOURCE_KEY] = st.session_state.get(BOARD_SOURCE_KEY, BOARD_ALL_SOURCES)
        with st.container(key="aligners_board_screen_bar"):
            st.toggle(
                "📺 Modo pantalla", key=BOARD_SCREEN_KEY, on_change=sync_board_screen_param
            )
    else:
        render_board_toolbar()
    source_filter = st.session_state.get(BOARD_SOURCE_KEY, BOARD_ALL_SOURCES)
    if source_filter not in board_source_options():
        source_filter = BOARD_ALL_SOURCES
    render_board_live(source_filter, screen)


# "🚨 Alertas y pausas" (render_alerts) y "⚙️ Procesos y plazos"
# (render_processes) están ocultas; para mostrarlas otra vez basta con
# agregarlas aquí.
ALIGNERS_TAB_LABELS = (
    GENERAL_TAB_LABEL,
    "📋 Seguimiento",
    POLANCO_TAB_LABEL,
    AERIS_TAB_LABEL,
    case_search.TAB_LABEL,
    FORMS_TAB_LABEL,
    BOARD_TAB_LABEL,
)


def aligners_tab_labels() -> tuple[str, ...]:
    """Con las pestañas por hoja ocultas sólo queda la vista general de seguimiento."""
    if not individual_tracking_hidden():
        return ALIGNERS_TAB_LABELS
    return tuple(label for label in ALIGNERS_TAB_LABELS if label not in TRACKING_TAB_SHEETS)


@st.fragment(key="aligners_active_tab_content")
def render_app_tabs(current_user: str) -> None:
    """Ejecuta únicamente el contenido de la pestaña visible."""
    apply_board_url(current_user)
    labels = aligners_tab_labels()
    # Nombres anteriores de pestañas que una sesión abierta pudo haber guardado.
    previous_labels = {"📋 Seguimiento Polanco": POLANCO_TAB_LABEL,
                       "🏢 Seguimiento Polanco": POLANCO_TAB_LABEL, "AERIS": AERIS_TAB_LABEL}
    for key in ("aligners_active_tab", f"aligners_primary_tabs_{current_user}"):
        current = st.session_state.get(key)
        previous = previous_labels.get(current, current)
        if previous in TRACKING_TAB_SHEETS and previous not in labels:
            previous = GENERAL_TAB_LABEL
        if previous != current:
            st.session_state[key] = previous
    tabs = st.tabs(
        labels,
        key=f"aligners_primary_tabs_{current_user}",
        on_change=guard_tracking_tab_change,
        args=(current_user,),
    )
    for label, tab in zip(labels, tabs):
        if not tab.open:
            continue
        with tab:
            st.session_state["aligners_active_tab"] = label
            st.session_state["aligners_order_sheet"] = (
                TRACKING_TAB_SHEETS.get(label, SHEET_ORDERS)
            )
            if label in TRACKING_TAB_SHEETS:
                if st.session_state.pop("aligners_tab_warning", False):
                    st.warning("Guarda o descarta los cambios antes de cambiar de pestaña.")
                try:
                    ensure_times_headers()
                    render_workbench(current_user)
                except gspread.WorksheetNotFound:
                    st.error(f"No se encontró la hoja {current_order_sheet()} en Control ALINEADORES. "
                             "Revisa su nombre y recarga la página después de agregarla.")
            elif label == GENERAL_TAB_LABEL:
                if st.session_state.pop("aligners_tab_warning", False):
                    st.warning("Guarda o descarta los cambios antes de cambiar de pestaña.")
                render_general_workbench(current_user)
            elif label == "🚨 Alertas y pausas":
                render_alerts(current_user)
            elif label == "⚙️ Procesos y plazos":
                render_processes(current_user)
            elif label == BOARD_TAB_LABEL:
                render_board(current_user)
            elif label == case_search.TAB_LABEL:
                render_case_search(current_user)
            else:
                render_aligners_forms_tab()
        break


def apply_custom_css() -> None:
    st.markdown(
        """<style>
        .stApp {
            background: radial-gradient(ellipse at 0 0, #C9F3DA 0, transparent 48%),
                        radial-gradient(ellipse at 100% 30%, #CDEDEA 0, transparent 45%), #EFFBF3;
        }
        .block-container {padding-top: 3rem; padding-bottom: 2rem; max-width: 100%;}
        [data-testid="stHeader"] {background: transparent;}
        [data-testid="stSidebar"] {background: #E4F5EA; border-right: 1px solid #BFE3CC;}
        h1, h2, h3 {letter-spacing: -.025em; color: #1B4B36;}
        .align-hero {
            position: relative; overflow: hidden; padding: 24px 30px; margin: 0 0 20px;
            display: flex; justify-content: space-between; align-items: center; gap: 20px;
            color: #FFF; background: linear-gradient(115deg, #123B2C 0%, #12915F 54%, #7ACB5D 100%);
            border: 1px solid #7BCDA6; border-radius: 20px; box-shadow: 0 12px 30px #0F3F2A22;
        }
        .align-hero h1 {color: #FFF; font-size: 2rem; margin: 6px 0; padding: 0;}
        .align-hero p {color: #E3FBEA; margin: 0; font-size: .93rem;}
        .align-brand {font-size: .72rem; font-weight: 800; letter-spacing: .17em; color: #C7F0D6;}
        .align-hero-badge {
            background: #FFFFFF18; border: 1px solid #FFFFFF42; border-radius: 14px;
            padding: 12px 18px; font-size: .84rem; color: #FFF; white-space: nowrap;
        }
        [data-baseweb="tab-list"] {
            gap: 8px; padding: 7px; border-radius: 15px; background: #DCF2E4;
            border: 1px solid #B7DFC4; margin-bottom: 12px;
        }
        [data-baseweb="tab"] {
            height: 45px; padding: 0 20px; border-radius: 10px; background: #F1FCF5;
            border: 1px solid #C7E8D2; color: #265C42; font-weight: 700;
        }
        [data-baseweb="tab"][aria-selected="true"] {
            background: linear-gradient(110deg, #159A66, #0C6E4A); color: #FFF;
            border-color: #0E8358; box-shadow: 0 4px 12px #12965F33;
        }
        [data-baseweb="tab"][aria-selected="true"] p {color: #FFF;}
        [data-baseweb="tab-highlight"], [data-baseweb="tab-border"] {display: none;}
        [data-testid="stTab"][aria-selected="true"],
        [data-testid="stTab"][aria-selected="true"] p {color: #0E8358 !important; font-weight: 800;}
        .react-aria-SelectionIndicator {background: #0E8358 !important;}
        [data-testid="stMetric"] {
            border: 1px solid #B9E4CB; border-top-width: 4px; border-radius: 14px;
            padding: 13px 16px; box-shadow: 0 5px 16px #42266C0D;
        }
        .st-key-align_total [data-testid="stMetric"], .st-key-align_total_alerts [data-testid="stMetric"] {background:#E7FBF0;border-color:#3FAE7B;color:#0F6B44;}
        .st-key-align_red [data-testid="stMetric"], .st-key-align_red_alerts [data-testid="stMetric"] {background:#FFECEF;border-color:#DF5875;color:#A72B48;}
        .st-key-align_amber [data-testid="stMetric"], .st-key-align_amber_alerts [data-testid="stMetric"] {background:#FFF3D7;border-color:#DBA131;color:#885A08;}
        .st-key-align_pause [data-testid="stMetric"], .st-key-align_pause_alerts [data-testid="stMetric"] {background:#DCF5F1;border-color:#159C8B;color:#0D5C52;}
        .st-key-align_green [data-testid="stMetric"], .st-key-align_green_alerts [data-testid="stMetric"] {background:#E0F7EE;border-color:#37A989;color:#13654E;}
        .st-key-align_gray [data-testid="stMetric"], .st-key-align_gray_alerts [data-testid="stMetric"] {background:#EBEFF8;border-color:#8193B4;color:#4C5E7D;}
        .st-key-align_filter_total button,
        .st-key-align_filter_red button,
        .st-key-align_filter_amber button,
        .st-key-align_filter_pause button,
        .st-key-align_filter_green button,
        .st-key-align_filter_gray button {
            position:relative;min-height:96px;justify-content:flex-start;padding:14px 15px;
            border-width:2px;border-radius:14px;text-align:left;
            transition:transform 150ms ease,box-shadow 150ms ease;
        }
        .st-key-align_filter_total button {background:#E7FBF0 !important;border-color:#3FAE7B !important;color:#0F6B44 !important;}
        .st-key-align_filter_red button {background:#FFECEF !important;border-color:#DF5875 !important;color:#A72B48 !important;}
        .st-key-align_filter_amber button {background:#FFF3D7 !important;border-color:#DBA131 !important;color:#885A08 !important;}
        .st-key-align_filter_pause button {background:#DCF5F1 !important;border-color:#159C8B !important;color:#0D5C52 !important;}
        .st-key-align_filter_green button {background:#E0F7EE !important;border-color:#37A989 !important;color:#13654E !important;}
        .st-key-align_filter_gray button {background:#EBEFF8 !important;border-color:#8193B4 !important;color:#4C5E7D !important;}
        .st-key-align_filter_total button p,
        .st-key-align_filter_red button p,
        .st-key-align_filter_amber button p,
        .st-key-align_filter_pause button p,
        .st-key-align_filter_green button p,
        .st-key-align_filter_gray button p {
            color:inherit !important;white-space:pre-line;line-height:1.15;text-align:left;
        }
        .st-key-align_filter_total button strong,
        .st-key-align_filter_red button strong,
        .st-key-align_filter_amber button strong,
        .st-key-align_filter_pause button strong,
        .st-key-align_filter_green button strong,
        .st-key-align_filter_gray button strong {display:block;margin-top:7px;font-size:1.7rem;line-height:1;font-weight:850;}
        .st-key-align_filter_total button[kind="primary"],
        .st-key-align_filter_red button[kind="primary"],
        .st-key-align_filter_amber button[kind="primary"],
        .st-key-align_filter_pause button[kind="primary"],
        .st-key-align_filter_green button[kind="primary"],
        .st-key-align_filter_gray button[kind="primary"] {
            transform:translateY(-2px);border-width:3px !important;
            box-shadow:0 0 0 3px #1F8F5C33,0 10px 22px #0E402B2B !important;
        }
        .st-key-align_filter_total button[kind="primary"]::after,
        .st-key-align_filter_red button[kind="primary"]::after,
        .st-key-align_filter_amber button[kind="primary"]::after,
        .st-key-align_filter_pause button[kind="primary"]::after,
        .st-key-align_filter_green button[kind="primary"]::after,
        .st-key-align_filter_gray button[kind="primary"]::after {
            content:"✓";position:absolute;right:11px;top:9px;width:22px;height:22px;
            display:grid;place-items:center;border-radius:50%;background:#0E6B47;color:#FFF;
            font-size:.76rem;font-weight:900;
        }
        .align-edit-bar {
            height: 40px; display:flex; align-items:center; padding:0 14px; border-radius:10px;
            border:1px solid #BFE3CC; font-size:.85rem; font-weight:600; margin: 8px 0;
        }
        .align-edit-bar.is-ready {background:#DFF5E7;color:#1D6B47;}
        .align-edit-bar.is-pending {background:#FCE9BA;border-color:#DCA544;color:#805410;}
        .align-column-legend {display:flex;justify-content:flex-end;gap:8px;align-items:end;min-height:38px;}
        .align-column-legend span {border-radius:999px;padding:5px 10px;color:#FFF;font-size:.76rem;font-weight:700;}
        .align-column-legend .editable {background:#12875E;}
        .align-column-legend .readonly {background:#52667D;}
        .align-column-legend .automatic {background:#147C84;}
        .align-column-legend .auto-editable {background:linear-gradient(100deg,#147C84 0 48%,#12875E 52% 100%);}
        [data-testid="stWidgetLabel"] p {color:#265C42;font-weight:650;}
        button[kind="primary"], [data-testid="stBaseButton-primary"] {
            background:linear-gradient(110deg,#17A06B,#0C6E4A);border-color:#0E8358;color:#FFF;
            box-shadow:0 4px 12px #17A06B2B;
        }
        button[kind="primary"]:not(:disabled):hover,
        [data-testid="stBaseButton-primary"]:not(:disabled):hover {
            box-shadow:0 5px 16px #17A06B4D;
        }
        button[kind="primary"]:disabled,
        [data-testid="stBaseButton-primary"]:disabled {
            background:#D6F0DF;border-color:#9DCBAE;color:#2C5B41;opacity:1;
            box-shadow:none;cursor:not-allowed;
        }
        button[kind="primary"]:disabled *,
        [data-testid="stBaseButton-primary"]:disabled * {
            color:#2C5B41 !important;-webkit-text-fill-color:#2C5B41;opacity:1;
        }
        .align-forms-hero {background:linear-gradient(135deg,#EAFBF1 0%,#EFFBF3 55%,#F1FAF7 100%);border:1px solid #BFE3CC;border-radius:20px;padding:18px 20px;margin:12px 0 18px;box-shadow:0 12px 30px rgba(15,63,42,.08);}
        .align-forms-hero-title {font-size:1.05rem;font-weight:800;color:#123B2C;margin-bottom:4px;}
        .align-forms-pill {display:inline-block;border-radius:999px;padding:7px 12px;margin:4px 7px 8px 0;font-weight:800;}
        .align-forms-pill-id {background:#DCF2E4;color:#0F6B44;}
        .align-forms-pill-form {background:#EEF0FF;color:#3B3F8F;}
        .align-forms-pill-link {background:#E0F7EE;color:#13654E;}
        .align-forms-pill-missing {background:#FFECEF;color:#A72B48;}
        .align-forms-detail {background:linear-gradient(180deg,#FFFFFF 0%,#F7FCF9 100%);border:1px solid #D7EDDD;border-left:5px solid #12875E;border-radius:14px;padding:12px 14px;margin:7px 0;box-shadow:0 4px 12px rgba(15,63,42,.05);}
        .align-forms-label {color:#3F6B54;font-size:.78rem;font-weight:850;text-transform:uppercase;letter-spacing:.03em;}
        .align-forms-value {color:#1B4B36;font-size:1rem;margin-top:5px;word-break:break-word;line-height:1.35;}
        .align-forms-links-list {margin-top:.45rem;padding-left:1.75rem;}
        .align-forms-links-list li {padding:.45rem .2rem;border-bottom:1px solid #D7EDDD;}
        .align-forms-links-list a {font-weight:750;color:#0E8358;}
        .align-forms-link-url {margin-top:.15rem;color:#5C7568;font-size:.84rem;overflow-wrap:anywhere;}
        .st-key-align_forms_pdf_td [data-testid="stBaseButton-secondary"],
        .st-key-align_forms_pdf_marca_blanca [data-testid="stBaseButton-secondary"],
        .st-key-align_forms_pdf_otros_productos [data-testid="stBaseButton-secondary"] {
            border-color:#0E8358;color:#0E8358;
        }
        @media (max-width: 800px) {
            .align-hero {padding:20px;}.align-hero h1 {font-size:1.55rem;}.align-hero-badge {display:none;}
        }
        </style>""",
        unsafe_allow_html=True,
    )


def render_embedded_workspace(current_user: str) -> None:
    """Renderiza alineadores dentro de lab_pg usando la sesión compartida."""

    render_app_tabs(current_user)


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
        render_embedded_workspace(current_user)
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
