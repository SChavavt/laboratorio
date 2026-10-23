"""Fecha persistente de edición y orden común de las mesas de laboratorio."""

from datetime import datetime, timezone
from functools import wraps
from threading import RLock
from zoneinfo import ZoneInfo

import pandas as pd
from gspread.cell import Cell


LAST_EDIT_COLUMN = "_ULTIMA_EDICION_APP"
RECENT_FIRST = "Última edición primero"
SHEET_ORDER = "Orden de la hoja"
URGENT_FIRST = "Atender urgentes primero"
ORDER_MODES = [RECENT_FIRST, SHEET_ORDER, URGENT_FIRST]
APP_TIMEZONE = ZoneInfo("America/Mexico_City")
_write_lock = RLock()


def serialized_order_write(function):
    """Serializa lectura y escritura entre sesiones al incorporar el encabezado."""
    @wraps(function)
    def save(*args, **kwargs):
        with _write_lock:
            return function(*args, **kwargs)
    return save


def edit_timestamp(now: datetime) -> str:
    """Texto ISO con zona horaria y microsegundos; Sheets no lo convierte en fecha local."""
    if now.tzinfo is None:
        now = now.replace(tzinfo=APP_TIMEZONE)
    return now.astimezone(timezone.utc).isoformat(timespec="microseconds")


def prepare_activity_column(worksheet, values: list[list[str]]) -> tuple[list[str], int, list[Cell]]:
    """Prepara un encabezado nuevo al final, sin pisar fórmulas ni columnas sin título.

    El llamador guarda los Cell del encabezado junto con la edición. Sólo se
    amplía la cuadrícula si la nueva columna queda fuera de su tamaño actual.
    """
    headers = list(values[1])
    position = next((index for index, header in enumerate(headers, start=1)
                     if str(header).strip().upper() == LAST_EDIT_COLUMN), None)
    if position is not None:
        return headers, position, []
    column_count = getattr(worksheet, "col_count", None)
    # Añadir después de toda la cuadrícula también respeta fórmulas sin
    # encabezado cuyo resultado vacío no aparece en get_all_values().
    position = max(max(map(len, values), default=len(headers)), column_count or 0) + 1
    if column_count is not None and position > column_count:
        worksheet.resize(cols=position)
    headers.extend([""] * (position - len(headers) - 1))
    headers.append(LAST_EDIT_COLUMN)
    return headers, position, [Cell(2, position, LAST_EDIT_COLUMN)]


def recent_first(frame: pd.DataFrame) -> pd.DataFrame:
    """Últimos guardados arriba; sin fecha válida se conserva el orden de origen."""
    if LAST_EDIT_COLUMN not in frame or frame.empty:
        return frame.copy()
    timestamps = pd.to_datetime(frame[LAST_EDIT_COLUMN].reset_index(drop=True),
                                format="ISO8601", utc=True, errors="coerce")
    order = timestamps.sort_values(ascending=False, kind="stable", na_position="last").index
    return frame.iloc[order].reset_index(drop=True)
