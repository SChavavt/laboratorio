"""Fecha persistente de edición y orden común de las mesas de laboratorio."""

from datetime import datetime, timezone
from collections import Counter
from functools import wraps
from threading import RLock
from zoneinfo import ZoneInfo

import pandas as pd
from gspread.cell import Cell
from order_identifiers import identifier_resolver


LAST_EDIT_COLUMN = "_ULTIMA_EDICION_APP"
HISTORY_ACTIVITY_COLUMN = "_ULTIMA_ACTIVIDAD_HISTORICA"
ACTIVITY_ORDER_VERSION = 2
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


def activity_timestamp(value):
    """Lee fechas ISO y de Sheets; las fechas sin zona son de Ciudad de México."""
    text = str(value).strip()
    if text.lower() in {"", "none", "nan", "nat", "<na>"}:
        return pd.NaT
    try:
        parsed = datetime.fromisoformat(text)
    except (TypeError, ValueError):
        parsed = None
        for pattern in ("%Y/%m/%d %H:%M:%S", "%Y/%m/%d %H:%M", "%Y/%m/%d",
                        "%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M", "%d/%m/%Y",
                        "%d-%m-%Y %H:%M:%S", "%d-%m-%Y %H:%M", "%d-%m-%Y"):
            try:
                parsed = datetime.strptime(text, pattern)
                break
            except ValueError:
                continue
    if parsed is None:
        return pd.NaT
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=APP_TIMEZONE)
    return pd.Timestamp(parsed.astimezone(timezone.utc))


def with_history_activity(
    frame: pd.DataFrame, history: pd.DataFrame, id_column: str, *, order_ids=None, resolve_legacy: bool = True,
) -> pd.DataFrame:
    """Añade sólo a la copia de lectura la última actividad real de la bitácora.

    Se usan registros, inicios/cierres, pagos e impresión; nunca fechas límite
    o de entrega futura. Los folios se resuelven dentro de su hoja de origen.
    """
    result = frame.copy()
    if id_column not in result:
        return result
    identifiers = list(result[id_column] if order_ids is None else order_ids)
    identifiers = [str(value).strip() for value in identifiers]
    counts = Counter(identifiers)
    resolve = identifier_resolver(identifiers) if resolve_legacy else lambda value: str(value).strip()
    latest = {}
    if id_column in history:
        events = (("FECHA_REGISTRO_LOG", None), ("FECHA_INICIO", "HORA_INICIO"),
                  ("FECHA_FIN", "HORA_FIN"), ("PAGO_FECHA", None),
                  ("FECHA_IMPRESION", "HORA_IMPRESION"))
        for row in history.to_dict("records"):
            identifier = resolve(row.get(id_column, ""))
            if not identifier or counts[identifier] != 1:
                continue
            for date_column, time_column in events:
                day = str(row.get(date_column, "")).strip()
                if day.lower() in {"", "none", "nan", "nat", "<na>"}:
                    continue
                clock = str(row.get(time_column, "")).strip() if time_column else ""
                if clock.lower() in {"none", "nan", "nat", "<na>"}:
                    clock = ""
                stamp = activity_timestamp(f"{day} {clock}".strip())
                if pd.notna(stamp) and (identifier not in latest or stamp > latest[identifier]):
                    latest[identifier] = stamp
    result[HISTORY_ACTIVITY_COLUMN] = [latest.get(str(value).strip(), pd.NaT) for value in result[id_column]]
    return result


def initialize_recent_order(state, key: str, *, pending: bool) -> bool:
    """Activa una vez el orden nuevo en sesiones abiertas, cuando no hay borradores."""
    marker = f"{key}_activity_version"
    if pending or state.get(marker) == ACTIVITY_ORDER_VERSION:
        return False
    state[key] = RECENT_FIRST
    state[marker] = ACTIVITY_ORDER_VERSION
    return True


def recent_first(frame: pd.DataFrame) -> pd.DataFrame:
    """Última edición/actividad arriba; sin historial, recepción reciente primero."""
    if frame.empty:
        return frame.copy()
    dates = pd.DataFrame(index=range(len(frame)))
    for column in (LAST_EDIT_COLUMN, HISTORY_ACTIVITY_COLUMN):
        values = frame[column] if column in frame else pd.Series("", index=frame.index)
        dates[column] = pd.to_datetime(values.reset_index(drop=True).map(activity_timestamp), utc=True)
    dates["activity"] = dates[[LAST_EDIT_COLUMN, HISTORY_ACTIVITY_COLUMN]].max(axis=1)
    received = frame.get("FECHA DE RECEPCIÓN", pd.Series("", index=frame.index))
    dates["received"] = pd.to_datetime(received.reset_index(drop=True).map(activity_timestamp), utc=True)
    order = dates.sort_values(["activity", "received"], ascending=False,
                              kind="stable", na_position="last").index
    return frame.iloc[order].reset_index(drop=True)
