"""Avisos de respuestas nuevas de Google Forms, por usuario.

La primera parte es lógica pura (sin red) y la segunda la campana de la app.
lab_pg le pasa los formularios que puede abrir cada usuario y cómo leer y
guardar su estado, así este módulo no importa ninguna app.

Cada usuario guarda, por formulario, un estado ``{"base": ..., "vistas": [...]}``:
todas las respuestas con marca temporal hasta ``base`` ya se vieron, y
``vistas`` son las posteriores que el usuario ya abrió una por una. Se usa la
marca temporal y no el número de fila porque si alguien borra una fila del Sheet
los números se recorren y una respuesta nueva podría heredar uno ya visto.
"""

from __future__ import annotations

import html
import json
import re
import unicodedata
from datetime import datetime
from typing import Any, Callable, Iterable

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

# Google Forms en es-MX escribe "2/10/2026 12:52:50" (día/mes/año).
_DAY_FIRST_PATTERN = re.compile(
    r"(\d{1,2})/(\d{1,2})/(\d{4})\s+(\d{1,2}):(\d{2})(?::(\d{2}))?"
)


def response_key(timestamp: Any) -> str:
    """Convierte la marca temporal del Sheet en una clave ISO ordenable.

    Devuelve "" si no se reconoce el formato; esas respuestas nunca avisan.
    """

    text = str(timestamp or "").strip()
    match = _DAY_FIRST_PATTERN.fullmatch(text)
    try:
        if match:
            day, month, year, hour, minute, second = match.groups()
            parsed = datetime(
                int(year), int(month), int(day), int(hour), int(minute), int(second or 0)
            )
        else:
            parsed = datetime.fromisoformat(text)
    except ValueError:
        return ""
    return parsed.replace(tzinfo=None).isoformat(timespec="seconds")


def baseline_state(keys: Iterable[str]) -> dict[str, Any]:
    """Estado inicial: lo que ya existe cuenta como visto para no avisar el histórico."""

    known = [key for key in keys if key]
    return {"base": max(known, default=""), "vistas": []}


def clean_state(state: Any) -> dict[str, Any] | None:
    """Valida un estado leído del Sheet; None significa que aún no hay línea base."""

    if not isinstance(state, dict) or "base" not in state:
        return None
    vistas = state.get("vistas", [])
    return {
        "base": str(state.get("base") or ""),
        "vistas": sorted({str(key) for key in vistas if key}) if isinstance(vistas, list) else [],
    }


def pending_keys(keys: Iterable[str], state: dict[str, Any] | None) -> list[str]:
    """Claves posteriores a ``base`` que el usuario todavía no abre, de la más vieja a la más nueva."""

    if state is None:
        return []
    seen = set(state["vistas"])
    return sorted({key for key in keys if key and key > state["base"] and key not in seen})


def mark_seen(
    state: dict[str, Any] | None, seen: Iterable[str], keys: Iterable[str]
) -> dict[str, Any]:
    """Marca respuestas como vistas y compacta el estado.

    ``keys`` son todas las respuestas actuales: ``base`` avanza mientras la
    siguiente respuesta ya esté vista, así ``vistas`` no crece sin límite.
    """

    known = sorted({key for key in keys if key})
    current = state if state is not None else baseline_state(known)
    base = current["base"]
    vistas = set(current["vistas"]) | {key for key in seen if key}
    for key in known:
        if key <= base:
            continue
        if key not in vistas:
            break
        base = key
    return {"base": base, "vistas": sorted(key for key in vistas if key > base)}


def load_states(raw: Any) -> dict[str, dict[str, Any]]:
    """Lee el JSON guardado por usuario; un valor dañado equivale a no tener estado."""

    try:
        data = json.loads(raw) if isinstance(raw, str) and raw.strip() else {}
    except ValueError:
        return {}
    if not isinstance(data, dict):
        return {}
    states = {}
    for form_key, state in data.items():
        cleaned = clean_state(state)
        if cleaned is not None:
            states[str(form_key)] = cleaned
    return states


def dump_states(states: dict[str, dict[str, Any]]) -> str:
    return json.dumps(states, ensure_ascii=False, sort_keys=True)


def merge_states(
    stored: dict[str, dict[str, Any]], local: dict[str, dict[str, Any]]
) -> dict[str, dict[str, Any]]:
    """Une el estado guardado con el de esta sesión sin volver a marcar nada como nuevo.

    Así dos dispositivos del mismo usuario no se pisan: lo visto en uno se queda visto.
    """

    merged = {}
    for form_key in sorted({*stored, *local}):
        first, second = stored.get(form_key), local.get(form_key)
        if first is None or second is None:
            merged[form_key] = dict(first or second)
            continue
        base = max(first["base"], second["base"])
        vistas = {*first["vistas"], *second["vistas"]}
        merged[form_key] = {"base": base, "vistas": sorted(key for key in vistas if key > base)}
    return merged


# ==============================
# Campana de avisos en la app
# ==============================
REFRESH_SECONDS = 60
MAX_LISTED = 8
TIMESTAMP_HEADERS = {"MARCA TEMPORAL", "TIMESTAMP"}
PATIENT_HINTS = ("NOMBRE COMPLETO DEL PACIENTE", "NOMBRE DEL PACIENTE")
DOCTOR_HINTS = ("MEDICO TRATANTE", "DOCTOR TRATANTE", "NOMBRE DEL DOCTOR")
# Claves de sesión compartidas con las pestañas de Forms.
STATES_KEY = "forms_notices_states"
PENDING_KEY = "forms_notices_pending"
VIEWED_KEY = "forms_notices_viewed"
FOCUS_KEY = "forms_notices_focus"
GOTO_KEY = "forms_notices_goto"
ANNOUNCED_KEY = "forms_notices_announced"
FRAMES_KEY = "forms_notices_frames"
MARK_ALL_KEY = "forms_notices_mark_all_requested"
OWNER_KEY = "forms_notices_owner"
SELECTIONS_KEY = "forms_notices_selections"
SESSION_KEYS = (
    STATES_KEY, PENDING_KEY, VIEWED_KEY, FOCUS_KEY, GOTO_KEY, ANNOUNCED_KEY,
    FRAMES_KEY, MARK_ALL_KEY, SELECTIONS_KEY,
)
TITLE_RUN_KEY = "forms_notices_title_run"
TABLE_VERSIONS_KEY = "forms_notices_table_versions"
FLASH_SECONDS = 12


def _normalize(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(character for character in text if not unicodedata.combining(character))
    return re.sub(r"\s+", " ", text).strip().upper()


def timestamp_column(columns: Iterable[Any]) -> str:
    """Columna "Marca temporal" que Google agrega a cada Sheet de respuestas."""

    return next((str(column) for column in columns if _normalize(column) in TIMESTAMP_HEADERS), "")


def _hint_column(columns: Iterable[Any], hints: Iterable[str]) -> str:
    normalized = [(str(column), _normalize(column)) for column in columns]
    for hint in hints:
        for column, text in normalized:
            if hint in text:
                return column
    return ""


def response_summaries(frame: pd.DataFrame) -> dict[str, dict[str, str]]:
    """Paciente, doctor y fecha de cada respuesta, indexados por su clave."""

    if frame is None or frame.empty:
        return {}
    columns = list(frame.columns)
    stamp = timestamp_column(columns)
    if not stamp:
        return {}
    patient, doctor = _hint_column(columns, PATIENT_HINTS), _hint_column(columns, DOCTOR_HINTS)
    summaries = {}
    for _, row in frame.iterrows():
        key = response_key(row.get(stamp, ""))
        if key:
            summaries[key] = {
                "timestamp": str(row.get(stamp, "")).strip(),
                "paciente": str(row.get(patient, "") if patient else "").strip(),
                "doctor": str(row.get(doctor, "") if doctor else "").strip(),
            }
    return summaries


def build_notices(
    sources: list[dict[str, Any]],
    frames: dict[str, pd.DataFrame],
    states: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    """Calcula las respuestas pendientes de un usuario.

    Devuelve ``(avisos de la más nueva a la más vieja, estados)``. Un formulario
    sin estado recibe su línea base; uno que no se pudo leer conserva su estado.
    """

    updated = dict(states)
    notices: list[dict[str, Any]] = []
    for source in sources:
        frame = frames.get(source["key"])
        if frame is None:
            continue
        summaries = response_summaries(frame)
        keys = list(summaries)
        state = updated.get(source["key"])
        state = mark_seen(state, [], keys) if state is not None else baseline_state(keys)
        updated[source["key"]] = state
        for key in pending_keys(keys, state):
            notices.append({"form": source["key"], "label": source["label"], "key": key, **summaries[key]})
    notices.sort(key=lambda notice: notice["key"], reverse=True)
    return notices, updated


def notice_text(notice: dict[str, Any]) -> str:
    """Texto corto de un aviso: formulario, paciente, doctor y fecha."""

    parts = [notice["label"]]
    if notice.get("paciente"):
        parts.append(notice["paciente"])
    if notice.get("doctor"):
        parts.append(f"Dr(a). {notice['doctor']}")
    parts.append(notice.get("timestamp") or notice["key"].replace("T", " "))
    return " · ".join(parts)


def pending_for(form_key: str) -> set[str]:
    return set(st.session_state.get(PENDING_KEY, {}).get(form_key, ()))


def record_viewed(form_key: str, timestamp: Any) -> None:
    """La pestaña de Forms avisa que el usuario ya tiene abierta esta respuesta.

    Aquí deja de contar como nueva en la pestaña. La campana la guarda como vista
    y baja su número en su siguiente ciclo (a más tardar en un minuto), porque la
    pestaña corre en su propio fragmento.
    """

    key = response_key(timestamp)
    if not key or key not in pending_for(form_key):
        return
    viewed = st.session_state.setdefault(VIEWED_KEY, [])
    if [form_key, key] not in viewed:
        viewed.append([form_key, key])
    pending = st.session_state.get(PENDING_KEY, {})
    pending[form_key] = [item for item in pending.get(form_key, []) if item != key]


def new_response_numbers(display_df: pd.DataFrame, form_key: str) -> set[int]:
    """Números de "Respuesta #" que todavía son nuevos para el usuario."""

    pending = pending_for(form_key)
    stamp = timestamp_column(display_df.columns)
    if not pending or not stamp:
        return set()
    return {
        int(number)
        for number, value in zip(display_df["Respuesta #"], display_df[stamp])
        if response_key(value) in pending
    }


def table_widget_key(table_key: str) -> str:
    """Key real de la tabla de respuestas; cambia cada vez que un aviso abre una.

    Con una key nueva el navegador olvida la fila que tenía marcada: si no, la
    reenvía en el siguiente rerun y vuelve a imponerse sobre el selector.
    """

    version = st.session_state.get(TABLE_VERSIONS_KEY, {}).get(table_key, 0)
    return f"{table_key}_{version}" if version else table_key


def remember_selection(selector_key: str, number: Any) -> None:
    """Guarda la respuesta elegida fuera del widget.

    Streamlit borra el estado de un selector que deja de dibujarse (por ejemplo,
    al cambiar de subpestaña); sin esto, al volver se abriría la más nueva y
    quedaría marcada como vista sin que nadie la eligiera.
    """

    st.session_state.setdefault(SELECTIONS_KEY, {})[selector_key] = number


def focus_response(
    review_df: pd.DataFrame,
    display_df: pd.DataFrame,
    form_key: str,
    table_key: str,
    selector_key: str,
) -> pd.DataFrame:
    """Prepara la tabla y el selector de respuestas de un formulario.

    - Recupera la respuesta que el usuario tenía elegida antes de cambiar de
      subpestaña o de pestaña.
    - Deja seleccionada la que pidió un aviso, buscándola por marca temporal y
      no por número de fila.
    - Agrega al final la elegida y las nuevas que no estén entre las 25 más
      recientes, en cada run: así siempre se pueden abrir y el selector no salta
      a la más nueva en el siguiente rerun.
    """

    remembered = st.session_state.get(SELECTIONS_KEY, {}).get(selector_key)
    if selector_key not in st.session_state and remembered is not None:
        st.session_state[selector_key] = remembered
    focus = st.session_state.get(FOCUS_KEY, {})
    key = focus.pop(form_key, "") if isinstance(focus, dict) else ""
    stamp = timestamp_column(review_df.columns)
    if key and stamp:
        matches = review_df[review_df[stamp].map(response_key) == key]
        if not matches.empty:
            st.session_state[selector_key] = int(matches.iloc[-1]["Respuesta #"])
            versions = st.session_state.setdefault(TABLE_VERSIONS_KEY, {})
            versions[table_key] = versions.get(table_key, 0) + 1
    wanted = set(new_response_numbers(review_df, form_key))
    if st.session_state.get(selector_key) is not None:
        wanted.add(st.session_state[selector_key])
    missing = wanted - set(display_df["Respuesta #"].tolist())
    if missing:
        extra = review_df[review_df["Respuesta #"].isin(missing)]
        display_df = pd.concat(
            [display_df, extra.sort_values("Respuesta #", ascending=False)], ignore_index=True
        )
    return display_df


def request_navigation(notice: dict[str, Any]) -> None:
    """Deja pendiente abrir la respuesta; lab_pg lo aplica al inicio del siguiente run."""

    st.session_state[GOTO_KEY] = {"form": notice["form"], "key": notice["key"]}
    viewed = st.session_state.setdefault(VIEWED_KEY, [])
    if [notice["form"], notice["key"]] not in viewed:
        viewed.append([notice["form"], notice["key"]])


def take_navigation() -> dict[str, str] | None:
    """Entrega una sola vez la navegación pedida y deja el enfoque para la pestaña."""

    goto = st.session_state.pop(GOTO_KEY, None)
    if not isinstance(goto, dict) or not goto.get("form"):
        return None
    st.session_state.setdefault(FOCUS_KEY, {})[goto["form"]] = goto.get("key", "")
    return goto


def _read_frames(sources: list[dict[str, Any]]) -> dict[str, pd.DataFrame]:
    """Lee cada Sheet; si uno falla se usa su última lectura buena de la sesión."""

    last = st.session_state.setdefault(FRAMES_KEY, {})
    frames = {}
    for source in sources:
        try:
            frames[source["key"]] = source["read"]()
            last[source["key"]] = frames[source["key"]]
        except Exception:
            if source["key"] in last:
                frames[source["key"]] = last[source["key"]]
    return frames


def _render_title_badge(count: int) -> None:
    """Pone "(N)" en la pestaña del navegador para verlo aunque la app esté atrás."""

    run = st.session_state.get(TITLE_RUN_KEY, 0) + 1
    st.session_state[TITLE_RUN_KEY] = run
    prefix = f"({count}) " if count else ""
    # El número de run obliga a ejecutar el script otra vez: Streamlit restablece
    # el título en cada rerun completo con el de set_page_config.
    components.html(
        f"""<script>/* {run} */
        const doc = window.parent.document;
        doc.title = {json.dumps(prefix)} + doc.title.replace(/^\(\d+\) /, "");
        </script>""",
        height=0,
    )


def _render_flash(fresh: list[dict[str, Any]]) -> None:
    """Aviso flotante que se desvanece solo, como un toast.

    No se usa st.toast: desde el rerun del fragmento escribe en el contenedor de
    eventos y borra el CSS de sólo estilo que otros fragmentos dejaron ahí (por
    ejemplo, el modo pantalla del Tablero). Este aviso vive en la campana.
    """

    plural = "s" if len(fresh) != 1 else ""
    items = "".join(f"<li>{html.escape(notice_text(notice))}</li>" for notice in fresh[:3])
    more = f"<div>Y {len(fresh) - 3} más.</div>" if len(fresh) > 3 else ""
    st.markdown(
        f"""<div class="forms-notices-flash" role="status">
        <strong>🔔 Respuesta{plural} nueva{plural} de Forms</strong><ul>{items}</ul>{more}</div>
        <style>
        .forms-notices-flash {{position:fixed; top:4.25rem; right:1.25rem; z-index:999990;
            max-width:min(26rem, calc(100vw - 2.5rem)); background:#FFFFFF; color:#1B4B36;
            border:1px solid #BFE3CC; border-left:5px solid #12875E; border-radius:14px;
            padding:.75rem 1rem; box-shadow:0 12px 30px rgba(15,63,42,.18); font-size:.92rem;
            pointer-events:none; animation:forms-notices-flash {FLASH_SECONDS}s ease forwards;}}
        .forms-notices-flash ul {{margin:.35rem 0 0; padding-left:1.1rem;}}
        @keyframes forms-notices-flash {{
            0% {{opacity:0; transform:translateY(-6px);}} 4% {{opacity:1; transform:none;}}
            88% {{opacity:1;}} 100% {{opacity:0; visibility:hidden;}}
        }}
        </style>""",
        unsafe_allow_html=True,
    )


def _request_mark_all(listed: list[list[str]]) -> None:
    # Sólo lo que la campana mostró: una respuesta que llegó después sigue siendo nueva.
    st.session_state[MARK_ALL_KEY] = listed


@st.fragment(run_every=REFRESH_SECONDS)
def render_bell(
    current_user: str,
    sources: list[dict[str, Any]],
    load_states: Callable[[str], dict[str, dict[str, Any]]],
    save_states: Callable[[str, dict[str, dict[str, Any]]], None],
    can_navigate: Callable[[], str],
    is_hidden: Callable[[], bool] = lambda: False,
) -> None:
    """Campana con las respuestas nuevas del usuario; se revisa sola cada minuto.

    ``can_navigate`` devuelve "" si se puede saltar a otra vista o el motivo
    por el que no (por ejemplo, cambios sin guardar). ``is_hidden`` se revisa en
    cada ciclo: el modo pantalla del Tablero se activa sin rerun completo.
    """

    if st.session_state.get(OWNER_KEY) != current_user:
        # Otro usuario entró en el mismo navegador: lo de la sesión anterior no es suyo.
        for key in SESSION_KEYS:
            st.session_state.pop(key, None)
        st.session_state[OWNER_KEY] = current_user
    if is_hidden():
        return
    try:
        stored = load_states(current_user)
    except Exception:
        # Sin estado guardado no se sabe qué es nuevo: mejor no avisar que avisar de más.
        return
    session_states = st.session_state.get(STATES_KEY, {})
    states = merge_states(stored, session_states)
    viewed = st.session_state.pop(VIEWED_KEY, [])
    frames = _read_frames(sources)
    keys_by_form = {key: list(response_summaries(frame)) for key, frame in frames.items()}
    for form_key, key in viewed:
        if form_key in states:
            states[form_key] = mark_seen(states[form_key], [key], keys_by_form.get(form_key, [key]))
    for form_key, key in st.session_state.pop(MARK_ALL_KEY, None) or []:
        if form_key in states:
            states[form_key] = mark_seen(states[form_key], [key], keys_by_form.get(form_key, [key]))
    notices, states = build_notices(sources, frames, states)
    st.session_state[STATES_KEY] = states
    st.session_state[PENDING_KEY] = {
        source["key"]: [notice["key"] for notice in notices if notice["form"] == source["key"]]
        for source in sources
    }
    # Se guarda siempre que la sesión sepa algo que el Sheet no tiene: así también
    # se reintenta un guardado fallido o pisado por otro dispositivo del usuario.
    if states != stored:
        try:
            save_states(current_user, states)
        except Exception:
            pass

    announced = st.session_state.setdefault(ANNOUNCED_KEY, set())
    fresh = [notice for notice in notices if (notice["form"], notice["key"]) not in announced]
    if fresh:
        _render_flash(fresh)
    announced.update((notice["form"], notice["key"]) for notice in notices)

    _render_title_badge(len(notices))
    if not notices:
        return
    plural = "s" if len(notices) != 1 else ""
    with st.popover(
        f"🔔 {len(notices)} respuesta{plural} nueva{plural} de Forms",
        type="primary",
        key="forms_notices_popover",
    ):
        st.caption("Toca **Ver** para abrir la pestaña de Forms con esa respuesta seleccionada.")
        for index, notice in enumerate(notices[:MAX_LISTED]):
            text_column, button_column = st.columns([5, 1], vertical_alignment="center")
            text_column.markdown(html.escape(notice_text(notice)))
            if button_column.button("Ver", key=f"forms_notices_open_{notice['form']}_{notice['key']}"):
                blocked = can_navigate()
                if blocked:
                    st.warning(blocked)
                else:
                    request_navigation(notice)
                    st.rerun(scope="app")
        if len(notices) > MAX_LISTED:
            st.caption(f"Y {len(notices) - MAX_LISTED} más en la pestaña 📥 Recibidos de Forms.")
        # El callback corre antes del siguiente ciclo de la campana, que ya las cuenta vistas.
        st.button(
            "✓ Marcar todas como vistas",
            key="forms_notices_mark_all",
            on_click=_request_mark_all,
            args=([[notice["form"], notice["key"]] for notice in notices],),
        )
