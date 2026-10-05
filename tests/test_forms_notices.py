"""Pruebas de los avisos de respuestas nuevas de Forms (estado, campana y saltos)."""

import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import forms_notices as notices


def keys(*timestamps):
    return [notices.response_key(value) for value in timestamps]


def test_sheet_timestamp_is_day_first_and_sortable():
    assert notices.response_key("2/10/2026 12:52:50") == "2026-10-02T12:52:50"
    assert notices.response_key("21/9/2026 9:01:21") == "2026-09-21T09:01:21"
    assert notices.response_key("2026-10-02 12:52:50") == "2026-10-02T12:52:50"
    assert notices.response_key("") == ""
    assert notices.response_key("ayer") == ""
    assert notices.response_key("31/2/2026 10:00:00") == ""
    # 21/9 9:01 es anterior a 2/10 aunque como texto "21/9" > "2/10".
    assert sorted(keys("2/10/2026 11:37:18", "21/9/2026 9:01:21")) == keys(
        "21/9/2026 9:01:21", "2/10/2026 11:37:18"
    )


def test_first_read_sets_baseline_without_announcing_history():
    current = keys("1/10/2026 19:15:05", "2/10/2026 12:52:50")
    assert notices.pending_keys(current, None) == []
    state = notices.baseline_state(current)
    assert state == {"base": "2026-10-02T12:52:50", "vistas": []}
    assert notices.pending_keys(current, state) == []
    assert notices.baseline_state([]) == {"base": "", "vistas": []}


def test_new_responses_stay_pending_until_each_one_is_seen():
    state = notices.baseline_state(keys("1/10/2026 19:15:05"))
    current = keys("1/10/2026 19:15:05", "2/10/2026 11:37:18", "2/10/2026 12:52:50")
    newest, = keys("2/10/2026 12:52:50")
    oldest_new, = keys("2/10/2026 11:37:18")
    assert notices.pending_keys(current, state) == [oldest_new, newest]

    # Abrir la más nueva no oculta la anterior que sigue sin verse.
    state = notices.mark_seen(state, [newest], current)
    assert state == {"base": "2026-10-01T19:15:05", "vistas": [newest]}
    assert notices.pending_keys(current, state) == [oldest_new]

    # Al verla, base avanza y la lista de vistas queda vacía.
    state = notices.mark_seen(state, [oldest_new], current)
    assert state == {"base": newest, "vistas": []}
    assert notices.pending_keys(current, state) == []


def test_marking_several_seen_and_rows_deleted_from_sheet():
    state = notices.baseline_state(keys("1/10/2026 19:15:05"))
    current = keys("1/10/2026 19:15:05", "2/10/2026 11:37:18", "2/10/2026 12:52:50")
    state = notices.mark_seen(state, current[1:], current)
    assert state == {"base": "2026-10-02T12:52:50", "vistas": []}
    # Si borran filas viejas del Sheet, la siguiente respuesta sí avisa.
    later = keys("2/10/2026 12:52:50", "3/10/2026 9:00:00")
    assert notices.pending_keys(later, state) == keys("3/10/2026 9:00:00")
    assert notices.mark_seen(None, [], []) == {"base": "", "vistas": []}


def test_states_roundtrip_and_ignore_damaged_json():
    states = {"td": {"base": "2026-10-02T12:52:50", "vistas": ["2026-10-03T09:00:00"]}}
    assert notices.load_states(notices.dump_states(states)) == states
    assert notices.load_states("") == {}
    assert notices.load_states("{dañado") == {}
    assert notices.load_states('["td"]') == {}
    assert notices.load_states('{"td": {"vistas": []}, "aparatos": {"base": ""}}') == {
        "aparatos": {"base": "", "vistas": []}
    }


TD_ROWS = [
    {
        "Marca temporal": "1/10/2026 19:15:05",
        "Nombre completo del paciente:": "Mercedez Ruiz",
        "Nombre completo del médico tratante:": "Carolina Arguello",
    },
]
NEW_TD_ROW = {
    "Marca temporal": "2/10/2026 12:52:50",
    "Nombre completo del paciente:": "Regina Gonzalez",
    "Nombre completo del médico tratante:": "Emilio Ruiz",
}
LATE_TD_ROW = {
    "Marca temporal": "2/10/2026 12:53:40",
    "Nombre completo del paciente:": "Llegó después",
    "Nombre completo del médico tratante:": "Otro",
}


def test_summaries_find_patient_and_doctor_in_each_form():
    import pandas as pd

    aparatos = pd.DataFrame([{
        "Marca temporal": "25/8/2026 15:57:44",
        "Doctor tratante:\n(Nombre completo)": "Daniela Rojo",
        "Edad del paciente:": "13 años",
        "Nombre completo del paciente:": "Victoria",
    }])
    assert notices.response_summaries(aparatos) == {
        "2026-08-25T15:57:44": {"timestamp": "25/8/2026 15:57:44", "paciente": "Victoria", "doctor": "Daniela Rojo"}
    }
    assert notices.response_summaries(pd.DataFrame([{"Otra": "x"}])) == {}
    notice = {"label": "🦷 TD", "key": "2026-10-02T12:52:50", "paciente": "Regina", "doctor": "Emilio",
              "timestamp": "2/10/2026 12:52:50"}
    assert notices.notice_text(notice) == "🦷 TD · Regina · Dr(a). Emilio · 2/10/2026 12:52:50"


def test_build_notices_sets_baselines_and_keeps_unreadable_forms():
    import pandas as pd

    sources = [{"key": "td", "label": "TD"}, {"key": "aparatos", "label": "Aparatos"}]
    stored_aparatos = {"base": "2026-09-01T00:00:00", "vistas": []}
    found, states = notices.build_notices(
        sources, {"td": pd.DataFrame(TD_ROWS)}, {"aparatos": stored_aparatos}
    )
    assert found == []
    assert states == {"td": {"base": "2026-10-01T19:15:05", "vistas": []}, "aparatos": stored_aparatos}

    frames = {"td": pd.DataFrame([*TD_ROWS, NEW_TD_ROW])}
    found, again = notices.build_notices(sources, frames, states)
    assert again == states
    assert [(item["form"], item["paciente"]) for item in found] == [("td", "Regina Gonzalez")]


def test_merge_never_turns_a_seen_response_back_into_new():
    stored = {"td": {"base": "2026-10-02T12:52:50", "vistas": []}}
    local = {
        "td": {"base": "2026-10-01T19:15:05", "vistas": ["2026-10-03T09:00:00"]},
        "aparatos": {"base": "2026-09-01T00:00:00", "vistas": []},
    }
    assert notices.merge_states(stored, local) == {
        "aparatos": {"base": "2026-09-01T00:00:00", "vistas": []},
        "td": {"base": "2026-10-02T12:52:50", "vistas": ["2026-10-03T09:00:00"]},
    }


def lab_app(user="Admin"):
    from streamlit.testing.v1 import AppTest

    root = str(Path(__file__).resolve().parents[1])
    script = f'''
import sys
sys.path.insert(0, {root!r})
import pandas as pd
import streamlit as st
import lab_pg as app
from unittest.mock import patch

rows = list({TD_ROWS!r})
if st.session_state.get("test_new_response"):
    rows.append({NEW_TD_ROW!r})
if st.session_state.get("test_late_response"):
    rows.append({LATE_TD_ROW!r})
store = st.session_state.setdefault("test_store", {{}})
saves = st.session_state.setdefault("test_saves", [])
sources = [{{"key": "td", "label": "🦷 Prescripción Alineadores TD", "read": lambda: pd.DataFrame(rows)}}]

def save(user, states):
    saves.append(user)
    store.update(states)

st.session_state["authenticated_user"] = {user!r}
with (
    patch.object(app, "require_authenticated_user", lambda: {user!r}),
    patch.object(app, "render_selected_workspace", lambda view, user: st.write(f"vista: {{view}}")),
    patch.object(app, "forms_notice_sources", lambda user: sources),
    patch.object(app, "load_forms_notice_states", lambda user: {{k: dict(v) for k, v in store.items()}}),
    patch.object(app, "save_forms_notice_states", save),
    patch.object(app, "workspace_has_pending_edits", lambda view: st.session_state.get("test_pending", False)),
    patch.object(app, "forms_notice_hidden", lambda: st.session_state.get("test_hidden", False)),
):
    app.main()
'''
    return AppTest.from_string(script, default_timeout=15)


def bell_label(at):
    return [item.proto.popover.label for item in at.get("popover")]


def flashes(at):
    return [item.value for item in at.markdown if "forms-notices-flash" in item.value]


def test_bell_announces_new_response_and_opens_it_in_forms_tab():
    import lab_pg as app

    at = lab_app().run()
    assert not at.exception
    # Primera vez: se guarda la línea base y no se anuncia el histórico.
    assert at.session_state["test_store"] == {"td": {"base": "2026-10-01T19:15:05", "vistas": []}}
    assert bell_label(at) == []

    at.session_state["test_new_response"] = True
    at.run()
    assert not at.exception
    assert bell_label(at) == ["🔔 1 respuesta nueva de Forms"]
    (flash,) = flashes(at)
    assert "Regina Gonzalez" in flash
    assert not at.get("toast")
    assert any(
        "🦷 Prescripción Alineadores TD · Regina Gonzalez · Dr(a). Emilio Ruiz" in item.value
        for item in at.markdown
    )

    at.button(key="forms_notices_open_td_2026-10-02T12:52:50").click().run()
    assert not at.exception
    assert at.session_state[app.LAB_WORKSPACE_STATE_KEY] == app.LAB_VIEW_ALIGNERS
    assert at.session_state["aligners_primary_tabs_Admin"] == "📥 Recibidos de Forms"
    assert at.session_state["aligners_forms_sub_tabs"] == "🦷 Prescripción Alineadores TD"
    assert at.session_state["forms_notices_focus"] == {"td": "2026-10-02T12:52:50"}
    assert at.query_params["vista"] == ["alineadores"]
    assert at.session_state["test_store"]["td"] == {"base": "2026-10-02T12:52:50", "vistas": []}
    assert bell_label(at) == []


def test_bell_does_not_jump_with_unsaved_changes_and_marks_all_seen():
    import lab_pg as app

    at = lab_app("Vero").run()
    at.session_state["test_new_response"] = True
    at.run()
    assert bell_label(at) == ["🔔 1 respuesta nueva de Forms"]
    assert len(flashes(at)) == 1

    at.session_state["test_pending"] = True
    at.button(key="forms_notices_open_td_2026-10-02T12:52:50").click().run()
    assert not at.exception
    assert app.LAB_WORKSPACE_STATE_KEY not in at.session_state or (
        at.session_state[app.LAB_WORKSPACE_STATE_KEY] != app.LAB_VIEW_ALIGNERS
    )
    assert [item.value for item in at.warning] == [
        "Guarda o descarta los cambios pendientes antes de abrir la respuesta."
    ]
    # El mismo aviso no vuelve a salir flotando en cada ciclo.
    assert flashes(at) == []

    # Llega otra respuesta entre que se dibujó la lista y el clic en "Marcar todas".
    at.session_state["test_late_response"] = True
    at.button(key="forms_notices_mark_all").click().run()
    assert not at.exception
    assert at.session_state["test_store"]["td"] == {"base": "2026-10-02T12:52:50", "vistas": []}
    # Sólo se marcó lo que la lista mostraba; la tardía sigue avisando.
    assert bell_label(at) == ["🔔 1 respuesta nueva de Forms"]
    assert "Llegó después" in flashes(at)[0]


def test_bell_stays_quiet_in_board_screen_mode_and_resumes_after():
    at = lab_app().run()
    at.session_state["test_new_response"] = True
    at.session_state["test_hidden"] = True
    at.run()
    assert not at.exception
    assert bell_label(at) == [] and flashes(at) == []

    at.session_state["test_hidden"] = False
    at.run()
    assert bell_label(at) == ["🔔 1 respuesta nueva de Forms"]


def test_bell_saves_again_what_another_device_overwrote():
    at = lab_app().run()
    at.session_state["test_new_response"] = True
    at.run()
    at.button(key="forms_notices_open_td_2026-10-02T12:52:50").click().run()
    assert at.session_state["test_store"]["td"]["base"] == "2026-10-02T12:52:50"
    saves = len(at.session_state["test_saves"])

    # Otro dispositivo del mismo usuario guardó encima su estado viejo.
    at.session_state["test_store"]["td"] = {"base": "2026-10-01T19:15:05", "vistas": []}
    at.run()
    assert at.session_state["test_store"]["td"] == {"base": "2026-10-02T12:52:50", "vistas": []}
    assert len(at.session_state["test_saves"]) == saves + 1
    at.run()
    assert len(at.session_state["test_saves"]) == saves + 1  # Ya coincide: no vuelve a escribir.


def test_each_user_only_gets_forms_they_can_open(monkeypatch):
    import lab_pg as app

    monkeypatch.setattr(app, "get_forms_config", lambda: {"sheet_id": "aparatos-sheet", "worksheet": "R"})
    monkeypatch.setattr(app.st, "secrets", {})
    admin = [source["key"] for source in app.forms_notice_sources("Admin")]
    vero = [source["key"] for source in app.forms_notice_sources("Vero")]
    assert admin == ["aparatos", "td", "marca_blanca", "otros_productos"]
    assert vero == ["td", "marca_blanca", "otros_productos"]

    monkeypatch.setattr(app, "get_forms_config", lambda: {"sheet_id": "", "worksheet": "R"})
    assert [source["key"] for source in app.forms_notice_sources("Jime")] == ["td", "marca_blanca", "otros_productos"]


class FakeNoticeSheet:
    def __init__(self, values):
        self.values = values
        self.appended = []
        self.updated = []

    def get_all_values(self):
        return [list(row) for row in self.values]

    def append_row(self, payload, value_input_option):
        self.appended.append(payload)

    def update_cells(self, cells, value_input_option):
        self.updated.append([(cell.row, cell.col, cell.value) for cell in cells])


def test_notice_state_sheet_upserts_one_row_per_user_and_merges(monkeypatch):
    import json
    import lab_pg as app

    stored = {"td": {"base": "2026-10-02T12:52:50", "vistas": []}}
    sheet = FakeNoticeSheet([
        app.FORMS_NOTICE_HEADERS,
        ["Jime", json.dumps({"td": {"base": "2026-09-01T00:00:00", "vistas": []}}), "x"],
        ["Admin", "{dañado", "x"],
        ["Jime", json.dumps(stored), "y"],
    ])
    monkeypatch.setattr(app, "get_forms_notices_worksheet", lambda: sheet)
    monkeypatch.setattr(app, "app_now", lambda: datetime(2026, 10, 4, 12, 0))
    assert app.forms_notice_row(sheet.values, "Jime") == (4, json.dumps(stored))
    assert app.forms_notice_row(sheet.values, "Lesly") == (None, "")

    app.save_forms_notice_states("Jime", {"td": {"base": "2026-10-01T00:00:00", "vistas": ["2026-10-03T09:00:00"]}})
    (row,) = sheet.updated
    assert row[0][:2] == (4, 1) and row[0][2] == "Jime"
    assert json.loads(row[1][2]) == {"td": {"base": "2026-10-02T12:52:50", "vistas": ["2026-10-03T09:00:00"]}}

    app.save_forms_notice_states("Lesly", {"aparatos": {"base": "", "vistas": []}})
    assert sheet.appended[0][:2] == ["Lesly", '{"aparatos": {"base": "", "vistas": []}}']


def test_forms_tab_preselects_notice_response_and_marks_it_seen():
    from streamlit.testing.v1 import AppTest

    root = str(Path(__file__).resolve().parents[1])
    rows = [
        {"Marca temporal": f"{day}/9/2026 10:00:00", "Nombre completo del paciente:": f"Paciente {day}"}
        for day in range(1, 31)
    ]
    script = f'''
import sys
sys.path.insert(0, {root!r})
import pandas as pd
import streamlit as st
import alineadores_pg as app
from unittest.mock import patch

responses = pd.DataFrame({rows!r})
if "test_started" not in st.session_state:
    st.session_state["test_started"] = True
    # Pidió abrir una respuesta vieja (fuera de las 25 más nuevas) y hay dos pendientes.
    st.session_state["forms_notices_focus"] = {{"td": "2026-09-02T10:00:00"}}
    st.session_state["forms_notices_pending"] = {{"td": ["2026-09-02T10:00:00", "2026-09-30T10:00:00"]}}
    st.session_state["aligners_forms_table_td"] = {{"selection": {{"rows": [0], "columns": []}}}}
form = {{"key": "td", "label": "TD", "sheet_id": "sheet", "worksheet": "Respuestas", "form_id": ""}}
with patch.object(app, "read_forms_responses_df", lambda *_: responses), \\
        patch.object(app, "read_form_sections", lambda _: ([], "")):
    app.render_aligners_form_review(form)
'''
    at = AppTest.from_string(script, default_timeout=15).run()
    assert not at.exception
    selector = at.selectbox(key="aligners_forms_selector_td")
    assert selector.value == 3  # "Respuesta #" de la fila del 2/9.
    assert all("🆕" not in option for option in selector.options)
    assert any(item.value == "🆕 Respuestas nuevas que todavía no abres: #31, #3" for item in at.caption)
    assert at.session_state["forms_notices_viewed"] == [["td", "2026-09-02T10:00:00"]]
    assert at.session_state["forms_notices_pending"] == {"td": ["2026-09-30T10:00:00"]}
    assert at.session_state["forms_notices_focus"] == {}
    # La tabla usa otra key: la fila que el navegador tenía marcada ya no manda.
    assert at.session_state["forms_notices_table_versions"] == {"aligners_forms_table_td": 1}

    # En el siguiente rerun (p. ej. descargar el PDF) sigue en la misma respuesta.
    at.run()
    assert not at.exception
    assert at.selectbox(key="aligners_forms_selector_td").value == 3
    assert at.session_state["forms_notices_viewed"] == [["td", "2026-09-02T10:00:00"]]


def test_another_user_in_same_browser_starts_with_clean_notice_state():
    at = lab_app("Jime").run()
    at.session_state["test_new_response"] = True
    at.run()
    at.session_state["forms_notices_viewed"] = [["td", "2026-10-02T12:52:50"]]
    at.session_state["forms_notices_states"] = {"td": {"base": "2099-01-01T00:00:00", "vistas": []}}
    at.session_state["forms_notices_owner"] = "Admin"  # Simula que antes había entrado Admin.
    at.run()
    assert not at.exception
    # Lo que Admin dejó en la sesión no se guarda como visto para Jime.
    assert at.session_state["test_store"]["td"] == {"base": "2026-10-01T19:15:05", "vistas": []}
    assert bell_label(at) == ["🔔 1 respuesta nueva de Forms"]


class FakeResponseSheet:
    def __init__(self, values=None, error=None):
        self.values, self.error = values, error

    def get_all_values(self):
        if self.error:
            raise self.error
        return self.values


def test_bell_reader_has_its_own_cache_and_never_waits_on_quota(monkeypatch):
    import gspread
    import pandas as pd
    import lab_pg as app

    sheet = FakeResponseSheet([["Marca temporal", "Paciente", "Paciente"], ["2/10/2026 12:52:50", "Regina"]])
    monkeypatch.setattr(app, "get_forms_notice_response_worksheet", lambda *_: sheet)
    app.read_forms_notice_responses.clear()
    frame = app.read_forms_notice_responses("sheet-1", "Respuestas")
    assert list(frame.columns) == ["Marca temporal", "Paciente", "Paciente_2"]
    assert frame.iloc[0].tolist() == ["2/10/2026 12:52:50", "Regina", ""]
    # Guardar algo en la app vacía los cachés de lectura, pero no el de la campana.
    sheet.values = []
    app.clear_sheet_data_cache()
    pd.testing.assert_frame_equal(app.read_forms_notice_responses("sheet-1", "Respuestas"), frame)

    class Response:
        status_code = 429
        text = "RATE_LIMIT_EXCEEDED"

        def json(self):
            return {"error": {"code": 429, "message": "RATE_LIMIT_EXCEEDED", "status": "RESOURCE_EXHAUSTED"}}

    cleared = []
    monkeypatch.setattr(app, "get_forms_notice_response_worksheet", type("Cached", (), {
        "__call__": lambda self, *_: FakeResponseSheet(error=gspread.exceptions.APIError(Response())),
        "clear": lambda self, *args: cleared.append(args),
    })())
    monkeypatch.setattr(app.time, "sleep", lambda seconds: (_ for _ in ()).throw(AssertionError("esperó")))
    app.read_forms_notice_responses.clear()
    try:
        app.read_forms_notice_responses("sheet-1", "Respuestas")
    except gspread.exceptions.APIError:
        pass
    else:
        raise AssertionError("debía fallar")
    assert cleared == [("sheet-1", "Respuestas")]
    app.read_forms_notice_responses.clear()


def test_forms_tab_keeps_choice_across_sub_tabs_and_lists_old_pending():
    from streamlit.testing.v1 import AppTest

    root = str(Path(__file__).resolve().parents[1])
    rows = [
        {"Marca temporal": f"{day}/9/2026 10:00:00", "Nombre completo del paciente:": f"Paciente {day}"}
        for day in range(1, 31)
    ]
    script = f'''
import sys
sys.path.insert(0, {root!r})
import pandas as pd
import streamlit as st
import alineadores_pg as app
from unittest.mock import patch

responses = pd.DataFrame({rows!r})
# Vero estuvo fuera: tiene pendientes también fuera de las 25 más nuevas.
st.session_state.setdefault("forms_notices_pending", {{"td": ["2026-09-01T10:00:00", "2026-09-04T10:00:00"]}})
form = {{"key": "td", "label": "TD", "sheet_id": "sheet", "worksheet": "Respuestas", "form_id": ""}}
with patch.object(app, "read_forms_responses_df", lambda *_: responses), \\
        patch.object(app, "read_form_sections", lambda _: ([], "")):
    if not st.session_state.get("test_other_sub_tab"):
        app.render_aligners_form_review(form)
    else:
        st.write("otra subpestaña")
'''
    at = AppTest.from_string(script, default_timeout=15).run()
    assert not at.exception
    selector = at.selectbox(key="aligners_forms_selector_td")
    assert {"2", "5"} <= set(selector.options)  # #2 y #5 quedan fuera de las 25 más nuevas.
    assert any(item.value == "🆕 Respuestas nuevas que todavía no abres: #5, #2" for item in at.caption)

    selector.set_value(5).run()
    assert at.selectbox(key="aligners_forms_selector_td").value == 5
    # Cambia a otra subpestaña (el selector deja de dibujarse) y regresa.
    at.session_state["test_other_sub_tab"] = True
    at.run()
    at.session_state["test_other_sub_tab"] = False
    at.run()
    assert not at.exception
    assert at.selectbox(key="aligners_forms_selector_td").value == 5
