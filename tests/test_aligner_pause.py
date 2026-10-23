"""Archivo temporal de casos, con Sheets simulados y sin datos de producción."""
import copy
from pathlib import Path

import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

import alineadores_pg as app
from test_aeris import definitions, sheets
from test_alineadores_pg import PROCESS_VALUES, order
from test_general import general_app


def test_case_pause_is_global_and_does_not_hide_operational_pauses():
    flows = app.parse_process_matrix(PROCESS_VALUES)
    for product, definition in flows.items():
        assert app.PAUSED_STATUS in app.get_allowed_next_statuses(product, definition.normal_statuses[0], flows)
        assert app.PAUSED_STATUS in app.get_manual_stage_statuses(product, definition.normal_statuses[0], flows, "Jime")
        assert app.get_time_limit(product, app.PAUSED_STATUS, flows) is None
    orders = pd.DataFrame([
        order("001", "🚫 Caso en pausa"), order("002", "SOLICITUD DE CAMBIOS"),
        order("003", "IMPRESIÓN EN PAUSA"), order("004", "ENVIADO"), order("005", "CANCELADO"),
        order("", app.PAUSED_STATUS),
    ])
    assert list(app.active_orders(orders, flows)[app.ID_COLUMN]) == ["002", "003"]
    assert list(app.paused_orders(orders, flows)[app.ID_COLUMN]) == ["001"]
    board = app.build_board_table([("Alineadores", orders, pd.DataFrame())], flows)
    assert set(board[app.ID_COLUMN]) == {"002", "003"}
    assert app.status_display_value("Caso en pausa") == "🚫 CASO EN PAUSA"


@pytest.mark.parametrize("sheet_name", app.ORDER_TIME_SHEETS)
def test_pause_resume_preserves_history_and_only_writes_origin(sheets, sheet_name):
    app.st.session_state["aligners_order_sheet"] = sheet_name
    times_name = app.ORDER_TIME_SHEETS[sheet_name]
    before = {name: copy.deepcopy(sheet.values) for name, sheet in sheets.items()}
    snapshot = app.get_snapshot("Jime")
    source = snapshot["table"]
    initial_stage = source.iloc[0][app.STATUS_COLUMN]
    app.initialize_order_timer(source.iloc[0], "Jime", snapshot["definitions"])
    edited = source.copy()
    edited.loc[0, app.STATUS_COLUMN] = "🚫 Caso en pausa"
    assert app.save_workbench_changes(source, source, edited, "Jime", snapshot["definitions"]) == (["001"], [])

    paused = app.get_snapshot("Jime")
    assert paused["table"].empty
    assert paused["sent"].empty
    assert paused["paused"][app.ID_COLUMN].tolist() == ["001"]
    logs = [dict(zip(app.TIMES_HEADERS, row)) for row in sheets[times_name].values[1:]]
    assert logs[0]["FECHA_FIN"]
    assert logs[-1]["ES_PAUSA"] == "Sí"
    assert logs[-1]["FASE_ORDEN"] == "PAUSA"
    assert logs[-1]["STATUS_REANUDAR"] == initial_stage
    assert not logs[-1]["TIEMPO_MAXIMO_HORAS"]
    assert not logs[-1]["FECHA_LIMITE"]

    baseline = app.display_workbench_df(paused["paused"], paused["definitions"])
    edited = baseline.copy()
    edited.loc[0, app.STATUS_COLUMN] = initial_stage
    assert app.save_workbench_changes(paused["paused"], baseline, edited, "Jime",
                                     paused["definitions"], reactivate=True) == (["001"], [])
    refreshed = app.get_snapshot("Jime")
    assert refreshed["table"][app.ID_COLUMN].tolist() == ["001"]
    assert refreshed["paused"].empty
    logs = [dict(zip(app.TIMES_HEADERS, row)) for row in sheets[times_name].values[1:]]
    assert logs[-2]["FECHA_FIN"]
    assert logs[-1]["ES_PAUSA"] == "No"
    assert logs[-1][app.STATUS_COLUMN] == initial_stage
    assert "Reactivado desde Caso en pausa" in logs[-1]["COMENTARIOS_CAMBIO"]
    assert f"CASO EN PAUSA → {initial_stage}" in logs[-1]["COMENTARIOS_CAMBIO"]
    for name, values in before.items():
        if name not in {sheet_name, times_name}:
            assert sheets[name].values == values


def test_paused_reactivation_keeps_permissions_and_concurrency_checks(sheets):
    sheet = sheets[app.SHEET_ORDERS]
    sheet.values[2][1] = app.PAUSED_STATUS
    snapshot = app.get_snapshot("Jime")
    source = snapshot["paused"]
    edited = source.copy()
    edited.loc[0, app.STATUS_COLUMN] = "LISTO STLs PARA IMPRESIÓN"
    for user in ("Juan", "Paula"):
        assert app.save_workbench_changes(source, source, edited, user, definitions(), reactivate=True)[0] == []
    for target in ("ENVIADO", "CANCELADO", "ETAPA INVENTADA"):
        assert app.validate_delta(source.iloc[0], {app.STATUS_COLUMN: target}, definitions(), reactivate=True)
    assert app.validate_delta(source.iloc[0], {"NOMBRE PACIENTE": "Otro"}, definitions(), reactivate=True)
    sheet.values[2][1] = "ENVIADO"
    saved, errors = app.save_workbench_changes(source, source, edited, "Jime", definitions(), reactivate=True)
    assert not saved and any("Otro usuario" in error for error in errors)
    assert len(sheets[app.SHEET_TIMES].values) == 1


def test_paused_changes_protect_navigation_and_discard_only_the_archive(monkeypatch):
    flows = definitions()
    baseline = pd.DataFrame([order(status=app.PAUSED_STATUS, product="AERIS")])
    edited = baseline.copy()
    edited.loc[0, app.STATUS_COLUMN] = "LISTO STLs PARA IMPRESIÓN"
    state = {
        "aligners_paused_editor_key": "paused_grid", "aligners_paused_editor_baseline": baseline,
        "paused_grid": {"rows": edited.to_dict("records")},
        "aligners_snapshot": {"definitions": flows}, "aligners_active_tab": "📋 Seguimiento",
        "aligners_primary_tabs_Jime": app.POLANCO_TAB_LABEL,
        "order_detail_drafts_aligners": {"other": {"changes": {"NOMBRE PACIENTE": "Conservar"}}},
    }
    monkeypatch.setattr(app.st, "session_state", state)
    assert app.paused_change_count(flows) == 1
    assert app.pending_change_count(flows) == 2
    app.guard_tracking_tab_change("Jime")
    assert state["aligners_primary_tabs_Jime"] == "📋 Seguimiento"
    app.discard_archive_changes("paused")
    assert app.paused_change_count(flows) == 0
    assert app.order_detail.pending_count("aligners") == 1


def test_general_pause_leaves_active_list_and_exposes_archive():
    at = general_app()
    at.session_state["grid_edits"] = {"🏢 Polanco|001": {app.STATUS_COLUMN: "🚫 CASO EN PAUSA"}}
    at.run()
    assert not at.exception
    at.button(key="general_aligners_save").click().run()
    assert not at.exception and not at.error
    assert at.session_state["grid_shown"][app.ORIGIN_COLUMN].tolist() == ["📋 Seguimiento"]
    assert any(item.label == "🚫 Caso en pausa · 1 pedido(s)" for item in at.expander)
    assert at.session_state["sheets"][app.SHEET_POLANCO].values[2][1] == app.PAUSED_STATUS
    assert at.session_state["sheets"][app.SHEET_ORDERS].values[2][1] == "REVISIÓN DE ARCHIVOS"

    # Un solo expander por archivo; dentro, un expander cerrado por origen.
    assert [item.label for item in at.expander if "pedido(s)" in item.label] == [
        "🚫 Caso en pausa · 1 pedido(s)", "🚚 Enviados · 0 pedido(s)",
    ]
    at.session_state["general_aligners_paused_expander"] = True
    at.run()
    assert not at.exception
    assert [item.label for item in at.expander if "pedido(s)" in item.label] == [
        "🚫 Caso en pausa · 1 pedido(s)",
        "📋 Seguimiento · 0 pedido(s)", "🏢 Polanco · 1 pedido(s)", "🌬️ AERIS · 0 pedido(s)",
        "🚚 Enviados · 0 pedido(s)",
    ]
    assert not any(item.key == "general_polanco_aligners_paused_search" for item in at.text_input)
    # Al abrir Polanco aparece su buscador y su tabla.
    at.session_state["general_aligners_paused_expander"] = True
    at.session_state["general_polanco_aligners_paused_origin_expander"] = True
    at.run()
    assert not at.exception
    assert any(item.key == "polanco_aligners_paused_search" for item in at.text_input)


def test_paused_archive_filters_reactivates_and_retains_pending_edits():
    root = str(Path(__file__).resolve().parents[1])
    script = f'''
import sys
sys.path.insert(0, {root!r})
sys.path.insert(0, {str(Path(__file__).parent)!r})
import streamlit as st
import pandas as pd
import alineadores_pg as app
from unittest.mock import patch
from test_polanco import Sheet, FORM_CATALOG, definitions

if "sheets" not in st.session_state:
    app.read_order_values.clear()
    app.read_times_values.clear()
    st.session_state["sheets"] = {{
        app.SHEET_ORDERS: Sheet([[], [app.ID_COLUMN, app.STATUS_COLUMN, app.PRODUCT_COLUMN,
            "NOMBRE DOCTOR", "NOMBRE PACIENTE"],
            ["001", "CASO EN PAUSA", "Graphy", "Doctora", "Paciente"],
            ["002", "ENVIADO", "Graphy", "Doctor", "Otro"]]),
        app.SHEET_TIMES: Sheet([app.TIMES_HEADERS]),
    }}

def grid_editor(grid, options, key):
    st.session_state["shown_options"] = options
    edited = grid.copy()
    if "aligners_paused_grid_" in key and st.session_state.get("resume"):
        if st.session_state.setdefault("edit_key", key) == key:
            edited.loc[0, app.STATUS_COLUMN] = "REVISIÓN DE ARCHIVOS"
            st.session_state[key] = {{"rows": edited.to_dict("records")}}
    return edited

with patch.object(app, "get_worksheet", st.session_state["sheets"].__getitem__), \\
     patch.object(app, "read_process_definitions", definitions), \\
     patch.object(app, "get_order_form_catalog", lambda: FORM_CATALOG), \\
     patch.object(app, "render_aligners_grid", grid_editor):
    # AppTest no conserva el estado del expander al interactuar con otro widget.
    if st.session_state.get("test_open"):
        st.session_state["aligners_paused_expander"] = True
    app.render_workbench("Jime")
'''
    at = AppTest.from_string(script, default_timeout=15).run()
    assert not at.exception
    assert any(item.label == "🚫 Caso en pausa · 1 pedido(s)" for item in at.expander)
    assert not any(item.key == "aligners_paused_search" for item in at.text_input)
    at.session_state["test_open"] = True
    at.run()
    assert not at.exception
    assert at.multiselect(key="aligners_paused_products").options == ["Graphy"]
    assert at.button(key="aligners_paused_save").disabled
    options = at.session_state["shown_options"]
    assert next(col for col in options["columnDefs"] if col["field"] == "NOMBRE PACIENTE")["editable"] is False
    at.text_input(key="aligners_paused_search").set_value("sin coincidencias").run()
    assert not at.exception
    assert any("coincidan" in item.value for item in at.info)
    at.text_input(key="aligners_paused_search").set_value("").run()
    at.session_state["resume"] = True
    at.run().run()
    assert at.button(key="aligners_refresh").disabled
    assert at.text_input(key="aligners_paused_search").disabled
    at.session_state["test_open"] = False
    at.session_state["aligners_paused_expander"] = False
    at.run()
    assert not at.exception
    assert at.session_state["aligners_paused_expander"] is True
    assert not at.button(key="aligners_paused_save").disabled
    at.button(key="aligners_paused_save").click().run()
    assert not at.exception and not at.error
    assert at.session_state["sheets"][app.SHEET_ORDERS].values[2][1] == "REVISIÓN DE ARCHIVOS"
    assert any(item.label == "🚫 Caso en pausa · 0 pedido(s)" for item in at.expander)
    assert not at.button(key="aligners_refresh").disabled
