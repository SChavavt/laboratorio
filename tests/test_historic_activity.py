"""Orden inicial con historial previo, sin editar casos ni escribir en Sheets."""
import copy
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

import alineadores_pg as align
import lab_pg as lab
from order_activity import (ACTIVITY_ORDER_VERSION, HISTORY_ACTIVITY_COLUMN, LAST_EDIT_COLUMN,
                            RECENT_FIRST, SHEET_ORDER, initialize_recent_order,
                            recent_first, with_history_activity)
from test_general import general_app
from test_polanco import Sheet


def test_history_and_new_saves_sort_together_without_inventing_edit_dates():
    orders = pd.DataFrame([
        {"id": "never", "FECHA DE RECEPCIÓN": "2026/10/26"},
        {"id": "old", "FECHA DE RECEPCIÓN": "2026/10/25"},
        {"id": "latest", LAST_EDIT_COLUMN: "2026-10-24T23:00:00Z"},
        {"id": "saved", LAST_EDIT_COLUMN: "2026-10-25T12:00:00-06:00"},
        {"id": "undated"},
    ]).fillna("")
    history = pd.DataFrame([
        {"id": "old", "FECHA_REGISTRO_LOG": "2026-10-20 12:00:00", "FECHA_LIMITE": "2030-01-01"},
        {"id": "latest", "FECHA_REGISTRO_LOG": "2026-10-25 13:00:00"},
        {"id": "latest", "FECHA_REGISTRO_LOG": "2026-10-22 12:00:00"},
        {"id": "saved", "FECHA_REGISTRO_LOG": "2026-10-20 13:00:00"},
        {"id": "undated", "FECHA_REGISTRO_LOG": "fecha inválida"},
    ])
    original_orders, original_history = orders.copy(deep=True), history.copy(deep=True)
    result = recent_first(with_history_activity(orders, history, "id"))
    assert result["id"].tolist() == ["latest", "saved", "old", "never", "undated"]
    assert result.loc[result["id"] == "latest", LAST_EDIT_COLUMN].iloc[0] == "2026-10-24T23:00:00Z"
    pd.testing.assert_frame_equal(orders, original_orders)
    pd.testing.assert_frame_equal(history, original_history)


@pytest.mark.parametrize("fields", [
    {"FECHA_INICIO": "2026-10-25", "HORA_INICIO": "13:02:03"},
    {"FECHA_FIN": "2026/10/25", "HORA_FIN": "13:02:03"},
    {"PAGO_FECHA": "25/10/2026 13:02:03"},
    {"FECHA_IMPRESION": "25-10-2026", "HORA_IMPRESION": "13:02:03"},
])
def test_real_historical_events_use_mexico_time_and_ignore_deadlines(fields):
    result = with_history_activity(pd.DataFrame([{"id": "001"}]),
        pd.DataFrame([{"id": "001", **fields, "FECHA_LIMITE": "2030-01-01"}]), "id")
    assert result.iloc[0][HISTORY_ACTIVITY_COLUMN] == pd.Timestamp("2026-10-25T19:02:03Z")


def test_historical_folios_do_not_merge_ambiguous_cases_or_unrelated_sheets():
    history = pd.DataFrame([{"id": "1", "FECHA_REGISTRO_LOG": "2026-10-25 12:00:00"}])
    active = pd.DataFrame([{"id": "001"}])
    assert pd.notna(with_history_activity(active, history, "id").iloc[0][HISTORY_ACTIVITY_COLUMN])
    # El otro folio puede estar archivado; cuenta igualmente para resolver la ambigüedad.
    assert pd.isna(with_history_activity(active, history, "id", order_ids=["001", "01"])
                   .iloc[0][HISTORY_ACTIVITY_COLUMN])
    assert pd.isna(with_history_activity(active, history, "id", resolve_legacy=False)
                   .iloc[0][HISTORY_ACTIVITY_COLUMN])
    duplicated = pd.DataFrame([{"id": "001"}, {"id": "001"}])
    assert with_history_activity(duplicated, history, "id")[HISTORY_ACTIVITY_COLUMN].isna().all()
    assert with_history_activity(active, pd.DataFrame(), "id")[HISTORY_ACTIVITY_COLUMN].isna().all()


def test_cases_without_history_sort_by_reception_then_keep_undated_rows_stable():
    orders = pd.DataFrame([
        {"id": "no-date-1"}, {"id": "older", "FECHA DE RECEPCIÓN": "2026/10/01"},
        {"id": "newer", "FECHA DE RECEPCIÓN": "02/10/2026"}, {"id": "no-date-2"},
    ]).fillna("")
    assert recent_first(orders)["id"].tolist() == ["newer", "older", "no-date-1", "no-date-2"]


def test_open_session_switches_to_recent_once_and_preserves_pending_changes():
    state = {"order": SHEET_ORDER, "order_activity_version": 1}
    before = state.copy()
    assert not initialize_recent_order(state, "order", pending=True)
    assert state == before
    assert initialize_recent_order(state, "order", pending=False)
    assert state["order"] == RECENT_FIRST
    assert state["order_activity_version"] == ACTIVITY_ORDER_VERSION
    state["order"] = SHEET_ORDER
    assert not initialize_recent_order(state, "order", pending=False)
    assert state["order"] == SHEET_ORDER


def test_general_orders_are_already_sorted_from_each_origins_history_without_writes():
    align.read_order_values.clear()
    align.read_times_values.clear()
    headers = [align.ID_COLUMN, "STATUS", "PRODUCTO", "NOMBRE DOCTOR", "NOMBRE PACIENTE", "FECHA DE RECEPCIÓN"]
    data = {
        align.SHEET_ORDERS: Sheet([[], headers,
            ["001", "REVISIÓN DE ARCHIVOS", "Graphy", "Doctor", "Anterior", "2026-10-01"],
            ["002", "REVISIÓN DE ARCHIVOS", "Graphy", "Doctor", "Sin historial", "2026-10-26"]]),
        align.SHEET_POLANCO: Sheet([[], headers,
            ["001", "REVISIÓN DE ARCHIVOS", "Graphy", "Doctor", "Más reciente", "2026-10-01"]]),
        align.SHEET_AERIS: Sheet([[], headers]),
    }
    for orders_sheet, times_sheet in align.ORDER_TIME_SHEETS.items():
        day = "2026-10-25" if orders_sheet == align.SHEET_POLANCO else "2026-10-23"
        values = [align.TIMES_HEADERS]
        if orders_sheet != align.SHEET_AERIS:
            log = {align.ID_COLUMN: "001", "STATUS": "REVISIÓN DE ARCHIVOS", "FECHA_INICIO": day,
                   "HORA_INICIO": "12:00:00", "FECHA_REGISTRO_LOG": f"{day} 12:00:00"}
            values.append([log.get(column, "") for column in align.TIMES_HEADERS])
        data[times_sheet] = Sheet(values)
    before = {key: copy.deepcopy(sheet.values) for key, sheet in data.items()}
    at = general_app()
    at.session_state["sheets"] = data
    at.session_state["general_order_mode"] = SHEET_ORDER  # Sesión abierta con el orden viejo.
    at.run()
    assert not at.exception and not at.error
    assert at.selectbox(key="general_order_mode").value == RECENT_FIRST
    assert at.session_state["grid_shown"][[align.ORIGIN_COLUMN, align.ID_COLUMN]].values.tolist() == [
        ["🏢 Polanco", "001"], ["📋 Seguimiento", "001"], ["📋 Seguimiento", "002"],
    ]
    assert not any(column.startswith("_") and column != align.GENERAL_ROW_KEY_COLUMN
                   for column in at.session_state["grid_shown"])
    assert {key: sheet.values for key, sheet in at.session_state["sheets"].items()} == before
    at.selectbox(key="general_order_mode").select(SHEET_ORDER).run()
    assert at.session_state["grid_shown"][align.ORIGIN_COLUMN].tolist() == ["📋 Seguimiento", "📋 Seguimiento", "🏢 Polanco"]
    align.read_order_values.clear()
    align.read_times_values.clear()


@pytest.mark.parametrize("sheet_name", [lab.SHEET_ESTATUS, *align.ORDER_TIME_SHEETS])
def test_individual_views_upgrade_cached_sessions_without_editing_any_case(sheet_name):
    root = str(Path(__file__).resolve().parents[1])
    script = f'''
import sys
sys.path.insert(0, {root!r})
sys.path.insert(0, {str(Path(__file__).parent)!r})
import pandas as pd
import streamlit as st
from unittest.mock import patch
from test_aeris import definitions
from order_activity import HISTORY_ACTIVITY_COLUMN
import lab_pg as lab
import alineadores_pg as align

sheet = {sheet_name!r}
app = lab if sheet == lab.SHEET_ESTATUS else align
orders = pd.DataFrame([
    {{app.ID_COLUMN: "001", "STATUS": "REVISIÓN DE ARCHIVOS", "APARATO": "MSE", "PRODUCTO": "Graphy",
      "NOMBRE DOCTOR": "Doctor", "NOMBRE PACIENTE": "Sin historial", "FECHA DE RECEPCIÓN": "2026-10-26"}},
    {{app.ID_COLUMN: "002", "STATUS": "REVISIÓN DE ARCHIVOS", "APARATO": "MSE", "PRODUCTO": "Graphy",
      "NOMBRE DOCTOR": "Doctor", "NOMBRE PACIENTE": "Actualizado", "FECHA DE RECEPCIÓN": "2026-10-01"}},
])
times = pd.DataFrame([{{app.ID_COLUMN: "002", "STATUS": "REVISIÓN DE ARCHIVOS", "FECHA_FIN": "",
    "FECHA_INICIO": "2026-10-25", "HORA_INICIO": "12:00:00", "FECHA_REGISTRO_LOG": "2026-10-25 12:00:00"}}])

def fake_grid(grid, options, key):
    st.session_state["shown"] = grid
    return (grid, []) if app is lab else grid

if app is lab:
    if "workbench_snapshot" not in st.session_state:
        st.session_state["workbench_snapshot"] = {{"user": "Jime", "times": times, "at": lab.app_now(),
            "table": lab.build_workbench_table(orders, times).drop(columns=HISTORY_ACTIVITY_COLUMN),
            "paused_table": orders.iloc[:0], "sent_table": orders.iloc[:0],
            "sent_statuses": lab.WORKBENCH_SENT_STATUSES}}
        st.session_state["workbench_order"] = "Orden de la hoja"
    with patch.object(lab, "get_workbench_form_catalog", lambda: {{}}), \\
         patch.object(lab, "workbench_saved_column_order", lambda *args: []), \\
         patch.object(lab, "render_grid", fake_grid):
        lab.render_workbench("Jime")
else:
    st.session_state["aligners_order_sheet"] = sheet
    if align.tracking_key("aligners_snapshot") not in st.session_state:
        st.session_state[align.tracking_key("aligners_snapshot")] = {{"user": "Jime", "definitions": definitions(),
            "orders": orders, "times": times, "table": align.build_tracking_table(orders, times, definitions())
                .drop(columns=HISTORY_ACTIVITY_COLUMN), "sent": orders.iloc[:0], "paused": orders.iloc[:0], "at": align.app_now()}}
        st.session_state[align.tracking_key("aligners_order_mode")] = "Orden de la hoja"
    with patch.object(align, "tracking_form_catalog", lambda: {{}}), \\
         patch.object(align, "render_aligners_grid", fake_grid):
        align.render_workbench("Jime")
'''
    at = AppTest.from_string(script, default_timeout=15).run()
    assert not at.exception
    id_column = lab.ID_COLUMN if sheet_name == lab.SHEET_ESTATUS else align.ID_COLUMN
    assert at.session_state["shown"][id_column].tolist() == ["002", "001"]
    assert HISTORY_ACTIVITY_COLUMN not in at.session_state["shown"]
    at.run()
    assert not at.exception
    assert at.session_state["shown"][id_column].tolist() == ["002", "001"]
