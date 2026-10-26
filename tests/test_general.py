"""Seguimiento General: las tres hojas en una tabla, cada orden guardada en su hoja."""
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import alineadores_pg as app

ROOT = Path(__file__).resolve().parents[1]
TESTS = Path(__file__).resolve().parent

SCRIPT = '''
import sys
sys.path.insert(0, {root!r})
sys.path.insert(0, {tests!r})
import streamlit as st
import alineadores_pg as app
from unittest.mock import patch
from test_polanco import Sheet, FORM_CATALOG, definitions

if "sheets" not in st.session_state:
    app.read_order_values.clear()
    app.read_times_values.clear()
    headers = [app.ID_COLUMN, app.STATUS_COLUMN, app.PRODUCT_COLUMN, "NOMBRE DOCTOR",
               "NOMBRE PACIENTE", "FECHA DE RECEPCIÓN"]
    # El mismo folio existe en Seguimiento y en Polanco.
    st.session_state["sheets"] = {{
        app.SHEET_ORDERS: Sheet([[], headers,
            ["001", "REVISIÓN DE ARCHIVOS", "Graphy", "Dra. Normal", "Ana", "2026-10-01"]]),
        app.SHEET_POLANCO: Sheet([[], headers + ["ADEUDO"],
            ["001", "REVISIÓN DE ARCHIVOS", "Graphy", "Dr. Polanco", "Beto", "2026-10-02", "500"]]),
        # AERIS tiene una columna propia y ningún pedido activo.
        app.SHEET_AERIS: Sheet([[], headers + ["SOLO AERIS"]]),
        **{{name: Sheet([app.TIMES_HEADERS]) for name in
            (app.SHEET_TIMES, app.SHEET_POLANCO_TIMES, app.SHEET_AERIS_TIMES)}},
    }}
sheets = st.session_state["sheets"]


def fake_grid(grid, options, key, id_column="No. Orden"):
    """La tabla AG Grid no corre en AppTest: se guardan sus datos y se simulan ediciones."""
    st.session_state["grid_shown"] = grid
    st.session_state["grid_options"] = options
    edited = grid.copy()
    edits = st.session_state.get("grid_edits", {{}})
    # Como el AG Grid real: las ediciones viven en esa tabla; al guardar o descartar
    # la tabla cambia de key y empieza limpia.
    if edits and st.session_state.setdefault("grid_edits_key", key) != key:
        edits = {{}}
    for row_key, values in edits.items():
        for column, value in values.items():
            edited.loc[edited[id_column] == row_key, column] = value
    if edits:
        st.session_state[key] = {{
            "editedRows": edited[edited[id_column].isin(edits)].to_dict("records"),
            "selectedIds": edited.loc[edited["SELECCIONAR"].eq(True), id_column].tolist(),
        }}
    edited.attrs["activeId"] = None
    edited.attrs["manualStageTargets"] = {{}}
    return edited


with patch.object(app, "get_worksheet", sheets.__getitem__), \\
     patch.object(app, "ensure_times_headers", lambda: None), \\
     patch.object(app, "read_process_definitions", definitions), \\
     patch.object(app, "get_order_form_catalog", lambda: FORM_CATALOG), \\
     patch.object(app, "render_aligners_grid", fake_grid):
    app.render_general_workbench(st.session_state.get("user", "Jime"))
'''


def general_app():
    from streamlit.testing.v1 import AppTest

    return AppTest.from_string(SCRIPT.format(root=str(ROOT), tests=str(TESTS)), default_timeout=20)


def test_merge_column_orders_keeps_each_sheet_order():
    assert app.merge_column_orders([
        ["No. Orden", "STATUS", "PRODUCTO", "SEMÁFORO"],
        ["No. Orden", "STATUS", "ADEUDO", "SEMÁFORO"],
        ["No. Orden", "PRODUCTO", "EXTRA AERIS"],
    ]) == ["No. Orden", "STATUS", "ADEUDO", "PRODUCTO", "EXTRA AERIS", "SEMÁFORO"]


def test_general_table_tells_origin_even_with_repeated_folios():
    sources = [
        {"origin": "📋 Seguimiento", "snapshot": {"table": pd.DataFrame(
            [{app.ID_COLUMN: "001", "TIPO": "ARTTDLAB"}])}},
        {"origin": "🏢 Polanco", "snapshot": {"table": pd.DataFrame(
            [{app.ID_COLUMN: "001", "ADEUDO": "500"}])}},
        {"origin": "🌬️ AERIS", "snapshot": {"table": pd.DataFrame()}},
    ]
    table = app.build_general_table(sources)
    assert table[[app.ORIGIN_COLUMN, app.ID_COLUMN, "TIPO", "ADEUDO"]].values.tolist() == [
        ["📋 Seguimiento", "001", "ARTTDLAB", ""],
        ["🏢 Polanco", "001", "", "500"],
    ]
    assert table[app.GENERAL_ROW_KEY_COLUMN].is_unique


def test_general_view_lists_every_sheet_and_opens_polanco_card():
    at = general_app().run()
    assert not at.exception
    shown = at.session_state["grid_shown"]
    assert shown[[app.ORIGIN_COLUMN, app.ID_COLUMN, "NOMBRE PACIENTE"]].values.tolist() == [
        ["🏢 Polanco", "001", "Beto"],
        ["📋 Seguimiento", "001", "Ana"],
    ]
    assert "ADEUDO" in shown.columns
    assert not at.warning

    # Abrir la orden de Polanco muestra su ficha, aunque Seguimiento tenga el mismo folio.
    at.session_state["general_selected"] = ["🏢 Polanco|001"]
    at.run()
    assert not at.exception
    origins = {f"#### {origin}" for origin, _ in app.GENERAL_ORIGINS}
    assert [item.value for item in at.markdown if item.value in origins] == ["#### 🏢 Polanco"]
    assert any(item.value == "Beto" for item in at.text_input)
    assert not any(item.value == "Ana" for item in at.text_input)

    # Filtrar por origen deja sólo esa hoja.
    at.selectbox(key="general_origin_filter").select("📋 Seguimiento").run()
    assert not at.exception
    assert at.session_state["grid_shown"][app.ORIGIN_COLUMN].tolist() == ["📋 Seguimiento"]


def test_general_new_order_goes_only_to_chosen_origin():
    at = general_app()
    at.session_state["general_new_expander"] = True
    at.run()
    assert not at.exception
    # Sin elegir origen no hay formulario: nada se guarda por accidente en otra hoja.
    assert not at.text_input or not any("NOMBRE PACIENTE" in item.label for item in at.text_input)
    at.session_state["general_new_origin"] = "🏢 Polanco"
    at.run()
    assert not at.exception
    assert any(item.label == "💰 ADEUDO" for item in at.number_input)
    product = next(item for item in at.selectbox if "PRODUCTO" in item.label)
    product.select("GRAPHY")
    for item in at.text_input:
        if "NOMBRE DOCTOR" in item.label or "NOMBRE PACIENTE" in item.label:
            item.input("Nuevo")
    next(button for button in at.button if button.label == "💾 Guardar nueva orden").click().run()
    assert not at.exception
    sheets = at.session_state["sheets"]
    assert [row[4] for row in sheets[app.SHEET_POLANCO].values[2:]] == ["Beto", "Nuevo"]
    assert len(sheets[app.SHEET_ORDERS].values) == 3
    assert len(sheets[app.SHEET_AERIS].values) == 2
    assert len(sheets[app.SHEET_POLANCO_TIMES].values) == 2
    assert len(sheets[app.SHEET_TIMES].values) == 1
    # La tabla ya muestra la orden nueva con su origen.
    assert at.session_state["grid_shown"][app.ORIGIN_COLUMN].tolist() == ["🏢 Polanco", "🏢 Polanco", "📋 Seguimiento"]
    assert at.session_state["grid_shown"].iloc[0]["NOMBRE PACIENTE"] == "Nuevo"


def test_general_card_saves_change_in_its_own_sheet():
    at = general_app().run()
    at.session_state["general_selected"] = ["🏢 Polanco|001"]
    at.run()
    assert not at.exception
    patient = next(item for item in at.text_input if item.value == "Beto")
    patient.input("Beto Corregido").run()
    assert not at.exception
    save = next(button for button in at.button if "Guardar" in button.label and not button.disabled)
    save.click().run()
    assert not at.exception
    sheets = at.session_state["sheets"]
    assert sheets[app.SHEET_POLANCO].values[2][4] == "Beto Corregido"
    assert sheets[app.SHEET_ORDERS].values[2][4] == "Ana"


def test_hide_toggle_remembers_choice_in_session_and_link():
    at = general_app().run()
    assert not at.exception
    at.toggle(key=app.GENERAL_HIDE_TOGGLE_KEY).set_value(True).run()
    assert not at.exception
    assert at.session_state[app.GENERAL_HIDE_STATE_KEY] is True
    assert at.query_params[app.GENERAL_HIDE_PARAM] == [app.GENERAL_HIDE_PARAM_VALUE]
    at.toggle(key=app.GENERAL_HIDE_TOGGLE_KEY).set_value(False).run()
    assert at.session_state[app.GENERAL_HIDE_STATE_KEY] is False
    assert app.GENERAL_HIDE_PARAM not in at.query_params


def test_general_grid_pins_origin_and_limits_edits_to_each_sheet_columns():
    at = general_app().run()
    assert not at.exception
    options = at.session_state["grid_options"]
    pinned = [column["field"] for column in options["columnDefs"] if column.get("pinned") == "left"]
    assert pinned[:4] == ["SELECCIONAR", app.ORIGIN_COLUMN, app.ID_COLUMN, "SEMÁFORO"]
    hidden = {column["field"] for column in options["columnDefs"] if column.get("hide")}
    assert app.GENERAL_ROW_KEY_COLUMN in hidden
    context = options["context"]
    assert context["idColumn"] == app.GENERAL_ROW_KEY_COLUMN
    assert "ADEUDO" in context["rowEditable"]["🏢 Polanco|001"]
    assert "ADEUDO" not in context["rowEditable"]["📋 Seguimiento|001"]
    assert app.ORIGIN_COLUMN not in context["rowEditable"]["🏢 Polanco|001"]
    # Cada fila ofrece las etapas de su propio pedido.
    assert set(context["stageOptions"]) == {"📋 Seguimiento|001", "🏢 Polanco|001"}


def test_general_grid_saves_each_edit_in_its_own_sheet():
    at = general_app()
    at.session_state["grid_edits"] = {
        "🏢 Polanco|001": {"NOMBRE PACIENTE": "Beto Corregido",
                           app.STATUS_COLUMN: app.status_display_value("ENVIADO")},
        "📋 Seguimiento|001": {"NOMBRE DOCTOR": "Dra. Normal Corregida"},
    }
    at.run()
    assert not at.exception
    assert any("2 pedidos con cambios pendientes" in item.value for item in at.caption)
    at.button(key="general_aligners_save").click().run()
    assert not at.exception
    assert not at.error
    sheets = at.session_state["sheets"]
    assert sheets[app.SHEET_POLANCO].values[2][1:5] == [
        "ENVIADO", "Graphy", "Dr. Polanco", "Beto Corregido"]
    assert sheets[app.SHEET_ORDERS].values[2][1:5] == [
        "REVISIÓN DE ARCHIVOS", "Graphy", "Dra. Normal Corregida", "Ana"]
    # El cambio de etapa queda en la bitácora de Polanco y en ninguna otra.
    assert len(sheets[app.SHEET_POLANCO_TIMES].values) == 2
    assert len(sheets[app.SHEET_TIMES].values) == 1
    # El pedido enviado sale de la tabla de activos.
    assert at.session_state["grid_shown"][app.ORIGIN_COLUMN].tolist() == ["📋 Seguimiento"]


def test_pending_general_edits_lock_filters_and_can_be_discarded():
    at = general_app()
    at.session_state["grid_edits"] = {"🏢 Polanco|001": {"NOMBRE PACIENTE": "Sin guardar"}}
    at.run()
    at.run()
    assert not at.exception
    assert at.selectbox(key="general_origin_filter").disabled
    assert at.button(key="general_refresh").disabled
    at.button(key="general_aligners_discard").click().run()
    assert not at.exception
    assert not at.selectbox(key="general_origin_filter").disabled
    assert at.session_state["sheets"][app.SHEET_POLANCO].values[2][4] == "Beto"
