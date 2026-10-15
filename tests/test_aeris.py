"""Flujos AERIS y Polanco independientes, con Sheets simulados y sin red."""
import copy
import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock

import gspread
import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import alineadores_pg as app
from test_polanco import Sheet

AERIS_HEADERS = [app.ID_COLUMN, app.STATUS_COLUMN, app.PRODUCT_COLUMN,
    "NOMBRE DOCTOR", "NOMBRE PACIENTE", "DETALLE COMENTARIOS", "VENDEDOR",
    "SERVICIO", "PAQUETE MARCA BLANCA", "ARCHIVOS RECIBIDOS", "FECHA DE RECEPCIÓN"]
AERIS_CATALOG = {"PRODUCTO": ["AERIS"], "VENDEDOR": ["JIMENA"],
    "SERVICIO": ["CONFECCIÓN"], "PAQUETE MARCA BLANCA": ["NO APLICA"],
    "ARCHIVOS RECIBIDOS": ["STLs"]}
PROCESS_VALUES = [
    ["AERIS", "", "Graphy", ""], ["Fases", "Tiempo", "Fases", "Tiempo"],
    ["LISTO STLs PARA IMPRESIÓN", "<2 dias", "REVISIÓN DE ARCHIVOS", "<1 dia"],
    ["EN IMPRESIÓN Y CONFECCIÓN", "<4 dias", "ENVIADO", ""],
    ["LISTO P/ENVÍO", "<2 dias"], ["ENVIADO", ""],
    ["IMPRESIÓN EN PAUSA", ""], ["CANCELADO", ""],
]


def definitions():
    return app.parse_process_matrix(PROCESS_VALUES)


@pytest.fixture
def sheets(monkeypatch):
    data = {}
    for name in app.ORDER_TIME_SHEETS:
        row = ["001", "LISTO STLs PARA IMPRESIÓN", "AERIS", "Doctor", "Paciente",
               name, "", "", "", "", "2026-10-14"]
        data[name] = Sheet([[], AERIS_HEADERS, row])
    for name in app.ORDER_TIME_SHEETS.values():
        data[name] = Sheet([app.TIMES_HEADERS])
    monkeypatch.setattr(app.st, "session_state", {})
    monkeypatch.setattr(app, "get_worksheet", data.__getitem__)
    monkeypatch.setattr(app, "read_process_definitions", definitions)
    monkeypatch.setattr(app, "configured_spreadsheet_id", lambda: "fake")
    app.read_order_values.clear()
    app.read_times_values.clear()
    app._ensure_times_headers.clear()
    yield data
    app.read_order_values.clear()
    app.read_times_values.clear()
    app._ensure_times_headers.clear()


def new_values(patient="Paciente nuevo", product="AERIS"):
    return {app.PRODUCT_COLUMN: product, "NOMBRE DOCTOR": "Doctor nuevo",
            "NOMBRE PACIENTE": patient}


@pytest.mark.parametrize("user", ["Jime", "Juan", "Paula"])
def test_aeris_and_polanco_create_separate_orders_and_timers(sheets, user):
    original = copy.deepcopy(sheets[app.SHEET_ORDERS].values)
    for source in (app.SHEET_AERIS, app.SHEET_POLANCO):
        app.st.session_state["aligners_order_sheet"] = source
        identifier, warning = app.create_order(new_values(source), user, definitions())
        assert not warning
        assert identifier == f"{app.app_today():%d%m%Y}-001"
        assert sheets[source].values[-1][AERIS_HEADERS.index("NOMBRE PACIENTE")] == source
        assert sheets[source].values[-1][1] == "LISTO STLs PARA IMPRESIÓN"
        times = sheets[app.ORDER_TIME_SHEETS[source]].values
        assert len(times) == 2
        log = dict(zip(app.TIMES_HEADERS, times[-1]))
        assert log[app.ID_COLUMN] == identifier
        assert log["USUARIO"] == user
        assert log["TIEMPO_MAXIMO_HORAS"] == "48"
    assert sheets[app.SHEET_ORDERS].values == original
    assert len(sheets[app.SHEET_TIMES].values) == 1


def test_aeris_edit_and_status_change_touch_only_aeris(sheets):
    before = {name: copy.deepcopy(sheet.values) for name, sheet in sheets.items()}
    app.st.session_state["aligners_order_sheet"] = app.SHEET_AERIS
    source = app.read_orders_df()
    edited = source.copy()
    edited.loc[0, app.STATUS_COLUMN] = "EN IMPRESIÓN Y CONFECCIÓN"
    edited.loc[0, "DETALLE COMENTARIOS"] = "Sólo Aeris"
    saved, errors = app.save_workbench_changes(source, source, edited, "Jime", definitions())
    assert (saved, errors) == (["001"], [])
    assert sheets[app.SHEET_AERIS].values[2][1] == "EN IMPRESIÓN Y CONFECCIÓN"
    assert sheets[app.SHEET_AERIS].values[2][5] == "Sólo Aeris"
    assert len(sheets[app.SHEET_AERIS_TIMES].values) == 2
    for name in (app.SHEET_ORDERS, app.SHEET_POLANCO, app.SHEET_TIMES, app.SHEET_POLANCO_TIMES):
        assert sheets[name].values == before[name]


def test_aeris_rejects_other_products_while_polanco_keeps_them(sheets):
    app.st.session_state["aligners_order_sheet"] = app.SHEET_AERIS
    with pytest.raises(ValueError, match="sólo admite"):
        app.create_order(new_values(product="Graphy"), "Jime", definitions())
    source = app.read_orders_df()
    edited = source.copy()
    edited.loc[0, app.PRODUCT_COLUMN] = "Graphy"
    saved, errors = app.save_workbench_changes(source, source, edited, "Jime", definitions())
    assert not saved and any("sólo admite" in error for error in errors)
    assert not sheets[app.SHEET_AERIS].batches
    app.st.session_state["aligners_order_sheet"] = app.SHEET_POLANCO
    _, warning = app.create_order(new_values(product="Graphy"), "Jime", definitions())
    assert not warning
    assert sheets[app.SHEET_POLANCO].values[-1][2] == "Graphy"


def test_aeris_without_native_lists_accepts_optional_text_and_still_checks_configured_lists(sheets):
    app.st.session_state["aligners_order_sheet"] = app.SHEET_AERIS
    catalog = {"PRODUCTO": ["AERIS"]}
    _, warning = app.create_order({**new_values(), "SERVICIO": "CONFECCIÓN", "ARCHIVOS RECIBIDOS": "STLs"},
                                  "Jime", definitions(), catalog=catalog)
    assert not warning
    assert sheets[app.SHEET_AERIS].values[-1][AERIS_HEADERS.index("SERVICIO")] == "CONFECCIÓN"
    before = len(sheets[app.SHEET_AERIS].values)
    with pytest.raises(ValueError, match="catálogo"):
        app.create_order({**new_values(), "SERVICIO": "No permitido"},
                         "Jime", definitions(), catalog=AERIS_CATALOG)
    assert len(sheets[app.SHEET_AERIS].values) == before


def test_explicit_aeris_times_read_resolves_legacy_id_against_aeris_only(sheets):
    sheets[app.SHEET_AERIS].values[2][0] = "0001"
    log = {app.ID_COLUMN: "1", app.STATUS_COLUMN: "LISTO STLs PARA IMPRESIÓN",
           "FECHA_INICIO": "2026-10-14", "HORA_INICIO": "10:00:00"}
    sheets[app.SHEET_AERIS_TIMES].values.append([log.get(header, "") for header in app.TIMES_HEADERS])
    app.st.session_state["aligners_order_sheet"] = app.SHEET_POLANCO
    frame = app.read_times_df(app.SHEET_AERIS_TIMES)
    assert frame.iloc[0][app.ID_COLUMN] == "0001"
    assert app.current_order_sheet() == app.SHEET_POLANCO
    app.st.session_state["aligners_order_sheet"] = app.SHEET_AERIS
    app.register_status_change(identifier="0001", product="AERIS",
        previous_status="LISTO STLs PARA IMPRESIÓN", new_status="EN IMPRESIÓN Y CONFECCIÓN",
        current_user="Jime", definitions=definitions())
    assert sheets[app.SHEET_AERIS_TIMES].values[1][app.TIMES_HEADERS.index("FECHA_FIN")]
    assert len(sheets[app.SHEET_POLANCO_TIMES].values) == 1


def test_snapshots_filters_drafts_and_pending_tab_guard_are_isolated(sheets):
    snapshots = {}
    for source in app.ORDER_TIME_SHEETS:
        app.st.session_state["aligners_order_sheet"] = source
        snapshots[source] = app.get_snapshot("Jime")
        assert snapshots[source]["orders"].iloc[0]["DETALLE COMENTARIOS"] == source
        app.st.session_state[app.tracking_key("aligners_signal_filter")] = source
    app.st.session_state.update({"aligners_active_tab": "AERIS",
        "aligners_primary_tabs_Jime": "📋 Seguimiento Polanco",
        "order_detail_drafts_aeris_aligners": {"001": {"changes": {"SERVICIO": "Otro"}}}})
    app.guard_tracking_tab_change("Jime")
    assert app.st.session_state["aligners_primary_tabs_Jime"] == "AERIS"
    assert app.st.session_state["aligners_tab_warning"]
    app.reset_workbench()
    assert "aeris_aligners_snapshot" not in app.st.session_state
    assert app.st.session_state["aligners_snapshot"] is snapshots[app.SHEET_ORDERS]
    assert app.st.session_state["polanco_aligners_snapshot"] is snapshots[app.SHEET_POLANCO]
    assert app.st.session_state["polanco_aligners_signal_filter"] == app.SHEET_POLANCO
    assert app.order_detail.pending_count("polanco_aligners") == 0
    assert app.order_detail.pending_count("aeris_aligners") == 1


def test_native_aeris_catalog_is_kept_and_other_product_options_are_filtered(sheets, monkeypatch):
    metadata = {"sheets": [{"properties": {"title": app.SHEET_AERIS}, "tables": [{
        "range": {"startRowIndex": 1}, "columnProperties": [
            {"columnName": column, "dataValidationRule": {"condition": {
                "type": "ONE_OF_LIST", "values": [{"userEnteredValue": option} for option in options]}}}
            for column, options in {**AERIS_CATALOG, "PRODUCTO": ["Graphy", "AERIS"]}.items()]}]}]}
    catalogs = app.parse_order_form_catalogs(metadata)
    assert catalogs[app.SHEET_AERIS]["PRODUCTO"] == ["Graphy", "AERIS"]
    monkeypatch.setattr(app, "read_order_form_catalogs", lambda _: catalogs)
    app.st.session_state["aligners_order_sheet"] = app.SHEET_AERIS
    result = app.get_order_form_catalog()
    assert result["PRODUCTO"] == ["AERIS"]
    assert result["SERVICIO"] == ["CONFECCIÓN"]
    assert "TIPO" not in result and "ETAPA SOLICITUD" not in result


def test_catalog_fetch_excludes_missing_sheet_ranges(monkeypatch):
    metadata = {"sheets": [{"properties": {"title": name}} for name in (app.SHEET_ORDERS, app.SHEET_POLANCO)]}
    spreadsheet = Mock()
    spreadsheet.fetch_sheet_metadata.side_effect = [metadata, metadata]
    monkeypatch.setattr(app, "get_spreadsheet_by_id", lambda _: spreadsheet)
    app.read_order_form_catalogs.clear()
    result = app.read_order_form_catalogs("fake")
    params = spreadsheet.fetch_sheet_metadata.call_args_list[1].kwargs["params"]
    assert params["ranges"] == [f"'{app.SHEET_ORDERS}'!A2:CB12", "'POLANCO'!A2:CB12"]
    assert app.SHEET_AERIS not in result
    app.read_order_form_catalogs.clear()


def test_missing_timing_sheet_is_created_but_order_sheet_is_not(monkeypatch):
    spreadsheet = Mock()
    spreadsheet.worksheet.side_effect = gspread.WorksheetNotFound("missing")
    monkeypatch.setattr(app, "get_spreadsheet", lambda: spreadsheet)
    app.get_worksheet.clear()
    app.get_worksheet(app.SHEET_AERIS_TIMES)
    assert spreadsheet.add_worksheet.call_args.kwargs["title"] == app.SHEET_AERIS_TIMES
    spreadsheet.add_worksheet.reset_mock()
    with pytest.raises(gspread.WorksheetNotFound):
        app.get_worksheet(app.SHEET_AERIS)
    spreadsheet.add_worksheet.assert_not_called()
    app.get_worksheet.clear()


def test_search_and_invoice_verification_keep_identical_folios_as_separate_cases(sheets, monkeypatch):
    captured = {}
    monkeypatch.setattr(app.case_search, "render", lambda user, **kwargs: captured.update(kwargs))
    app.render_case_search("Jime")
    cases = captured["load_cases"]()
    assert list(cases[app.case_search.SOURCE_COLUMN]) == ["alineadores", "polanco", "aeris"]
    assert list(cases[app.case_search.ID_COLUMN]) == ["001", "001", "001"]
    app.st.session_state["aligners_order_sheet"] = app.SHEET_POLANCO
    assert app.invoice_case_type(app.SHEET_AERIS) == "aeris"
    captured["verify_case"]("aeris", "001")
    sheets[app.SHEET_AERIS].values.append(sheets[app.SHEET_AERIS].values[-1][:])
    with pytest.raises(ValueError, match="duplicado"):
        captured["verify_case"]("aeris", "001")
    captured["verify_case"]("polanco", "001")


def test_optional_missing_aeris_does_not_break_board_or_search(sheets, monkeypatch):
    def get_sheet(name):
        if name == app.SHEET_AERIS:
            raise gspread.WorksheetNotFound("missing")
        return sheets[name]
    monkeypatch.setattr(app, "get_worksheet", get_sheet)
    assert [label for label, _, _ in app.read_board_sources(app.BOARD_ALL_SOURCES)] == ["Alineadores", "Polanco"]
    captured = {}
    monkeypatch.setattr(app.case_search, "render", lambda user, **kwargs: captured.update(kwargs))
    app.render_case_search("Jime")
    assert set(captured["load_cases"]()[app.case_search.SOURCE_COLUMN]) == {"alineadores", "polanco"}


def test_board_counts_aeris_and_polanco_with_same_folio(sheets):
    sources = app.read_board_sources(app.BOARD_ALL_SOURCES)
    now = datetime(2026, 10, 14, 12)
    board = app.build_board_table(sources, definitions(), now=now)
    model = app.board_view_model(board, definitions(), {}, now=now)
    assert model["total"] == 3
    assert model["sources"] == [("Alineadores", 1), ("Polanco", 1), ("AERIS", 1)]
    assert all(card["show_source"] for card in model["priority"] if card["source"] in ("Polanco", "AERIS"))


@pytest.mark.parametrize("catalog", [AERIS_CATALOG, {}])
def test_empty_aeris_tab_has_independent_new_order_without_absent_fields(catalog):
    script = f'''
import sys
sys.path.insert(0, {str(Path(__file__).resolve().parents[1])!r})
from unittest.mock import patch
import streamlit as st
import pandas as pd
import alineadores_pg as app
snapshot = {{'user': 'Jime', 'definitions': app.parse_process_matrix({PROCESS_VALUES!r}),
    'orders': pd.DataFrame(columns={AERIS_HEADERS!r}), 'times': pd.DataFrame(),
    'table': pd.DataFrame(columns=app.COMPUTED_COLUMNS), 'sent': pd.DataFrame(), 'at': app.app_now()}}
def create(values, user, definitions, **kwargs):
    st.session_state['created_in'] = app.current_order_sheet()
    st.session_state['created_values'] = values
    return 'AERIS-DEMO', ''
with patch.object(app, 'ensure_times_headers', lambda: None), \\
     patch.object(app, 'get_snapshot', lambda _: snapshot), \\
     patch.object(app, 'read_order_form_catalogs', lambda _: {{app.SHEET_AERIS: {catalog!r}}}), \\
     patch.object(app, 'configured_spreadsheet_id', lambda: 'fake'), \\
     patch.object(app, 'create_order', create):
    st.session_state['aligners_primary_tabs_Jime'] = st.session_state.get('test_tab', '📋 Seguimiento Polanco')
    if st.session_state.get('test_open'):
        st.session_state['aeris_aligners_new_expander'] = True
    app.render_app_tabs('Jime')
'''
    at = AppTest.from_string(script, default_timeout=15).run()
    assert not at.exception and not at.text_input
    at.session_state["test_tab"] = "AERIS"
    at.run()
    assert not at.exception and not at.text_input
    assert at.session_state["aligners_order_sheet"] == app.SHEET_AERIS
    at.session_state["test_open"] = True
    at.run()
    assert not at.exception
    assert not any("TIPO" in item.label or "ETAPA SOLICITUD" in item.label for item in at.selectbox)
    product = next(item for item in at.selectbox if "PRODUCTO" in item.label)
    assert product.value == "AERIS"
    assert not at.warning
    for item in at.text_input:
        if "NOMBRE DOCTOR" in item.label or "NOMBRE PACIENTE" in item.label:
            item.input("Ejemplo")
    next(button for button in at.button if button.label == "💾 Guardar nueva orden").click().run()
    assert not at.exception
    assert at.session_state["created_in"] == app.SHEET_AERIS
    assert at.session_state["created_values"]["PRODUCTO"] == "AERIS"
    assert "TIPO" not in at.session_state["created_values"]
    assert "ETAPA SOLICITUD" not in at.session_state["created_values"]
    assert at.session_state["aeris_aligners_new_version"] == 1
    assert "polanco_aligners_new_version" not in at.session_state
