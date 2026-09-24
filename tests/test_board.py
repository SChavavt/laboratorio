"""Tablero de pantalla: datos combinados de Alineadores y Polanco, sin Sheets reales."""
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import gspread
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import aligners_board
import alineadores_pg as app
from test_alineadores_pg import PROCESS_VALUES

NOW = datetime(2026, 9, 24, 12)  # jueves


@pytest.fixture
def definitions():
    return app.parse_process_matrix(PROCESS_VALUES)


def order(identifier, status, received="", product="CONVENC.", patient="Paciente ejemplo"):
    return {
        app.ID_COLUMN: identifier,
        app.STATUS_COLUMN: status,
        app.PRODUCT_COLUMN: product,
        "NOMBRE DOCTOR": "Doctora ejemplo",
        "NOMBRE PACIENTE": patient,
        "FECHA DE RECEPCIÓN": received,
    }


def log(identifier, status, start, maximum="24", end=""):
    return {
        app.ID_COLUMN: identifier,
        app.STATUS_COLUMN: status,
        "FECHA_INICIO": f"{start:%Y-%m-%d}",
        "HORA_INICIO": f"{start:%H:%M:%S}",
        "FECHA_FIN": end,
        "TIEMPO_MAXIMO_HORAS": maximum,
    }


def sources():
    main_orders = pd.DataFrame([
        order("A-1", "REVISIÓN DE ARCHIVOS", "2026-09-22"),
        order("A-2", "EN PLANEACIÓN", "01/09/2026"),
        order("A-3", "ENVIADO", "2026-09-01"),
        order("A-4", "SOLICITUD DE CAMBIOS", ""),
    ])
    main_times = pd.DataFrame([
        log("A-1", "REVISIÓN DE ARCHIVOS", NOW - timedelta(hours=30)),
        log("A-2", "EN PLANEACIÓN", NOW - timedelta(hours=10), "96"),
        log("A-3", "ENVIADO", NOW - timedelta(days=1), "", end=f"{NOW - timedelta(days=1):%Y-%m-%d}"),
        log("A-4", "REVISIÓN DE ARCHIVOS", datetime(2026, 9, 14, 9), end="2026-09-15"),
        log("A-4", "SOLICITUD DE CAMBIOS", NOW - timedelta(hours=5), ""),
    ])
    polanco_orders = pd.DataFrame([
        order("A-1", "CASO APROBADO", "2026-09-10", patient="Paciente <Polanco>"),
        order("P-2", "ESTADO RARO", "2026-09-24"),
    ])
    polanco_times = pd.DataFrame(columns=app.TIMES_HEADERS)
    return [("Alineadores", main_orders, main_times), ("Polanco", polanco_orders, polanco_times)]


def test_business_days_skip_weekends():
    friday, monday = date(2026, 9, 18), date(2026, 9, 21)
    assert app.business_days_between(friday, monday) == 1
    assert app.business_days_between(monday, monday) == 0
    assert app.business_days_between(monday, friday) == 0
    assert app.business_days_between(monday, monday + timedelta(days=14)) == 10


def test_board_table_joins_both_sheets_oldest_first(definitions):
    board = app.build_board_table(sources(), definitions, now=NOW)

    # Enviados no aparecen; el mismo folio en otra hoja es otro pedido.
    assert list(zip(board[app.ID_COLUMN], board["HOJA"])) == [
        ("A-2", "Alineadores"),
        ("A-1", "Polanco"),
        ("A-4", "Alineadores"),
        ("A-1", "Alineadores"),
        ("P-2", "Polanco"),
    ]
    ages = dict(zip(board[app.ID_COLUMN] + "/" + board["HOJA"], board["DÍAS ACTIVO"]))
    assert ages["A-2/Alineadores"] == 17
    assert ages["A-1/Polanco"] == 10
    # Sin FECHA DE RECEPCIÓN se usa el primer registro de la bitácora.
    assert ages["A-4/Alineadores"] == 8
    assert ages["A-1/Alineadores"] == 2
    assert ages["P-2/Polanco"] == 0
    first = board.iloc[0]
    assert first["SEMÁFORO"] == "🟢 En tiempo"
    assert first["INICIO PEDIDO"] == date(2026, 9, 1)


def test_undated_orders_rank_after_dated_ones(definitions):
    orders = pd.DataFrame([order("X-1", "EN PLANEACIÓN", ""), order("X-2", "EN PLANEACIÓN", "2026-09-23")])
    board = app.build_board_table([("Alineadores", orders, pd.DataFrame())], definitions, now=NOW)
    assert list(board[app.ID_COLUMN]) == ["X-2", "X-1"]
    assert pd.isna(board.iloc[1]["DÍAS ACTIVO"])


def test_empty_sources_give_empty_board(definitions):
    board = app.build_board_table([("Polanco", pd.DataFrame(), pd.DataFrame())], definitions, now=NOW)
    assert board.empty
    assert {"HOJA", "DÍAS ACTIVO", "SEMÁFORO"}.issubset(board.columns)


def test_stage_order_merges_every_product_flow(definitions):
    normal, pauses = app.board_stage_order(definitions)
    assert normal == [
        "REVISIÓN DE ARCHIVOS", "POR HACER SETUP", "EN PLANEACIÓN", "REVISIÓN DEL SETUP POR DR",
        "CASO APROBADO", "LISTO STL PARA IMPRESIÓN", "EN IMPRESIÓN", "LISTO P/ENVÍO",
    ]
    assert pauses == ["BORRADOR TITAN", "SOLICITUD DE CAMBIOS", "IMPRESIÓN EN PAUSA"]


def test_week_activity_counts_receptions_and_logged_sends(definitions):
    activity = app.board_week_activity(sources(), definitions, today=NOW.date())
    # Semana del lunes 21: A-1 (22), P-2 (24, hoy); los enviados salen de la bitácora.
    assert activity == {"received": 2, "received_today": 1, "sent": 1, "sent_today": 0}


def test_view_model_groups_stages_and_ranks_priority(definitions):
    board = app.build_board_table(sources(), definitions, now=NOW)
    activity = app.board_week_activity(sources(), definitions, today=NOW.date())
    model = app.board_view_model(board, definitions, activity, now=NOW, priority_rows=2)

    assert model["total"] == 5
    assert model["sources"] == [("Alineadores", 3), ("Polanco", 2)]
    assert model["clock"] == "12:00"
    assert model["date_label"] == "jueves 24 de septiembre"
    assert [(card["folio"], card["rank"]) for card in model["priority"]] == [("A-2", 1), ("A-1", 2)]
    assert model["oldest"] == {"days": 17, "folio": "A-2", "patient": "Paciente ejemplo"}
    counts = {item["key"]: item["count"] for item in model["signals"]}
    assert counts == {"late": 1, "due": 0, "pause": 1, "ok": 1, "none": 2}

    normal, pauses, *others = model["groups"]
    stages = {stage["name"]: stage for stage in normal["stages"]}
    assert stages["REVISIÓN DE ARCHIVOS"]["signals"]["late"] == 1
    assert stages["REVISIÓN DE ARCHIVOS"]["cards"][0]["stage_time_label"] == "1.2 d de 1 d"
    polanco_card = stages["CASO APROBADO"]["cards"][0]
    assert polanco_card["show_source"] and polanco_card["source"] == "Polanco"
    assert [stage["count"] for stage in pauses["stages"]] == [0, 1, 0]
    assert pauses["stages"][1]["cards"][0]["stage_time_label"] == "5 h en pausa"
    # Una etapa que no existe en PROCESOS POR PRODUCTO no se pierde.
    assert others[0]["stages"][0]["cards"][0]["stage_time_label"] == "ESTADO RARO"
    assert [item["count"] for item in model["ages"]] == [2, 0, 2, 1, 0]


def test_single_sheet_filter_hides_source_chip(definitions):
    board = app.build_board_table(sources()[1:], definitions, now=NOW)
    model = app.board_view_model(board, definitions, {"received": 0, "received_today": 0,
                                 "sent": 0, "sent_today": 0}, now=NOW, source_filter="Polanco")
    assert model["sources"] == [("Polanco", 2)]
    assert model["subtitle"].startswith("Polanco ·")
    assert not any(card["show_source"] for card in model["priority"])


def test_cards_per_stage_limits_cards_and_counts_the_rest(definitions):
    orders = pd.DataFrame([order(f"X-{n}", "EN PLANEACIÓN", "2026-09-01") for n in range(7)])
    board = app.build_board_table([("Alineadores", orders, pd.DataFrame())], definitions, now=NOW)
    model = app.board_view_model(board, definitions, {"received": 0, "received_today": 0,
                                 "sent": 0, "sent_today": 0}, now=NOW, cards_per_stage=3)
    stage = next(s for s in model["groups"][0]["stages"] if s["name"] == "EN PLANEACIÓN")
    assert (stage["count"], len(stage["cards"]), stage["more"]) == (7, 3, 4)


def test_board_html_escapes_sheet_text(definitions):
    board = app.build_board_table(sources(), definitions, now=NOW)
    model = app.board_view_model(board, definitions, {"received": 0, "received_today": 0,
                                 "sent": 0, "sent_today": 0}, now=NOW, screen=True)
    html = aligners_board.render_board_html(model)
    assert "Paciente <Polanco>" not in html
    assert "Paciente &lt;Polanco&gt;" in html
    assert 'class="board is-screen"' in html
    assert ">POL</span>" in html
    assert "#1</span>" in html


def test_board_reads_both_sheets_without_switching_tracking_sheet(monkeypatch):
    calls = []
    monkeypatch.setattr(app.st, "session_state", {"aligners_order_sheet": app.SHEET_POLANCO})
    monkeypatch.setattr(app, "read_order_values", lambda name: calls.append(name) or [])
    monkeypatch.setattr(app, "read_times_values", lambda name: calls.append(name) or [])
    loaded = app.read_board_sources(app.BOARD_ALL_SOURCES)
    assert [label for label, _, _ in loaded] == ["Alineadores", "Polanco"]
    assert calls == [app.SHEET_ORDERS, app.SHEET_TIMES, app.SHEET_POLANCO, app.SHEET_POLANCO_TIMES]
    assert app.st.session_state == {"aligners_order_sheet": app.SHEET_POLANCO}
    calls.clear()
    app.read_board_sources("Polanco")
    assert calls == [app.SHEET_POLANCO, app.SHEET_POLANCO_TIMES]


BOARD_SCRIPT = '''
import sys
sys.path.insert(0, {root!r})
sys.path.insert(0, {tests!r})
import gspread
import pandas as pd
import streamlit as st
import alineadores_pg as app
from unittest.mock import patch
from test_board import NOW, sources
from test_alineadores_pg import PROCESS_VALUES

def read_sources(source_filter):
    if st.session_state.get("fail"):
        raise st.session_state["fail"]
    return [item for item in sources() if source_filter in (app.BOARD_ALL_SOURCES, item[0])]

with patch.object(app, "ensure_times_headers", lambda: None), \\
     patch.object(app, "render_workbench", lambda _: st.write("seguimiento ejecutado")), \\
     patch.object(app, "read_process_definitions", lambda: app.parse_process_matrix(PROCESS_VALUES)), \\
     patch.object(app, "read_board_sources", read_sources), \\
     patch.object(app, "app_now", lambda: NOW):
    app.render_app_tabs("Jime")
'''


def board_app():
    from streamlit.testing.v1 import AppTest
    tests = Path(__file__).resolve().parent
    script = BOARD_SCRIPT.format(root=str(tests.parent), tests=str(tests))
    return AppTest.from_string(script, default_timeout=20)


def board_html(at):
    return "".join(element.proto.body for element in at.get("html"))


def test_screen_link_opens_board_in_screen_mode():
    at = board_app()
    at.query_params[app.BOARD_SCREEN_PARAM] = "1"
    at.run()
    assert not at.exception
    assert at.session_state["aligners_primary_tabs_Jime"] == app.BOARD_TAB_LABEL
    assert at.session_state[app.BOARD_SCREEN_KEY] is True
    html = board_html(at)
    assert 'class="board is-screen"' in html
    assert ">5</strong>" in html

    at.toggle(key=app.BOARD_SCREEN_KEY).set_value(False).run()
    assert not at.exception
    assert 'class="board"' in board_html(at)
    assert app.BOARD_SCREEN_PARAM not in at.query_params


def test_board_keeps_last_reading_when_sheets_fail():
    at = board_app()
    at.session_state["aligners_primary_tabs_Jime"] = app.BOARD_TAB_LABEL
    at.run()
    assert not at.exception
    assert "⚠️" not in board_html(at)

    response = type("Response", (), {"status_code": 429, "json": lambda self: {"error": {"code": 429, "message": "RATE_LIMIT_EXCEEDED", "status": "RESOURCE_EXHAUSTED"}}, "text": "RATE_LIMIT_EXCEEDED"})()
    at.session_state["fail"] = gspread.exceptions.APIError(response)
    at.run()
    assert not at.exception
    html = board_html(at)
    assert "límite temporal de lecturas" in html
    assert "se muestra la lectura de las 12:00:00" in html


def test_default_tab_is_still_tracking():
    at = board_app().run()
    assert not at.exception
    assert at.session_state["aligners_primary_tabs_Jime"] == "📋 Seguimiento"
    assert not board_html(at)
