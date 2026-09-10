"""Pruebas puras de la app de alineadores; no acceden a Sheets."""

from datetime import datetime, timedelta
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import alineadores_pg as app


NOW = datetime(2026, 9, 9, 12)


PROCESS_VALUES = [
    [
        "Convencional", "", "Graphy", "", "Retenedores", "", "Modelos", "",
        "Setup", "", "Guarda", "", "Guía", "",
    ],
    ["Fases", "Tiempo"] * 7,
    ["REVISIÓN DE ARCHIVOS", "<1 dia"] * 7,
    [
        "POR HACER SETUP", "<1 dia", "POR HACER SETUP", "<1 dia",
        "CASO APROBADO", "<1 dia", "CASO APROBADO", "<1 dia",
        "POR HACER SETUP", "<1 dia", "POR HACER SETUP", "<1 dia",
        "POR HACER SETUP", "<1 dia",
    ],
    [
        "BORRADOR TITAN", "", "BORRADOR TITAN", "",
        "LISTO STL PARA IMPRESIÓN", "<2 dias", "LISTO STL PARA IMPRESIÓN", "<2 dias",
        "BORRADOR TITAN", "", "EN PLANEACIÓN", "<4 dias", "EN PLANEACIÓN", "<4 dias",
    ],
    [
        "EN PLANEACIÓN", "<4 dias", "EN PLANEACIÓN", "<4 dias",
        "EN IMPRESIÓN", "<4 dias", "EN IMPRESIÓN", "<4 dias",
        "EN PLANEACIÓN", "<4 dias", "REVISIÓN DEL SETUP POR DR", "<15 dias",
        "REVISIÓN DEL SETUP POR DR", "<15 dias",
    ],
    [
        "REVISIÓN DEL SETUP POR DR", "<15 dias", "REVISIÓN DEL SETUP POR DR", "<15 dias",
        "LISTO P/ENVÍO", "<2 dias", "LISTO P/ENVÍO", "<2 dias",
        "REVISIÓN DEL SETUP POR DR", "<15 dias", "SOLICITUD DE CAMBIOS", "<2 dias",
        "SOLICITUD DE CAMBIOS", "<2 dias",
    ],
    [
        "SOLICITUD DE CAMBIOS", "<2 dias", "SOLICITUD DE CAMBIOS", "<2 dias",
        "ENVIADO", "", "ENVIADO", "", "SOLICITUD DE CAMBIOS", "<2 dias",
        "CASO APROBADO", "<1 dia", "CASO APROBADO", "<1 dia",
    ],
    [
        "CASO APROBADO", "<1 dia", "CASO APROBADO", "<1 dia",
        "IMPRESIÓN EN PAUSA", "", "IMPRESIÓN EN PAUSA", "",
        "CASO APROBADO", "<1 dia", "LISTO STL PARA IMPRESIÓN", "<2 dias",
        "LISTO STL PARA IMPRESIÓN", "<2 dias",
    ],
    [
        "LISTO STL PARA IMPRESIÓN", "<2 dias", "LISTO STL PARA IMPRESIÓN", "<2 dias",
        "CANCELADO", "", "CANCELADO", "", "ENVIADO", "",
        "EN IMPRESIÓN", "<4 dias", "EN IMPRESIÓN", "<4 dias",
    ],
    [
        "EN IMPRESIÓN", "<4 dias", "EN IMPRESIÓN", "<4 dias", "", "", "", "",
        "IMPRESIÓN EN PAUSA", "", "LISTO P/ENVÍO", "<2 dias", "LISTO P/ENVÍO", "<2 dias",
    ],
    [
        "LISTO P/ENVÍO", "<2 dias", "LISTO P/ENVÍO", "<2 dias", "", "", "", "",
        "CANCELADO", "", "ENVIADO", "", "ENVIADO", "",
    ],
    [
        "ENVIADO", "", "ENVIADO", "", "", "", "", "", "", "",
        "IMPRESIÓN EN PAUSA", "", "IMPRESIÓN EN PAUSA", "",
    ],
    [
        "IMPRESIÓN EN PAUSA", "", "IMPRESIÓN EN PAUSA", "", "", "", "", "", "", "",
        "CANCELADO", "", "CANCELADO", "",
    ],
    ["CANCELADO", "", "CANCELADO", ""],
]


@pytest.fixture
def definitions():
    return app.parse_process_matrix(PROCESS_VALUES)


def order(identifier="001", status="REVISIÓN DE ARCHIVOS", product="CONVENC.", **extra):
    return {
        app.ID_COLUMN: identifier,
        app.STATUS_COLUMN: status,
        app.PRODUCT_COLUMN: product,
        "NOMBRE DOCTOR": "Doctora ejemplo",
        "NOMBRE PACIENTE": "Paciente ejemplo",
        **extra,
    }


def log(identifier="001", status="REVISIÓN DE ARCHIVOS", hours=2, maximum="24", **extra):
    start = NOW - timedelta(hours=hours)
    return {
        app.ID_COLUMN: identifier,
        app.STATUS_COLUMN: status,
        "FECHA_INICIO": start.strftime("%Y-%m-%d"),
        "HORA_INICIO": start.strftime("%H:%M:%S"),
        "FECHA_FIN": "",
        "TIEMPO_MAXIMO_HORAS": maximum,
        **extra,
    }


def test_process_matrix_separates_optional_pauses_from_normal_flow(definitions):
    conventional = definitions["CONVENCIONAL"]
    assert conventional.normal_statuses == (
        "REVISIÓN DE ARCHIVOS",
        "POR HACER SETUP",
        "EN PLANEACIÓN",
        "REVISIÓN DEL SETUP POR DR",
        "CASO APROBADO",
        "LISTO STL PARA IMPRESIÓN",
        "EN IMPRESIÓN",
        "LISTO P/ENVÍO",
        "ENVIADO",
    )
    assert conventional.pauses == (
        "BORRADOR TITAN",
        "SOLICITUD DE CAMBIOS",
        "IMPRESIÓN EN PAUSA",
    )
    assert "CANCELADO" not in conventional.normal_statuses


def test_product_alias_matches_convencional_abbreviation(definitions):
    assert app.get_process_definition("CONVENC.", definitions).name == "Convencional"
    assert app.get_process_definition("Convencional", definitions).name == "Convencional"


def test_current_drive_matrix_builds_all_seven_product_flows(definitions):
    assert {definition.name for definition in definitions.values()} == {
        "Convencional",
        "Graphy",
        "Retenedores",
        "Modelos",
        "Setup",
        "Guarda",
        "Guía",
    }
    assert definitions["GRAPHY"].normal_statuses == definitions["CONVENCIONAL"].normal_statuses
    assert definitions["MODELOS"].normal_statuses == definitions["RETENEDORES"].normal_statuses
    assert definitions["SETUP"].normal_statuses[-1] == "ENVIADO"
    assert definitions["GUARDA"].normal_statuses[-1] == "ENVIADO"
    assert definitions["GUIA"].normal_statuses[-1] == "ENVIADO"


def test_normal_stage_only_offers_next_stage_pauses_and_cancel(definitions):
    options = app.get_allowed_next_statuses(
        "CONVENC.", "POR HACER SETUP", definitions
    )
    assert options[:2] == ["POR HACER SETUP", "EN PLANEACIÓN"]
    assert options[2:-1] == list(app.PAUSE_STATUSES)
    assert options[-1] == "CANCELADO"
    assert "CASO APROBADO" not in options


def test_pause_can_resume_at_any_real_stage_without_becoming_a_flow_step(definitions):
    options = app.get_allowed_next_statuses(
        "CONVENC.", "SOLICITUD DE CAMBIOS", definitions
    )
    assert options[0] == "SOLICITUD DE CAMBIOS"
    assert "REVISIÓN DE ARCHIVOS" in options
    assert "EN PLANEACIÓN" in options
    assert "CASO APROBADO" in options
    assert options[-1] == "CANCELADO"


def test_pause_options_are_specific_to_each_product(definitions):
    options = app.get_allowed_next_statuses(
        "RETENEDORES", "REVISIÓN DE ARCHIVOS", definitions
    )
    assert "IMPRESIÓN EN PAUSA" in options
    assert "BORRADOR TITAN" not in options
    assert "SOLICITUD DE CAMBIOS" not in options


def test_cancel_alias_and_sent_orders_are_archived(definitions):
    frame = pd.DataFrame(
        [
            order("1", "ENVIADO"),
            order("2", "CANCELO"),
            order("3", "CANCELADO"),
            order("4", "EN PLANEACIÓN"),
        ]
    )
    result = app.active_orders(frame, definitions)
    assert list(result[app.ID_COLUMN]) == ["4"]
    assert app.canonical_status("CANCELO") == "CANCELADO"


@pytest.mark.parametrize(
    "hours,signal",
    [(19.19, "🟢 En tiempo"), (19.2, "🟡 Por vencer"), (23.99, "🟡 Por vencer"), (24, "🔴 Atrasado")],
)
def test_alert_thresholds_use_configured_business_hours(definitions, hours, signal):
    table = app.build_tracking_table(
        pd.DataFrame([order()]),
        pd.DataFrame([log(hours=hours)]),
        definitions,
        now=NOW,
    )
    assert table.iloc[0]["SEMÁFORO"] == signal


def test_pause_has_named_pause_alert_and_no_sla(definitions):
    table = app.build_tracking_table(
        pd.DataFrame([order(status="BORRADOR TITAN")]),
        pd.DataFrame([log(status="BORRADOR TITAN", hours=5, maximum="")]),
        definitions,
        now=NOW,
    )
    assert table.iloc[0]["SEMÁFORO"] == "🟣 En pausa"
    assert "Pausa: BORRADOR TITAN" in table.iloc[0]["DETALLE SEMÁFORO"]
    assert table.iloc[0]["PLAZO HORAS"] is None


def test_missing_or_mismatched_log_is_never_green(definitions):
    missing = app.build_tracking_table(
        pd.DataFrame([order()]), pd.DataFrame(), definitions, now=NOW
    )
    mismatched = app.build_tracking_table(
        pd.DataFrame([order()]),
        pd.DataFrame([log(status="EN PLANEACIÓN")]),
        definitions,
        now=NOW,
    )
    assert missing.iloc[0]["SEMÁFORO"] == "⚪ Sin medición"
    assert mismatched.iloc[0]["SEMÁFORO"] == "⚪ Sin medición"


def test_weekends_do_not_add_elapsed_hours():
    start = datetime(2026, 9, 4, 23, 30)  # viernes
    end = datetime(2026, 9, 7, 0, 30)  # lunes
    assert app.business_hours_elapsed(start, end) == 1


def test_grid_changes_are_compared_by_order_not_visual_position(definitions):
    original = pd.DataFrame(
        [order("001"), order("002")]
    )
    original["DETALLE COMENTARIOS"] = ""
    displayed = app.display_workbench_df(original, definitions)
    edited = displayed.iloc[::-1].copy()
    edited.loc[edited[app.ID_COLUMN] == "001", "DETALLE COMENTARIOS"] = "Revisar STL"
    assert app.grid_changes(displayed, edited, definitions) == [
        ("001", {"DETALLE COMENTARIOS": "Revisar STL"})
    ]


def test_visual_status_roundtrips_without_false_change(definitions):
    original = pd.DataFrame([order()])
    displayed = app.display_workbench_df(original, definitions)
    assert displayed.iloc[0][app.STATUS_COLUMN].startswith("🔵 ")
    assert app.grid_changes(displayed, displayed.copy(), definitions) == []


def test_aligner_sheet_id_never_reuses_apparatus_sheet_id():
    source = {
        "gsheets": {
            "sheet_id": "aparatos",
            "alineadores_sheet_id": "alineadores",
        }
    }
    assert app.configured_spreadsheet_id(source) == "alineadores"
    assert app.configured_spreadsheet_id({"gsheets": {"sheet_id": "aparatos"}}) == app.DEFAULT_SPREADSHEET_ID


def test_existing_password_hash_format_is_reused():
    stored = "pbkdf2_sha256$1$salt$8fa3d7592a7aedf30af5a656525188ac17c254fbc8cab1c56610278639fb64e2"
    assert app.password_matches("prueba", stored)
    assert not app.password_matches("incorrecta", stored)


def test_remembered_url_user_is_signed_and_cannot_be_renamed():
    passwords = {"Admin": "admin-secret", "Jime": "jime-secret"}
    signature = app.login_url_signature("Admin", passwords)
    assert app.valid_url_login("Admin", signature, passwords)
    assert not app.valid_url_login("Jime", signature, passwords)
    assert not app.valid_url_login("Admin", "altered", passwords)


def test_main_dashboard_renders_with_sheet_snapshot():
    from streamlit.testing.v1 import AppTest

    repository_root = str(Path(__file__).resolve().parents[1])
    script = f'''
import sys
sys.path.insert(0, {repository_root!r})
import pandas as pd
import alineadores_pg as app

definitions = app.parse_process_matrix({PROCESS_VALUES!r})
orders = pd.DataFrame([{order()!r}])
times = pd.DataFrame([{log()!r}])
table = app.build_tracking_table(orders, times, definitions, now=app.datetime(2026, 9, 9, 12))
snapshot = {{
    "user": "Admin", "definitions": definitions, "orders": orders,
    "times": times, "table": table, "at": app.datetime(2026, 9, 9, 12),
}}
app.require_authenticated_user = lambda: "Admin"
app.ensure_times_headers = lambda: None
app.get_snapshot = lambda _: snapshot
app.main()
'''
    at = AppTest.from_string(script, default_timeout=15).run()
    assert not at.exception
    assert any("Control de alineadores" in markdown.value for markdown in at.markdown)
