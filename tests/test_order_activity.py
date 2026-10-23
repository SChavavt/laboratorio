"""Últimos guardados arriba, sin mover filas de Sheets ni tocar fórmulas."""
import copy
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

import alineadores_pg as align
import lab_pg as lab
from order_activity import LAST_EDIT_COLUMN, RECENT_FIRST, SHEET_ORDER, URGENT_FIRST, recent_first
from test_aeris import definitions
from test_general import general_app
from test_polanco import Sheet


LAB_UPDATE = lab.update_row_by_columna_1
LAB_APPEND = lab.append_estatus_row
ALIGN_UPDATE = align.update_order_row
ALIGN_CREATE = align.create_order


class OrderSheet(Sheet):
    def __init__(self, values):
        super().__init__(values)
        self.col_count = 12  # Hay celdas/fórmulas sin encabezado más a la derecha.
        self.writes = []
        self.resizes = []

    def resize(self, *, cols):
        self.resizes.append(cols)
        self.col_count = cols

    def update_cells(self, cells, **kwargs):
        self.writes.append([(cell.row, cell.col, cell.value) for cell in cells])
        for cell in cells:
            while len(self.values) < cell.row:
                self.values.append([])
        super().update_cells(cells, **kwargs)


def make_sheet(id_column, kind_column):
    return OrderSheet([[], [id_column, "STATUS", kind_column, "NOMBRE DOCTOR",
        "NOMBRE PACIENTE", "DETALLE COMENTARIOS", "FECHA PARA ENTREGA"],
        ["001", "REVISIÓN DE ARCHIVOS", "Graphy", "Doctor", "Primero", "", "=HOY()+5"],
        ["002", "REVISIÓN DE ARCHIVOS", "Graphy", "Doctor", "Segundo", "", "=HOY()+6"]])


@pytest.mark.parametrize("sheet_name", [lab.SHEET_ESTATUS, *align.ORDER_TIME_SHEETS])
def test_saved_edits_survive_reload_and_never_move_physical_rows(monkeypatch, sheet_name):
    apparatus = sheet_name == lab.SHEET_ESTATUS
    module = lab if apparatus else align
    id_column = module.ID_COLUMN
    sheet = make_sheet(id_column, lab.APARATO_COLUMN if apparatus else align.PRODUCT_COLUMN)
    monkeypatch.setattr(module.st, "session_state", {})
    monkeypatch.setattr(module, "get_worksheet", lambda _: sheet)
    now = [datetime(2026, 10, 23, 12, 0, 0, 1)]
    monkeypatch.setattr(module, "app_now", lambda: now[0])
    writer = LAB_UPDATE if apparatus else ALIGN_UPDATE

    def reloaded():
        # Una lectura nueva desde el almacenamiento, sin metadatos de sesión.
        rows = [row + [""] * (len(sheet.values[1]) - len(row)) for row in sheet.get_all_values()[2:]]
        return pd.DataFrame(rows, columns=sheet.values[1])

    assert writer("002", {"NOMBRE PACIENTE": "Segundo editado"})["success"]
    position = sheet.values[1].index(LAST_EDIT_COLUMN)
    assert position == 12 and sheet.resizes == [13]
    assert recent_first(reloaded())[id_column].tolist() == ["002", "001"]
    assert [row[0] for row in sheet.values[2:]] == ["001", "002"]
    assert [row[6] for row in sheet.values[2:]] == ["=HOY()+5", "=HOY()+6"]
    # Dato corregido + fecha + encabezado se confirman en una sola escritura.
    assert {(row, col) for row, col, _ in sheet.writes[-1]} == {(4, 5), (2, 13), (4, 13)}

    now[0] = datetime(2026, 10, 23, 12, 0, 0, 2)
    assert writer("001", {"DETALLE COMENTARIOS": "Nueva nota"})["success"]
    assert recent_first(reloaded())[id_column].tolist() == ["001", "002"]
    assert sheet.values[1].count(LAST_EDIT_COLUMN) == 1 and sheet.resizes == [13]
    last_timestamp = sheet.values[2][position]
    now[0] = datetime(2026, 10, 23, 12, 1)
    assert writer("001", {"DETALLE COMENTARIOS": "Nueva nota"})["success"]
    assert sheet.values[2][position] == last_timestamp  # Guardar lo mismo no lo vuelve a subir.

    before = copy.deepcopy(sheet.values)
    failed = writer("002", {"NOMBRE PACIENTE": "No aplicar"}, expected_values={"NOMBRE PACIENTE": "Dato viejo"})
    assert not failed["success"] and sheet.values == before
    monkeypatch.setattr(sheet, "update_cells", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("sin conexión")))
    failed = writer("002", {"NOMBRE PACIENTE": "No aplicar"})
    assert not failed["success"] and sheet.values == before


@pytest.mark.parametrize("sheet_name", [lab.SHEET_ESTATUS, *align.ORDER_TIME_SHEETS])
def test_new_cases_also_receive_a_persistent_timestamp(monkeypatch, sheet_name):
    apparatus = sheet_name == lab.SHEET_ESTATUS
    module = lab if apparatus else align
    sheet = make_sheet(module.ID_COLUMN, lab.APARATO_COLUMN if apparatus else align.PRODUCT_COLUMN)
    monkeypatch.setattr(module.st, "session_state", {})
    monkeypatch.setattr(module, "app_now", lambda: datetime(2026, 10, 23, 12))
    monkeypatch.setattr(module, "apply_estatus_row_styles" if apparatus else "register_status_change", lambda *args, **kwargs: None)
    if apparatus:
        monkeypatch.setattr(lab, "get_worksheet", lambda _: sheet)
        LAB_APPEND({lab.ID_COLUMN: "003", lab.APARATO_COLUMN: "MSE", "NOMBRE PACIENTE": "Nuevo"})
    else:
        align.st.session_state["aligners_order_sheet"] = sheet_name
        times = Sheet([align.TIMES_HEADERS])
        monkeypatch.setattr(align, "get_worksheet", lambda name: sheet if name == sheet_name else times)
        ALIGN_CREATE({align.PRODUCT_COLUMN: "AERIS" if sheet_name == align.SHEET_AERIS else "Graphy",
                      "NOMBRE DOCTOR": "Doctor", "NOMBRE PACIENTE": "Nuevo",
                      LAST_EDIT_COLUMN: "1900-01-01"}, "Jime", definitions())
    position = sheet.values[1].index(LAST_EDIT_COLUMN)
    assert sheet.values[-1][position] == "2026-10-23T18:00:00.000000+00:00"
    assert sheet.values[-1][4] == "Nuevo"


def test_sort_handles_timezones_ties_bad_dates_and_legacy_rows_without_mutation():
    frame = pd.DataFrame({"id": ["legacy1", "offset", "new", "tie", "bad", "legacy2"],
        LAST_EDIT_COLUMN: ["", "2026-10-23T12:00:00-06:00", "2026-10-23T18:00:00.000001Z",
                           "2026-10-23T18:00:00Z", "no es fecha", None]}, index=[9, 4, 7, 1, 3, 2])
    original = frame.copy(deep=True)
    assert recent_first(frame)["id"].tolist() == ["new", "offset", "tie", "legacy1", "bad", "legacy2"]
    pd.testing.assert_frame_equal(frame, original)
    assert recent_first(frame.drop(columns=LAST_EDIT_COLUMN))["id"].tolist() == frame["id"].tolist()


def test_general_save_moves_only_the_edited_origin_to_the_top():
    at = general_app()
    at.session_state["grid_edits"] = {"🏢 Polanco|001": {"NOMBRE PACIENTE": "Beto editado"}}
    at.run()
    assert not at.exception
    assert at.selectbox(key="general_order_mode").value == RECENT_FIRST
    at.button(key="general_aligners_save").click().run()
    assert not at.exception and not at.error
    assert at.session_state["grid_shown"][align.ORIGIN_COLUMN].tolist() == ["🏢 Polanco", "📋 Seguimiento"]
    assert LAST_EDIT_COLUMN not in at.session_state["grid_shown"]
    at.selectbox(key="general_order_mode").select(SHEET_ORDER).run()
    assert at.session_state["grid_shown"][align.ORIGIN_COLUMN].tolist() == ["📋 Seguimiento", "🏢 Polanco"]


@pytest.mark.parametrize("view", ["aparatos", "alineadores"])
def test_both_views_default_to_recent_and_still_offer_other_orders(view):
    root = str(Path(__file__).resolve().parents[1])
    script = f'''
import sys
sys.path.insert(0, {root!r})
sys.path.insert(0, {str(Path(__file__).parent)!r})
import pandas as pd
import streamlit as st
from unittest.mock import patch
from test_aeris import definitions
from order_activity import LAST_EDIT_COLUMN
import lab_pg as lab
import alineadores_pg as align

view = {view!r}
app = lab if view == "aparatos" else align
orders = pd.DataFrame([
    {{app.ID_COLUMN: "001", "STATUS": "REVISIÓN DE ARCHIVOS", "APARATO": "MSE", "PRODUCTO": "Graphy",
      "NOMBRE DOCTOR": "Doctor", "NOMBRE PACIENTE": "Primero", LAST_EDIT_COLUMN: "2026-10-23T18:00:00Z"}},
    {{app.ID_COLUMN: "002", "STATUS": "REVISIÓN DE ARCHIVOS", "APARATO": "MSE", "PRODUCTO": "Graphy",
      "NOMBRE DOCTOR": "Doctor", "NOMBRE PACIENTE": "Segundo", LAST_EDIT_COLUMN: "2026-10-23T18:00:01Z"}},
])
orders["Columna_11"] = ""  # Separación sin encabezado antes de la columna interna.

def fake_grid(grid, options, key):
    st.session_state["shown"] = grid
    return (grid, []) if view == "aparatos" else grid

if view == "aparatos":
    with patch.object(lab, "read_sheet_df", lambda name: orders if name == lab.SHEET_ESTATUS else pd.DataFrame()), \\
         patch.object(lab, "get_workbench_form_catalog", lambda: {{}}), \\
         patch.object(lab, "workbench_saved_column_order", lambda *args: []), \\
         patch.object(lab, "render_grid", fake_grid):
        lab.render_workbench("Jime")
else:
    flows = definitions()
    snapshot = {{"user": "Jime", "definitions": flows, "orders": orders, "times": pd.DataFrame(),
        "table": align.build_tracking_table(orders, pd.DataFrame(), flows), "at": align.app_now()}}
    with patch.object(align, "get_snapshot", lambda _: snapshot), \\
         patch.object(align, "tracking_form_catalog", lambda: {{}}), \\
         patch.object(align, "render_aligners_grid", fake_grid):
        align.render_workbench("Jime")
'''
    at = AppTest.from_string(script, default_timeout=15).run()
    assert not at.exception
    key = "workbench_order" if view == "aparatos" else "aligners_order_mode"
    id_column = lab.ID_COLUMN if view == "aparatos" else align.ID_COLUMN
    assert at.selectbox(key=key).value == RECENT_FIRST
    assert at.session_state["shown"][id_column].tolist() == ["002", "001"]
    assert LAST_EDIT_COLUMN not in at.session_state["shown"]
    assert "Columna_11" not in at.session_state["shown"]
    at.selectbox(key=key).select(SHEET_ORDER).run()
    assert not at.exception
    assert at.session_state["shown"][id_column].tolist() == ["001", "002"]
    at.selectbox(key=key).select(URGENT_FIRST).run()
    assert not at.exception


def test_activity_column_cannot_be_edited_from_the_grid_or_order_card():
    assert LAST_EDIT_COLUMN not in lab.workbench_editable_columns("Admin", [LAST_EDIT_COLUMN])
    assert LAST_EDIT_COLUMN not in align.aligners_editable_columns([LAST_EDIT_COLUMN])
    assert LAST_EDIT_COLUMN not in align.tracking_columns([LAST_EDIT_COLUMN])
