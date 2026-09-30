"""Plan de tratamiento y envíos repetidos de Control ALINEADORES en la ficha del pedido."""
import copy
import re
import sys
from datetime import date
from pathlib import Path

import pandas as pd
from streamlit.testing.v1 import AppTest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import alineadores_pg as app


ROOT = str(Path(__file__).resolve().parents[1])
SHIPMENT_HEADERS = ["TEMP. SUP", "TEMP. INF", "NO. ALIN SUP", "NO. ALIN INF", "Total",
                    "FECHA PAGO IMPRESIÓN", "FECHA ENVÍO"]
# Fila 2 real de ALINEADORES (nuevo): plan R:V, ocho envíos W:BZ y un Total suelto en CA.
SHEET_HEADERS = [app.ID_COLUMN, app.STATUS_COLUMN, "TIPO", app.PRODUCT_COLUMN, "ETAPA SOLICITUD",
                 "NOMBRE DOCTOR", "NOMBRE PACIENTE", "DETALLE COMENTARIOS", "VENDEDOR", "SERVICIO",
                 "PAQUETE MARCA BLANCA", "ARCHIVOS RECIBIDOS", "FECHA DE RECEPCIÓN",
                 "FECHA PAGO PLANEACION ", "FECHA SUBIDO A TITAN", "FECHA ENTREGA TITAN",
                 "FECHA OBJETIVO ENVÍO", "TEMPLATE SUP", "TEMPLATE INF", "NO. ALIN SUP",
                 "NO. ALIN INF", "Total", *SHIPMENT_HEADERS * 8, "Total"]
COLUMNS = [app.canonical_column_name(item) for item in app.ensure_unique_column_names(SHEET_HEADERS)]


def test_layout_follows_sheet_blocks_and_ignores_loose_total():
    plan, shipments = app.shipment_layout(COLUMNS)
    assert plan.columns == {"temp_sup": "TEMPLATE SUP", "temp_inf": "TEMPLATE INF",
                            "alin_sup": "NO. ALIN SUP", "alin_inf": "NO. ALIN INF", "total": "Total"}
    assert [block.number for block in shipments] == list(range(1, 9))
    assert shipments[0].columns == {"temp_sup": "TEMP. SUP", "temp_inf": "TEMP. INF",
        "alin_sup": "NO. ALIN SUP_2", "alin_inf": "NO. ALIN INF_2", "total": "Total_2",
        "pago": "FECHA PAGO IMPRESIÓN", "envio": "FECHA ENVÍO"}
    assert shipments[-1].columns["alin_sup"] == "NO. ALIN SUP_9"
    assert shipments[-1].columns["envio"] == "FECHA ENVÍO_8"
    columns = app.block_columns(plan, shipments)
    assert len(columns) == 5 + 8 * 7
    assert "Total_10" not in columns and "FECHA OBJETIVO ENVÍO" not in columns
    # Un campo propio dentro de un envío (Polanco) no rompe el bloque.
    polanco = [*COLUMNS[:29], "ADEUDO ENVÍO", *COLUMNS[29:]]
    assert app.shipment_layout(polanco)[1][0].columns == shipments[0].columns
    assert app.shipment_layout([app.ID_COLUMN, "FECHA ENVÍO", "VENDEDOR"]) == (None, [])


def test_quantities_accept_numbers_and_ranges_like_the_sheet():
    assert [app.parse_shipment_quantity(value) for value in ["", "7", "1-7", "5- 8", "8.0", "siete"]] == [
        0, 7, 7, 4, 8, None]
    assert app.shipment_total({"temp_sup": "1", "temp_inf": "0", "alin_sup": "1-7", "alin_inf": "7"}) == "15"
    assert app.shipment_total({"alin_sup": "", "alin_inf": ""}) == ""
    assert app.shipment_total({"alin_sup": "pendiente"}) is None


def test_validation_rejects_free_text_quantities_and_accepts_payment_words():
    row = pd.Series({**dict.fromkeys(COLUMNS, ""), app.ID_COLUMN: "001", app.PRODUCT_COLUMN: "GRAPHY"})
    assert app.validate_delta(row, {"NO. ALIN SUP_3": "8-14", "FECHA PAGO IMPRESIÓN_2": "CORTESIA",
                                    "FECHA ENVÍO_2": "2026-10-06"}, {}) == []
    errors = app.validate_delta(row, {"NO. ALIN SUP_3": "unos", "FECHA ENVÍO_2": "CORTESIA"}, {})
    assert any("Envío 2 · Alineadores sup." in error for error in errors)
    assert any("FECHA ENVÍO_2" in error for error in errors)


class FormulaSheet:
    def __init__(self, values, formulas):
        self.values, self.formulas, self.cells = values, formulas, []

    def get_all_values(self):
        return copy.deepcopy(self.values)

    def row_values(self, index, value_render_option=None):
        return (self.formulas if value_render_option == "FORMULA" else self.values)[index - 1]

    def update_cells(self, cells, **kwargs):
        self.cells.extend((cell.col, cell.value) for cell in cells)


def test_saving_totals_keeps_sheet_formulas(monkeypatch):
    headers = [app.ID_COLUMN, "NO. ALIN SUP", "Total", "NO. ALIN SUP", "Total"]
    row = ["001", "14", "14", "", ""]
    sheet = FormulaSheet([[], headers, row], [[], headers, ["001", "14", "=B3", "", ""]])
    monkeypatch.setattr(app.st, "session_state", {})
    monkeypatch.setattr(app, "get_worksheet", lambda name: sheet)
    result = app.update_order_row("001", {"NO. ALIN SUP_2": "7", "Total_2": "7", "Total": "20"})
    assert result["success"]
    assert sorted(sheet.cells) == [(4, "7"), (5, "7")]


def editor_script(sheet, row):
    return f'''
import sys
sys.path.insert(0, {ROOT!r})
import pandas as pd
import streamlit as st
import alineadores_pg as app
st.session_state["aligners_order_sheet"] = {sheet!r}
st.session_state.setdefault("row", {row!r})
app.get_order_form_catalog = lambda: {{}}
def save(source, baseline, edited, user, definitions, **kwargs):
    changes = app.grid_changes(baseline, edited, definitions)
    errors = app.validate_delta(source.iloc[0], dict(changes)["001"], definitions)
    if errors:
        return [], errors
    st.session_state["attempt"] = changes
    st.session_state.row.update(dict(changes)["001"])
    return ["001"], []
app.save_workbench_changes = save
app.render_aligner_order_editor(pd.Series(st.session_state.row), "Jime", {{}})
'''


def order_row(**values):
    return {**dict.fromkeys(COLUMNS, ""), app.ID_COLUMN: "001", app.STATUS_COLUMN: "EN IMPRESIÓN",
            app.PRODUCT_COLUMN: "GRAPHY", "NOMBRE DOCTOR": "Doctor demo", "NOMBRE PACIENTE": "Paciente demo",
            **values}


def button(at, prefix):
    return next(item for item in at.button if item.label.startswith(prefix))


def shipment_text(at, label):
    return next(item for item in at.text_input if item.label == label)


def test_first_shipment_is_the_only_one_offered_and_total_is_calculated():
    at = AppTest.from_string(editor_script(app.SHEET_ORDERS, order_row()), default_timeout=20).run()
    assert not at.exception
    assert any(item.label.startswith("🚚 Envíos y alineadores · sin envíos") for item in at.expander)
    assert not any(item.label.startswith("➕ Registrar") for item in at.button)
    assert any("##### 📦 Envío 1" in item.value for item in at.markdown)
    # Ni el plan ni los envíos se capturan como campos sueltos de la ficha.
    assert not any("ALIN" in item.label or "TEMP" in item.label for item in at.text_input)
    template = next(item for item in at.number_input if item.label == "🧩 Template sup." and item.key.endswith("TEMP. SUP"))
    template.set_value(1).run()
    shipment_text(at, "🦷 Alineadores sup.").input("1-7").run()
    shipment_text(at, "🦷 Alineadores inf.").input("7").run()
    at.radio[0].set_value("🎁 Cortesía").run()
    button(at, "💾 Guardar este pedido").click().run()
    assert not at.exception and not at.error
    assert at.session_state["attempt"] == [("001", {
        "TEMP. SUP": "1", "NO. ALIN SUP_2": "1-7", "NO. ALIN INF_2": "7", "Total_2": "15",
        "FECHA PAGO IMPRESIÓN": "CORTESIA"})]
    assert button(at, "➕ Registrar Envío 2")


def test_next_shipment_shows_previous_values_copies_and_saves_only_its_block():
    row = order_row(**{"NO. ALIN SUP": "14", "NO. ALIN INF": "14", "Total": "28",
                       "TEMP. SUP": "0", "TEMP. INF": "0", "NO. ALIN SUP_2": "1-7",
                       "NO. ALIN INF_2": "1-7", "Total_2": "14",
                       "FECHA PAGO IMPRESIÓN": "25 septiembre", "FECHA ENVÍO": "29 septiembre"})
    at = AppTest.from_string(editor_script(app.SHEET_ORDERS, row), default_timeout=20).run()
    assert not at.exception
    assert any(item.value == "📦 Enviado: **14** de 28 piezas del plan · faltan 14" for item in at.markdown)
    assert shipment_text(at, "🦷 Alineadores sup.").value == "1-7"
    button(at, "➕ Registrar Envío 2").click().run()
    assert not at.exception
    assert shipment_text(at, "🦷 Alineadores sup.").value == ""
    captions = [item.value for item in at.caption]
    assert "↳ Envío 1: **1-7**" in captions and "↳ Envío 1: **29/09/2026**" in captions
    button(at, "📋 Usar las mismas cantidades").click().run()
    assert shipment_text(at, "🦷 Alineadores sup.").value == "1-7"
    shipment_text(at, "🦷 Alineadores sup.").input("8-14").run()
    shipment_text(at, "🦷 Alineadores inf.").input("8-14").run()
    at.date_input[-1].set_value(date(2026, 10, 6)).run()
    button(at, "💾 Guardar este pedido").click().run()
    assert not at.exception and not at.error
    [(identifier, delta)] = at.session_state["attempt"]
    assert identifier == "001"
    assert delta == {"TEMP. SUP_2": "0", "TEMP. INF_2": "0", "NO. ALIN SUP_3": "8-14",
                     "NO. ALIN INF_3": "8-14", "Total_3": "14", "FECHA ENVÍO_2": "2026-10-06"}
    assert any(item.label.endswith("2 envíos registrados") for item in at.expander)
    assert button(at, "➕ Registrar Envío 3")


def test_cancel_new_shipment_discards_only_its_fields():
    row = order_row(**{"NO. ALIN SUP_2": "7", "NO. ALIN INF_2": "7", "Total_2": "14"})
    at = AppTest.from_string(editor_script(app.SHEET_ORDERS, row), default_timeout=20).run()
    shipment_text(at, "🦷 Alineadores inf.").input("6").run()
    button(at, "➕ Registrar Envío 2").click().run()
    shipment_text(at, "🦷 Alineadores sup.").input("8").run()
    assert "4 campos con cambios" in [item.value for item in at.caption]
    button(at, "✖️ Cancelar nuevo envío").click().run()
    assert not at.exception
    assert shipment_text(at, "🦷 Alineadores inf.").value == "6"
    button(at, "💾 Guardar este pedido").click().run()
    assert at.session_state["attempt"] == [("001", {"NO. ALIN INF_2": "6", "Total_2": "13"})]


def test_invalid_quantity_is_flagged_and_not_saved():
    at = AppTest.from_string(editor_script(app.SHEET_ORDERS, order_row()), default_timeout=20).run()
    shipment_text(at, "🦷 Alineadores sup.").input("siete").run()
    assert any("Usa un número (7) o un rango (1-7)." in item.value for item in at.caption)
    button(at, "💾 Guardar este pedido").click().run()
    assert "attempt" not in at.session_state
    assert any("debe ser un número o un rango" in item.value for item in at.error)


def test_polanco_shipment_columns_move_from_loose_fields_to_the_section():
    row = order_row(ADEUDO="200")
    at = AppTest.from_string(editor_script(app.SHEET_POLANCO, row), default_timeout=20).run()
    assert not at.exception
    assert any(item.label.startswith("🚚 Envíos y alineadores") for item in at.expander)
    labels = [item.label for item in [*at.text_input, *at.date_input, *at.number_input]]
    assert not any(re.search(r"·\s*\d", label) for label in labels)
    assert not {"📅 FECHA ENVÍO", "📅 FECHA PAGO IMPRESIÓN", "Temp. Sup", "No. Alin Sup"} & set(labels)
    assert any("ADEUDO" in item.label.upper() for item in at.text_input)
