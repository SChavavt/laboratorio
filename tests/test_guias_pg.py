"""Pruebas de Solicitudes de guía; no acceden a Sheets ni S3."""
from datetime import date, datetime
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import guias_pg as guides
import lab_pg as app


NOW = datetime(2026, 9, 24, 12)
ROOT = str(Path(__file__).resolve().parents[1])
ADDRESS = {
    "nombre": " JUAN MEJIA ", "calle_numero": "Av. Reforma 10", "colonia": "Centro",
    "codigo_postal": "06000", "ciudad": "CDMX", "estado": "CDMX", "pais": "México",
    "telefono": "5512345678", "interior": "", "municipio": "Cuauhtémoc", "correo": "",
    "referencias": "Portón negro",
}
ORDER_HEADERS = [
    "ID_Pedido", "Hora_Registro", "id_vendedor", "Vendedor_Registro", "Cliente", "Folio_Factura",
    "Tipo_Envio", "Turno", "Fecha_Entrega", "Comentario", "Estado", "Estado_Pago",
    "Aplica_Pago", "Monto_Comprobante", "Adjuntos", "TD_Leal_Etapa", "Tipo_Venta",
    "Direccion_Guia_Retorno", "Adjuntos_Guia",
]


def test_request_row_matches_what_app_v_saves_for_arttd_jimena():
    row = dict(zip(ORDER_HEADERS, guides.build_guide_request_values(
        ORDER_HEADERS, pedido_id="PED-1", hora_registro="2026-09-24 12:00:00", cliente="JUAN MEJIA",
        folio_factura="F1234", comentario="Urgente", direccion_guia="Nombre: JUAN MEJIA",
        fecha_entrega=date(2026, 9, 24),
    )))
    assert row == {
        "ID_Pedido": "PED-1", "Hora_Registro": "2026-09-24 12:00:00", "id_vendedor": "ARTTDJIM01",
        "Vendedor_Registro": "ARTTD JIMENA", "Cliente": "JUAN MEJIA", "Folio_Factura": "F1234",
        "Tipo_Envio": "📋 Solicitudes de Guía", "Turno": "", "Fecha_Entrega": "2026-09-24",
        "Comentario": "Urgente", "Estado": "🟡 Pendiente", "Estado_Pago": "", "Aplica_Pago": "No",
        "Monto_Comprobante": "", "Adjuntos": "", "TD_Leal_Etapa": "NO APLICA", "Tipo_Venta": "Venta TD",
        "Direccion_Guia_Retorno": "Nombre: JUAN MEJIA", "Adjuntos_Guia": "",
    }


def test_dhl_address_uses_app_v_labels_and_skips_empty_fields():
    assert guides.build_dhl_mexico_address_payload(ADDRESS) == "\n".join([
        "Nombre: JUAN MEJIA", "Calle y número: Av. Reforma 10", "Colonia: Centro",
        "Código Postal: 06000", "Ciudad: CDMX", "Estado: CDMX", "País: México",
        "Teléfono: 5512345678", "Municipio / Alcaldía: Cuauhtémoc",
        "Referencias de dirección: Portón negro",
    ])
    assert guides.get_missing_dhl_mexico_required_fields({**ADDRESS, "colonia": " ", "telefono": ""}) == [
        "Colonia", "Teléfono",
    ]


def test_submission_identity_uses_app_v_format():
    pedido_id, hora_registro = guides.build_submission_identity(datetime(2026, 9, 24, 8, 5, 9))
    assert pedido_id.startswith("PED-20260924080509-") and len(pedido_id) == 27
    assert hora_registro == "2026-09-24 08:05:09"


def test_sheet_id_and_credentials_come_from_secrets():
    assert guides.configured_spreadsheet_id({}) == guides.DEFAULT_SPREADSHEET_ID
    assert guides.configured_spreadsheet_id({"gsheets": {"ventas_sheet_id": " abc "}}) == "abc"
    assert guides.configured_spreadsheet_id({"ventas_sheet_id": "root"}) == "root"
    lab_only = {"gsheets": {"google_credentials": '{"private_key": "a\\\\nb"}'}}
    assert guides.configured_credentials_info(lab_only)["private_key"] == "a\nb"
    own = {"gsheets": {"google_credentials": "{}", "ventas_google_credentials": {"client_email": "v@x"}}}
    assert guides.configured_credentials_info(own) == {"client_email": "v@x"}


class FakeWorksheet:
    def __init__(self, ids, updated_range="'data_pedidos'!A5:C5", left=None):
        self.ids, self.updated_range, self.left = list(ids), updated_range, left or []
        self.appended, self.updates, self.cleared = [], [], []

    def col_values(self, _):
        return self.ids

    def append_row(self, values, **_):
        self.appended.append(values)
        self.ids.append(values[0])
        return {"updates": {"updatedRange": self.updated_range}}

    def get(self, _):
        return self.left

    def update(self, cell_range, values, **_):
        self.updates.append((cell_range, values))

    def batch_clear(self, ranges):
        self.cleared.extend(ranges)


def test_append_is_skipped_when_the_same_id_was_already_written():
    worksheet = FakeWorksheet(["ID_Pedido", "PED-1"])
    guides.append_row_with_confirmation(worksheet, ["PED-1", "x"], "PED-1", 0, base_delay=0)
    assert worksheet.appended == []
    guides.append_row_with_confirmation(worksheet, ["PED-2", "x"], "PED-2", 0, base_delay=0)
    assert worksheet.appended == [["PED-2", "x"]]


def test_shifted_append_is_moved_back_to_column_a():
    worksheet = FakeWorksheet(["ID_Pedido"], updated_range="'data_pedidos'!AF53:AH53")
    guides.append_row_with_confirmation(worksheet, ["PED-3", "b", "c"], "PED-3", 0, base_delay=0)
    assert guides.appended_range_start({"updates": {"updatedRange": "'x'!AF53:AH53"}}) == (53, 32)
    cell_range, values = worksheet.updates[0]
    assert cell_range == "A53:AH53"
    assert values[0][:3] == ["PED-3", "b", "c"] and len(values[0]) == 34


def sources():
    pedidos = pd.DataFrame([
        {"ID_Pedido": "PED-A", "Cliente": "JUAN MEJIA", "Vendedor_Registro": "ARTTD JIMENA",
         "Tipo_Envio": "📋 Solicitudes de Guía", "Hora_Registro": "2026-09-24 09:00:00",
         "Fecha_Entrega": "2026-09-24", "id_vendedor": "arttdjim01",
         "Adjuntos_Guia": "https://b/g1.pdf, https://b/g2.pdf", "Folio_Factura": ""},
        {"ID_Pedido": "PED-B", "Cliente": "OTRO", "Vendedor_Registro": "BLANCA BRASILIA",
         "Hora_Registro": "2026-09-23 09:00:00", "Adjuntos_Guia": "https://b/g3.pdf", "id_vendedor": "BLANCA96"},
        {"ID_Pedido": "PED-C", "Cliente": "SIN GUIA", "Vendedor_Registro": "ARTTD JIMENA",
         "Hora_Registro": "2026-09-24 10:00:00", "id_vendedor": "ARTTDJIM01"},
        {"ID_Pedido": "PED-D", "Cliente": "LIMPIADO", "Vendedor_Registro": "ARTTD JIMENA",
         "Hora_Registro": "2026-09-24 10:00:00", "id_vendedor": "ARTTDJIM01",
         "Hoja_Ruta_Mensajero": "https://b/g4.pdf", "Completados_Limpiado": "sí"},
    ])
    viejo = pd.DataFrame([
        {"ID_Pedido": "PED-OLD", "Cliente": "VIEJO", "Vendedor_Registro": "SCHAVA",
         "Hora_Registro": "2026-07-01 09:00:00", "Adjuntos_Guia": "https://b/old.pdf"},
    ])
    casos = pd.DataFrame([
        {"ID_Pedido": "CAS-1", "Cliente": "CASO", "Vendedor_Registro": "SCHAVA", "Tipo_Caso": "Garantía",
         "Hora_Registro": "2026-09-22 09:00:00", "Hoja_Ruta_Mensajero": "https://b/c1.pdf"},
    ])
    return {guides.SHEET_PEDIDOS_OPERATIVOS: pedidos, guides.SHEET_PEDIDOS_HISTORICOS: viejo,
            guides.SHEET_CASOS_ESPECIALES: casos}


def test_dataset_keeps_rows_with_guides_from_the_three_sheets():
    dataset = guides.build_guides_dataset(sources())
    assert dataset["ID_Pedido"].tolist() == ["PED-D", "PED-A", "PED-B", "CAS-1", "PED-OLD"]
    first = dataset.set_index("ID_Pedido").loc["PED-A"]
    assert first["Ultima_Guia"] == "https://b/g2.pdf" and first["Folio_O_ID"] == "PED-A"
    assert dataset.set_index("ID_Pedido").loc["CAS-1", "Tipo_Envio"] == "🛠 Garantía"
    assert dataset.set_index("ID_Pedido").loc["PED-D", "Adjuntos_Guia"] == "https://b/g4.pdf"


def test_notice_counts_recent_uncleared_guides_of_arttd_jimena():
    summary = guides.summarize_session_guides(guides.build_guides_dataset(sources()), now=NOW)
    assert summary["total"] == 1 and summary["clientes"] == ["JUAN MEJIA"]
    assert guides.format_clients_preview(["A", "B", "C", "D"]) == "A, B y 2 más"
    assert guides.summarize_session_guides(pd.DataFrame(columns=guides.GUIDE_COLUMNS)) == {
        "total": 0, "clientes": [], "keys": []}


def test_loaded_tab_shows_last_month_of_jimena_and_schava_only():
    dataset = guides.build_guides_dataset(sources())
    visible = guides.visible_vendor_guides(guides.recent_guides(dataset, now=NOW))
    assert visible["ID_Pedido"].tolist() == ["PED-D", "PED-A", "CAS-1"]
    today = NOW.date()
    assert guides.apply_guides_filters(visible, date_mode="day", day=date(2026, 9, 22), today=today)[
        "ID_Pedido"].tolist() == ["CAS-1"]
    assert guides.apply_guides_filters(visible, vendor="SCHAVA", today=today)["ID_Pedido"].tolist() == ["CAS-1"]
    # Un rango invertido no filtra fechas, igual que app_v.
    assert len(guides.apply_guides_filters(visible, date_mode="range", start=today, end=date(2026, 9, 1))) == 3


def test_split_urls_accepts_json_and_plain_text():
    assert guides.split_urls('["https://a/1.pdf", {"url": "https://a/2.pdf"}]') == [
        "https://a/1.pdf", "https://a/2.pdf"]
    assert guides.split_urls("https://a/1.pdf, https://a/1.pdf\nhttps://a/3.pdf") == [
        "https://a/1.pdf", "https://a/3.pdf"]
    assert guides.split_urls("nan") == []


def test_guides_view_is_part_of_the_workspace_switch():
    assert app.LAB_VIEW_GUIDES in app.LAB_WORKSPACE_VIEWS
    assert app.normalize_workspace_view("guias") == app.LAB_VIEW_GUIDES
    assert app.normalize_workspace_view(app.LAB_VIEW_GUIDES) == app.LAB_VIEW_GUIDES
    assert app.normalize_workspace_view("alineadores") == app.LAB_VIEW_ALIGNERS
    assert app.normalize_workspace_view("") == app.LAB_VIEW_APPARATUS
    assert app.LAB_WORKSPACE_URL_VALUES[app.LAB_VIEW_GUIDES] == "guias"
    palettes = app.LAB_SHELL_PALETTES
    assert palettes[app.LAB_VIEW_GUIDES].keys() == palettes[app.LAB_VIEW_APPARATUS].keys()


def test_unified_app_switches_to_guides_without_loading_apparatus():
    from streamlit.testing.v1 import AppTest

    script = f"""
import sys
sys.path.insert(0, {ROOT!r})
import streamlit as st
import lab_pg as app
from unittest.mock import patch

st.session_state[app.LAB_WORKSPACE_STATE_KEY] = app.LAB_VIEW_GUIDES
st.session_state["apparatus_loaded"] = False
with (
    patch.object(app, "require_authenticated_user", lambda: "Jime"),
    patch.object(app, "ensure_tiempos_headers", lambda: st.session_state.__setitem__("apparatus_loaded", True)),
    patch.object(app.guias_pg, "render_embedded_workspace",
                 lambda user: st.session_state.__setitem__("guides_loaded", user)),
):
    app.main()
"""
    at = AppTest.from_string(script, default_timeout=15).run()
    assert not at.exception
    assert at.session_state["guides_loaded"] == "Jime"
    assert at.session_state["apparatus_loaded"] is False
    assert any("Solicitudes de guía" in markdown.value for markdown in at.markdown)


GUIDES_SCRIPT = f"""
import sys
sys.path.insert(0, {ROOT!r})
from datetime import datetime
from unittest.mock import patch
import pandas as pd
import streamlit as st
import guias_pg as guides

SOURCES = {{
    guides.SHEET_PEDIDOS_OPERATIVOS: pd.DataFrame([{{
        "ID_Pedido": "PED-A", "Cliente": "JUAN MEJIA", "Vendedor_Registro": "ARTTD JIMENA",
        "Tipo_Envio": "📋 Solicitudes de Guía", "Hora_Registro": "2026-09-24 09:00:00",
        "Fecha_Entrega": "2026-09-24", "id_vendedor": "ARTTDJIM01", "Adjuntos_Guia": "https://b/g1.pdf",
    }}]),
}}

def fake_register(request):
    st.session_state["registered"] = request
    return request["direccion"]["nombre"].strip()

# patch.object restaura el módulo compartido al terminar cada ejecución.
with (
    patch.object(guides, "app_now", lambda: datetime(2026, 9, 24, 12)),
    patch.object(guides, "read_guides_sources", lambda token=None: SOURCES),
    patch.object(guides, "guide_browser_url", lambda url: url),
    patch.object(guides, "register_guide_request", fake_register),
):
    guides.apply_custom_css()
    guides.render_embedded_workspace("Jime")
"""


def fill_address(at, version=0):
    for key, value in ADDRESS.items():
        if key != "referencias":
            at.text_input(key=f"guides_{key}_{version}").input(value)
    at.text_area(key=f"guides_referencias_{version}").input(ADDRESS["referencias"])


def test_request_form_registers_guide_like_app_v():
    from streamlit.testing.v1 import AppTest

    at = AppTest.from_string(GUIDES_SCRIPT, default_timeout=15).run()
    assert not at.exception
    assert [tab.label for tab in at.tabs] == guides.GUIDES_TAB_LABELS
    assert any("tienes 1 pedido(s) con guía cargada. Clientes: JUAN MEJIA." in item.value for item in at.info)
    assert at.selectbox(key="guides_vendedor_0").value == "ARTTD JIMENA"
    assert at.selectbox(key="guides_vendedor_0").disabled
    assert at.text_input(key="guides_pais_0").value == "México"

    at.text_input(key="guides_nombre_0").input("JUAN MEJIA")
    at.button[0].click().run()
    assert "registered" not in at.session_state
    assert any("Completa los campos obligatorios" in item.value for item in at.warning)

    fill_address(at)
    at.text_input(key="guides_folio_0").input(" F999 ")
    at.text_area(key="guides_comentario_0").input("Enviar hoy")
    at.button[0].click().run()
    assert not at.exception
    request = at.session_state["registered"]
    assert request["folio_factura"] == " F999 " and request["comentario"] == "Enviar hoy"
    assert request["direccion"]["codigo_postal"] == "06000"
    assert str(request["fecha_entrega"]) == "2026-09-24"
    assert any("JUAN MEJIA (ID vendedor: ARTTDJIM01) fue subido correctamente" in item.value
               for item in at.success)
    # El formulario se limpia con una nueva versión de claves.
    assert at.text_input(key="guides_nombre_1").value == ""


def test_loaded_tab_lists_the_guide_and_its_link():
    from streamlit.testing.v1 import AppTest

    at = AppTest.from_string(GUIDES_SCRIPT, default_timeout=15)
    at.session_state["guides_primary_tabs_Jime"] = guides.GUIDES_TAB_LOADED
    at.run()
    assert not at.exception
    assert at.selectbox(key="guides_filter_vendor").value == "ARTTD JIMENA"
    assert at.selectbox(key="guides_selected_order").value.startswith("📄 PED-A – JUAN MEJIA – ARTTD JIMENA")
    assert at.dataframe[0].value["Cliente"].tolist() == ["JUAN MEJIA"]


class FakeOrdersSheet(FakeWorksheet):
    def __init__(self, headers):
        super().__init__(["ID_Pedido"])
        self.headers = list(headers)

    def row_values(self, _):
        return self.headers

    def update(self, cell_range, values, **kwargs):
        super().update(cell_range, values, **kwargs)
        if cell_range == "A1":
            self.headers = list(values[0])


def test_register_adds_missing_columns_and_writes_one_row(monkeypatch):
    sheet = FakeOrdersSheet(["ID_Pedido", "Hora_Registro", "id_vendedor", "Vendedor_Registro", "Cliente",
                             "Tipo_Envio", "Comentario", "Estado"])
    monkeypatch.setattr(guides, "get_worksheet", lambda name: sheet)
    monkeypatch.setattr(guides, "app_now", lambda: NOW)
    monkeypatch.setattr(guides.st, "session_state", {})
    request = {"folio_factura": "", "comentario": " hola ", "direccion": ADDRESS, "fecha_entrega": NOW.date()}

    assert guides.register_guide_request(request) == "JUAN MEJIA"
    assert sheet.headers[-len(guides.REQUIRED_ORDER_HEADERS):] == guides.REQUIRED_ORDER_HEADERS
    row = dict(zip(sheet.headers, sheet.appended[0]))
    assert row["id_vendedor"] == "ARTTDJIM01" and row["Cliente"] == "JUAN MEJIA"
    assert row["Comentario"] == "hola" and row["Hora_Registro"] == "2026-09-24 12:00:00"
    assert row["Direccion_Guia_Retorno"].startswith("Nombre: JUAN MEJIA\nCalle y número: Av. Reforma 10")
    assert guides.st.session_state == {}


def test_retry_of_same_request_reuses_the_order_id(monkeypatch):
    monkeypatch.setattr(guides.st, "session_state", {})
    request = {"folio_factura": "", "comentario": "", "direccion": ADDRESS, "fecha_entrega": NOW.date()}
    first = guides.submission_identity_for(request)
    assert guides.submission_identity_for(request) == first
    assert guides.submission_identity_for({**request, "comentario": "otro"}) != first
