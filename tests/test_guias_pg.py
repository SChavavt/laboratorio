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
    guia = "📋 Solicitudes de Guía"
    pedidos = pd.DataFrame([
        {"ID_Pedido": "PED-A", "Cliente": "JUAN MEJIA", "Vendedor_Registro": "ARTTD JIMENA",
         "Tipo_Envio": guia, "Hora_Registro": "2026-09-24 09:00:00",
         "Fecha_Entrega": "2026-09-24", "id_vendedor": "arttdjim01",
         "Adjuntos_Guia": "https://b/g1.pdf, https://b/g2.pdf", "Folio_Factura": ""},
        {"ID_Pedido": "PED-B", "Cliente": "OTRO", "Vendedor_Registro": "SCHAVA", "Tipo_Envio": guia,
         "Hora_Registro": "2026-09-22 09:00:00", "Adjuntos_Guia": "https://b/g3.pdf", "id_vendedor": "SCHAVA"},
        {"ID_Pedido": "PED-C", "Cliente": "SIN GUIA", "Vendedor_Registro": "ARTTD JIMENA", "Tipo_Envio": guia,
         "Hora_Registro": "2026-09-24 10:00:00", "id_vendedor": "ARTTDJIM01"},
        {"ID_Pedido": "PED-D", "Cliente": "LIMPIADO", "Vendedor_Registro": "ARTTD JIMENA", "Tipo_Envio": guia,
         "Hora_Registro": "2026-09-24 10:00:00", "id_vendedor": "ARTTDJIM01",
         "Hoja_Ruta_Mensajero": "https://b/g4.pdf", "Completados_Limpiado": "sí"},
        # Un pedido de venta con guía no es una solicitud: no aparece en esta vista.
        {"ID_Pedido": "PED-E", "Cliente": "VENTA", "Vendedor_Registro": "SCHAVA",
         "Tipo_Envio": "🚚 Pedido Foráneo", "Hora_Registro": "2026-09-24 11:00:00",
         "Adjuntos_Guia": "https://b/g5.pdf", "id_vendedor": "ARTTDJIM01"},
        {"ID_Pedido": "PED-F", "Cliente": "OTRA VENDEDORA", "Vendedor_Registro": "BLANCA BRASILIA",
         "Tipo_Envio": guia, "Hora_Registro": "2026-09-23 09:00:00", "Adjuntos_Guia": "https://b/g6.pdf"},
        # Solicitudes sin guía: sólo las abiertas siguen en espera.
        {"ID_Pedido": "PED-G", "Cliente": "CANCELADA", "Vendedor_Registro": "ARTTD JIMENA", "Tipo_Envio": guia,
         "Hora_Registro": "2026-09-24 08:00:00", "Estado": "🔴 Cancelado"},
        {"ID_Pedido": "PED-H", "Cliente": "DE SCHAVA", "Vendedor_Registro": "SCHAVA", "Tipo_Envio": guia,
         "Hora_Registro": "2026-09-21 09:30:00", "Estado": "🟡 Pendiente", "Folio_Factura": "F77"},
        {"ID_Pedido": "PED-I", "Cliente": "YA LIMPIADA", "Vendedor_Registro": "ARTTD JIMENA",
         "Tipo_Envio": guia, "Hora_Registro": "2026-09-24 07:00:00", "Completados_Limpiado": "sí"},
        {"ID_Pedido": "PED-J", "Cliente": "VENTA SIN GUIA", "Vendedor_Registro": "ARTTD JIMENA",
         "Tipo_Envio": "🚚 Pedido Foráneo", "Hora_Registro": "2026-09-24 11:00:00"},
    ])
    viejo = pd.DataFrame([
        {"ID_Pedido": "PED-OLD", "Cliente": "VIEJO", "Vendedor_Registro": "SCHAVA", "Tipo_Envio": guia,
         "Hora_Registro": "2026-07-01 09:00:00", "Adjuntos_Guia": "https://b/old.pdf"},
        # Lo que pasó al histórico sin guía ya no está en espera.
        {"ID_Pedido": "PED-OLD2", "Cliente": "ARCHIVADO", "Vendedor_Registro": "ARTTD JIMENA",
         "Tipo_Envio": guia, "Hora_Registro": "2026-09-20 09:00:00"},
    ])
    return {guides.SHEET_PEDIDOS_OPERATIVOS: pedidos, guides.SHEET_PEDIDOS_HISTORICOS: viejo}


def test_dataset_keeps_only_guide_requests_that_already_have_a_guide():
    dataset = guides.build_guides_dataset(sources())
    assert dataset["ID_Pedido"].tolist() == ["PED-D", "PED-A", "PED-F", "PED-B", "PED-OLD"]
    first = dataset.set_index("ID_Pedido").loc["PED-A"]
    assert first["Ultima_Guia"] == "https://b/g2.pdf" and first["Folio_O_ID"] == "PED-A"
    assert dataset.set_index("ID_Pedido").loc["PED-D", "Adjuntos_Guia"] == "https://b/g4.pdf"
    assert guides.guide_display_label(first) == "📋 PED-A – JUAN MEJIA – ARTTD JIMENA · 24/09/26"


def test_notice_counts_recent_uncleared_guides_of_arttd_jimena():
    summary = guides.summarize_session_guides(guides.build_guides_dataset(sources()), now=NOW)
    assert summary["total"] == 1 and summary["clientes"] == ["JUAN MEJIA"]
    assert guides.format_clients_preview(["A", "B", "C", "D"]) == "A, B y 2 más"
    assert guides.summarize_session_guides(pd.DataFrame(columns=guides.GUIDE_COLUMNS)) == {
        "total": 0, "clientes": [], "keys": []}


def test_loaded_tab_shows_last_month_of_jimena_and_schava_only():
    dataset = guides.build_guides_dataset(sources())
    visible = guides.visible_vendor_guides(guides.recent_guides(dataset, now=NOW))
    assert visible["ID_Pedido"].tolist() == ["PED-D", "PED-A", "PED-B"]
    today = NOW.date()
    assert guides.apply_guides_filters(visible, date_mode="day", day=date(2026, 9, 22), today=today)[
        "ID_Pedido"].tolist() == ["PED-B"]
    assert guides.apply_guides_filters(visible, vendor="SCHAVA", today=today)["ID_Pedido"].tolist() == ["PED-B"]
    # Un rango invertido no filtra fechas, igual que app_v.
    assert len(guides.apply_guides_filters(visible, date_mode="range", start=today, end=date(2026, 9, 1))) == 3



def test_pending_lists_open_requests_without_guide_from_data_pedidos_only():
    pending = guides.build_pending_dataset(sources())
    assert pending["ID_Pedido"].tolist() == ["PED-C", "PED-H"]
    assert pending.set_index("ID_Pedido").loc["PED-H", "Folio_O_ID"] == "F77"
    table = guides.pending_table(pending, now=NOW)
    assert list(table.columns) == list(guides.PENDING_TABLE_COLUMNS.values())
    assert table.to_dict("records") == [
        {"Folio / ID": "PED-C", "Destinatario": "SIN GUIA", "Solicitó": "ARTTD JIMENA", "Estado": "",
         "Solicitada": "24/09/26 10:00", "En espera": "2 h"},
        {"Folio / ID": "F77", "Destinatario": "DE SCHAVA", "Solicitó": "SCHAVA", "Estado": "🟡 Pendiente",
         "Solicitada": "21/09/26 09:30", "En espera": "3 días"},
    ]
    assert guides.own_pending(pending)["ID_Pedido"].tolist() == ["PED-C"]
    assert guides.build_pending_dataset({}).empty


def test_waiting_time_and_closed_statuses():
    assert guides.format_waiting_time(datetime(2026, 9, 24, 11, 35), NOW) == "25 min"
    assert guides.format_waiting_time(datetime(2026, 9, 23, 11, 0), NOW) == "1 día"
    assert guides.format_waiting_time(datetime(2026, 9, 24, 12, 5), NOW) == "0 min"
    assert guides.format_waiting_time(pd.NaT, NOW) == ""
    assert guides.is_closed_status("🟢 Completado") and guides.is_closed_status("🔴 CANCELADO")
    assert not guides.is_closed_status("🟡 Pendiente") and not guides.is_closed_status("🔵 En Proceso")

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
    }}, {{
        "ID_Pedido": "PED-W", "Cliente": "ANA ESPERA", "Vendedor_Registro": "ARTTD JIMENA",
        "Tipo_Envio": "📋 Solicitudes de Guía", "Hora_Registro": "2026-09-24 10:30:00",
        "Fecha_Entrega": "2026-09-24", "id_vendedor": "ARTTDJIM01", "Estado": "🟡 Pendiente",
    }}]),
}}

def fake_register(request):
    st.session_state["registered"] = request
    return request["direccion"]["nombre"].strip(), "PED-TEST"

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
    assert any("tienes 1 solicitud(es) con guía cargada. Destinatarios: JUAN MEJIA." in item.value
               for item in at.info)
    assert any("Tienes 1 solicitud(es) en espera" in item.value for item in at.info)
    assert not any("pedido" in item.value.lower()
                   for item in [*at.markdown, *at.info, *at.caption, *at.subheader])
    assert at.text_input(key="guides_vendedor_0").value == "ARTTD JIMENA"
    assert at.text_input(key="guides_vendedor_0").disabled
    assert at.text_input(key="guides_fecha_0").value == "24/09/2026"
    assert at.text_input(key="guides_pais_0").value == "México"

    at.text_input(key="guides_nombre_0").input("JUAN MEJIA")
    at.button[0].click().run()
    assert "registered" not in at.session_state
    assert any("La solicitud no se registró" in item.value for item in at.warning)

    fill_address(at)
    at.text_input(key="guides_folio_0").input(" F999 ")
    at.text_area(key="guides_comentario_0").input("Enviar hoy")
    at.button[0].click().run()
    assert not at.exception
    request = at.session_state["registered"]
    assert request["folio_factura"] == " F999 " and request["comentario"] == "Enviar hoy"
    assert request["direccion"]["codigo_postal"] == "06000"
    assert str(request["fecha_entrega"]) == "2026-09-24"
    assert any("Solicitud de guía registrada para JUAN MEJIA (ID PED-TEST)." in item.value
               for item in at.success)
    assert any("⏳ En espera" in item.value for item in at.caption)
    # Se vuelven a leer las hojas para que la solicitud nueva ya salga en espera.
    assert guides.GUIDES_REFRESH_TOKEN_KEY in at.session_state
    at.button(key="guides_request_clear").click().run()
    assert not any("Solicitud de guía registrada" in item.value for item in at.success)
    # El formulario se limpia con una nueva versión de claves.
    assert at.text_input(key="guides_nombre_1").value == ""


def test_loaded_tab_lists_the_guide_and_its_link():
    from streamlit.testing.v1 import AppTest

    at = AppTest.from_string(GUIDES_SCRIPT, default_timeout=15)
    at.session_state["guides_primary_tabs_Jime"] = guides.GUIDES_TAB_LOADED
    at.run()
    assert not at.exception
    assert at.selectbox(key="guides_filter_vendor").value == "ARTTD JIMENA"
    assert at.selectbox(key="guides_selected_order").value == "📋 PED-A – JUAN MEJIA – ARTTD JIMENA · 24/09/26"
    table = at.dataframe[0].value
    assert list(table.columns) == list(guides.TABLE_COLUMNS.values())
    assert table["Destinatario"].tolist() == ["JUAN MEJIA"] and table["Registro"].tolist() == ["En curso"]
    texts = [item.value for item in [*at.markdown, *at.info, *at.caption, *at.subheader]]
    assert texts and not any("pedido" in text.lower() for text in texts)



def test_pending_tab_lists_requests_waiting_for_their_guide():
    from streamlit.testing.v1 import AppTest

    at = AppTest.from_string(GUIDES_SCRIPT, default_timeout=15)
    at.session_state["guides_primary_tabs_Jime"] = guides.GUIDES_TAB_PENDING
    at.run()
    assert not at.exception
    assert at.selectbox(key="guides_pending_vendor").value == "ARTTD JIMENA"
    table = at.dataframe[0].value
    assert list(table.columns) == list(guides.PENDING_TABLE_COLUMNS.values())
    assert table["Destinatario"].tolist() == ["ANA ESPERA"] and table["En espera"].tolist() == ["1 h"]
    assert any("En espera: 1" in item.value for item in at.markdown)
    texts = [item.value for item in [*at.markdown, *at.info, *at.caption, *at.subheader]]
    assert not any("pedido" in text.lower() for text in texts)

    at.selectbox(key="guides_pending_vendor").select("SCHAVA").run()
    assert not at.dataframe
    assert any("No hay solicitudes en espera" in item.value for item in at.success)

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

    destinatario, pedido_id = guides.register_guide_request(request)
    assert destinatario == "JUAN MEJIA" and pedido_id.startswith("PED-20260924120000-")
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
