"""Pruebas de la mesa unificada; no acceden a Sheets ni S3."""
from datetime import datetime, timedelta
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import lab_pg as app


NOW = datetime(2026, 9, 3, 12)


def case(identifier="001", status="REVISIÓN DE ARCHIVOS", apparatus="MSE", **extra):
    return {app.ID_COLUMN: identifier, app.APARATO_COLUMN: apparatus, app.STATUS_COLUMN: status,
            "NOMBRE DOCTOR": "Doctora ejemplo", "NOMBRE PACIENTE": "Paciente ejemplo", "PAGO": "SIN PAGO", **extra}


def log(identifier="001", status="REVISIÓN DE ARCHIVOS", hours=2, limit="5", **extra):
    start = NOW - timedelta(hours=hours)
    return {app.ID_COLUMN: identifier, app.STATUS_COLUMN: status, "FECHA_INICIO": start.strftime("%Y-%m-%d"),
            "HORA_INICIO": start.strftime("%H:%M:%S"), "FECHA_FIN": "", "TIEMPO_MAXIMO_HORAS": limit, **extra}


@pytest.fixture(autouse=True)
def fixed_time(monkeypatch):
    monkeypatch.setattr(app, "app_now", lambda: NOW)


@pytest.mark.parametrize("hours,signal", [(3.99,"🟢 En tiempo"), (4,"🟡 Por vencer"), (4.99,"🟡 Por vencer"), (5,"🔴 Atrasado")])
def test_thresholds(hours, signal):
    table = app.build_workbench_table(pd.DataFrame([case()]), pd.DataFrame([log(hours=hours)]))
    assert table.iloc[0]["SEMÁFORO"] == signal


def test_weekend_boundary_uses_exact_minutes():
    assert app.business_hours_elapsed(datetime(2026,9,4,23,30), datetime(2026,9,7,0,30)) == 1
    assert app.business_hours_elapsed(datetime(2026,9,5,12), datetime(2026,9,6,12)) == 0


@pytest.mark.parametrize("limit", ["nan", "inf", "-1", "0"])
def test_invalid_limits_are_gray(limit):
    result = app.build_workbench_table(pd.DataFrame([case()]), pd.DataFrame([log(limit=limit)]))
    assert result.iloc[0]["SEMÁFORO"] == "⚪ Sin medición"


def test_status_alias_does_not_create_false_concurrency_conflict():
    assert app.values_equivalent_for_column("STATUS", "REVISIÓN DE ARCHIVOS", "REVISION DE ARCHIVOS ")


def test_closed_aliases_and_unused_folios_are_hidden():
    df = pd.DataFrame([case("1", "ENVIADO"), case("2", "ENVIO DE ENCUESTA"), case("3", "CANCELO"),
                       case("004", "PRODUCTO ENVIADO"), case("5", "REVISION DE ARCHIVOS "),
                       {app.ID_COLUMN:"006"}])
    result = app.active_workbench_cases(df)
    assert list(result[app.ID_COLUMN]) == ["004", "5"]
    assert result.iloc[1][app.STATUS_COLUMN] == "REVISIÓN DE ARCHIVOS"


@pytest.mark.parametrize("logs,reason", [([],"Sin registro"), ([log(status="EN PLANEACIÓN")],"no corresponde"),
                                       ([log(),log()],"Varios registros"), ([log(limit="")],"Sin tiempo"),
                                       ([log(FECHA_INICIO="invalida")],"Sin fecha")])
def test_missing_or_ambiguous_timing_is_never_green(logs, reason):
    result = app.build_workbench_table(pd.DataFrame([case()]), pd.DataFrame(logs))
    assert result.iloc[0]["SEMÁFORO"] == "⚪ Sin medición"
    assert reason in result.iloc[0]["DETALLE SEMÁFORO"]
    assert len(result) == 1


def test_special_payment_warning_does_not_hide_overdue_stage(monkeypatch):
    monkeypatch.setattr(app, "get_special_payment_sla_alert_state", lambda *args: "Próximo a vencer - Pago planeación")
    result = app.build_workbench_table(pd.DataFrame([case()]), pd.DataFrame([log(hours=8)]))
    assert result.iloc[0]["SEMÁFORO"] == "🔴 Atrasado"
    assert "Pago planeación" in result.iloc[0]["DETALLE SEMÁFORO"]


def test_special_sla_without_log_and_spaced_headers():
    row = case(apparatus="TIGER", **{"FECHA PAGO PLANEACION ": "20 agosto"})
    result = app.build_workbench_table(pd.DataFrame([row]), pd.DataFrame())
    assert result.iloc[0]["SEMÁFORO"] == "🔴 Atrasado"
    assert "Sin registro" in result.iloc[0]["DETALLE SEMÁFORO"]


def test_compare_by_folio_not_visual_position():
    original = pd.DataFrame([case("001"),case("002")])
    edited = original.iloc[::-1].copy()
    edited.loc[edited[app.ID_COLUMN] == "001", "NOMBRE DOCTOR"] = "Nombre corregido"
    assert app.workbench_changes(original,edited) == [("001", {"NOMBRE DOCTOR":"Nombre corregido"})]


@pytest.mark.parametrize("user,column,value", [("Lesly","PAGO","TOTAL"), ("Vero","NOMBRE DOCTOR","Otro"),
                                             ("Jime",app.APARATO_COLUMN,"TIGER")])
def test_readonly_columns_are_enforced_server_side(user,column,value):
    original = pd.DataFrame([case()])
    errors = app.validate_workbench_changes(original, original, [("001",{column:value})],user)
    assert errors and "no puede editar" in errors[0]


def test_skip_stage_and_foreign_role_rejected():
    original = pd.DataFrame([case()])
    assert app.validate_workbench_changes(original,original,[("001",{"STATUS":"PRODUCTO ENVIADO"})],"Admin")
    assert app.validate_workbench_changes(original,original,[("001",{"STATUS":"ESCANEO MAL (EN REPETICIÓN)"})],"Vero")
    assert not app.validate_workbench_changes(original,original,[("001",{"STATUS":"ESCANEO MAL (EN REPETICIÓN)"})],"Jime")


def test_lesly_cannot_advance_before_printing():
    original = pd.DataFrame([case(status="LISTO P/SINTERIZADO")])
    delta = [("001", {"STATUS":"ELABORACIÓN PLATINA"})]
    assert "impresión" in app.validate_workbench_changes(original,original,delta,"Lesly")[0]
    original[app.ESTATUS_PRINT_DATE_COLUMN] = "2026-09-03"
    assert not app.validate_workbench_changes(original,original,delta,"Lesly")


def test_payment_blocks_advancement_without_authorization(monkeypatch):
    original = pd.DataFrame([case(status="PAGO PLANEACIÓN")])
    delta = [("001", {"STATUS":"EN PLANEACIÓN"})]
    monkeypatch.setattr(app,"get_active_tiempo_row",lambda _: (2,{"PUEDE_AVANZAR":"No"}))
    assert "pago" in app.validate_workbench_changes(original,original,delta,"Jime")[0]
    monkeypatch.setattr(app,"get_active_tiempo_row",lambda _: (2,{"PUEDE_AVANZAR":"Sí"}))
    assert not app.validate_workbench_changes(original,original,delta,"Jime")


def test_concurrent_status_change_blocks_even_metadata_edit():
    original = pd.DataFrame([case()])
    fresh = pd.DataFrame([case(status="EN PLANEACIÓN")])
    errors = app.validate_workbench_changes(original,fresh,[("001",{"NOMBRE DOCTOR":"Otro"})],"Admin")
    assert errors and "otro usuario" in errors[0]


def test_batch_preflight_does_not_write_any_row_on_error(monkeypatch):
    original = pd.DataFrame([case("001"),case("002")])
    edited = original.copy()
    edited.loc[0,"NOMBRE DOCTOR"] = "Corregido"
    edited.loc[1,"STATUS"] = "PRODUCTO ENVIADO"
    monkeypatch.setattr(app,"clear_sheet_data_cache",lambda: None)
    monkeypatch.setattr(app,"read_sheet_df",lambda _: original)
    def forbidden(*args, **kwargs):
        pytest.fail("No debe escribir un lote que no supera prevalidación")
    monkeypatch.setattr(app,"update_row_by_columna_1",forbidden)
    monkeypatch.setattr(app,"advance_case_status",forbidden)
    saved, errors = app.save_workbench_changes(original,edited,"Admin")
    assert not saved and errors


def test_metadata_save_only_changes_requested_cell(monkeypatch):
    original = pd.DataFrame([case()])
    edited = original.copy()
    edited.loc[0,"NOMBRE DOCTOR"] = "Corregido"
    calls=[]
    monkeypatch.setattr(app,"clear_sheet_data_cache",lambda: None)
    monkeypatch.setattr(app,"reset_workbench",lambda: None)
    monkeypatch.setattr(app,"read_sheet_df",lambda _: original)
    def save(identifier, changes, **kwargs):
        calls.append((identifier,changes,kwargs))
        return {"success":True,"skipped_columns":[],"error":""}
    monkeypatch.setattr(app,"update_row_by_columna_1",save)
    assert app.save_workbench_changes(original,edited,"Admin") == (["001"],[])
    assert calls[0][1] == {"NOMBRE DOCTOR":"Corregido"}
    assert app.STATUS_COLUMN in calls[0][2]["expected_values"]


@pytest.mark.parametrize("live_status,allowed",[("REVISION DE ARCHIVOS ",True),("EN PLANEACIÓN",False)])
def test_final_write_checks_fresh_values_and_accepts_aliases(monkeypatch,live_status,allowed):
    writes=[]
    class Sheet:
        def get_all_values(self):
            return [["pEDR"], [app.ID_COLUMN,"APARATO","STATUS","NOMBRE DOCTOR"],
                    ["001","MSE",live_status,"Original"]]
        def update_cells(self,cells,**kwargs): writes.extend(cells)
    monkeypatch.setattr(app,"get_worksheet",lambda _: Sheet())
    monkeypatch.setattr(app,"apply_estatus_row_styles",lambda *args: None)
    result=app.update_row_by_columna_1("001",{"NOMBRE DOCTOR":"Corregido"},
                                    expected_values={"STATUS":"REVISIÓN DE ARCHIVOS","NOMBRE DOCTOR":"Original"})
    assert result["success"] is allowed
    if allowed:
        assert [(cell.row,cell.col,cell.value) for cell in writes] == [(3,4,"Corregido")]
    else:
        assert not writes and "Otro usuario" in result["error"]


def test_new_log_respects_actual_header_order(monkeypatch):
    headers = ["ID_LOG",app.ID_COLUMN,"STATUS","FECHA_FIN","FECHA_INICIO","TIEMPO_MAXIMO_HORAS","PUEDE_AVANZAR","PUEDE_AVANZAR"]
    saved=[]
    class Sheet:
        def row_values(self, _): return headers
        def append_row(self, row, **kwargs): saved.append(row)
    monkeypatch.setattr(app,"ensure_tiempos_headers",lambda: None)
    monkeypatch.setattr(app,"close_previous_active_time",lambda _: True)
    monkeypatch.setattr(app,"clear_sheet_data_cache",lambda: None)
    monkeypatch.setattr(app,"read_sheet_df",lambda _: pd.DataFrame())
    monkeypatch.setattr(app,"get_worksheet",lambda _: Sheet())
    monkeypatch.setattr(app,"get_current_user",lambda: "Jime")
    app.register_status_change(identifier="001",apparatus="MSE",previous_status="ORDEN RECIBIDA",new_status="REVISIÓN DE ARCHIVOS")
    assert saved[0][3] == ""
    assert saved[0][4] == "2026-09-03"
    assert saved[0][5] == "5"
    assert saved[0][6] == saved[0][7]


@pytest.mark.parametrize("user",["Admin","Jime","Lesly","Vero"])
def test_unified_ui_renders_without_tabs(user):
    from streamlit.testing.v1 import AppTest
    script = f'''
import sys
sys.path.insert(0, {str(Path(__file__).resolve().parents[1])!r})
import lab_pg as app
import pandas as pd
app.require_authenticated_user = lambda: {user!r}
app.ensure_tiempos_headers = lambda: None
app.read_sheet_df = lambda name: pd.DataFrame([{case()!r}]) if name == app.SHEET_ESTATUS else pd.DataFrame()
app.main()
'''
    at = AppTest.from_string(script, default_timeout=15).run()
    assert not at.exception
    assert len(at.dataframe) == 1
    assert not at.tabs
    assert not at.get("segmented_control")
    assert at.metric[0].value == "1"
    at.text_input(key="workbench_search").set_value("no existe").run()
    assert not at.exception
    assert len(at.dataframe) == 0


def test_empty_workbench_renders():
    from streamlit.testing.v1 import AppTest
    script = f'''
import sys
sys.path.insert(0, {str(Path(__file__).resolve().parents[1])!r})
import lab_pg as app
import pandas as pd
app.require_authenticated_user = lambda: "Admin"
app.ensure_tiempos_headers = lambda: None
app.read_sheet_df = lambda name: pd.DataFrame()
app.main()
'''
    at = AppTest.from_string(script).run()
    assert not at.exception
    assert at.metric[0].value == "0"


def test_ui_edits_freeze_filters_and_save_to_same_folio():
    from streamlit.testing.v1 import AppTest
    script = f'''
import sys
sys.path.insert(0, {str(Path(__file__).resolve().parents[1])!r})
import lab_pg as app
import pandas as pd
import streamlit as st
if "demo_rows" not in st.session_state:
    st.session_state.demo_rows = [{case()!r}]
app.require_authenticated_user = lambda: "Admin"
app.ensure_tiempos_headers = lambda: None
app.clear_sheet_data_cache = lambda: None
app.read_sheet_df = lambda name: pd.DataFrame(st.session_state.demo_rows) if name == app.SHEET_ESTATUS else pd.DataFrame()
def update(identifier, changes, **kwargs):
    for row in st.session_state.demo_rows:
        if row[app.ID_COLUMN] == identifier:
            row.update(changes)
    return {{"success":True,"updated_columns":list(changes),"skipped_columns":[],"error":""}}
app.update_row_by_columna_1 = update
app.main()
'''
    at = AppTest.from_string(script, default_timeout=15).run()
    key = at.session_state["workbench_editor_key"]
    delta = {"edited_rows": {0:{"NOMBRE DOCTOR":"Nombre corregido"}},"added_rows":[],"deleted_rows":[]}
    at.session_state[key] = delta
    at.run()
    assert not at.exception
    assert at.text_input(key="workbench_search").disabled
    assert next(button for button in at.button if button.label == "Actualizar datos").disabled
    next(button for button in at.button if button.label == "Guardar cambios").click()
    # AppTest no serializa automáticamente los cambios de data_editor.
    at.session_state[key] = delta
    at.run()
    assert not at.exception
    assert at.session_state["demo_rows"][0][app.ID_COLUMN] == "001"
    assert at.session_state["demo_rows"][0]["NOMBRE DOCTOR"] == "Nombre corregido"
    assert not at.text_input(key="workbench_search").disabled
    assert any("Guardado: 001" in message.value for message in at.success)


@pytest.mark.parametrize("user,status",[("Admin","EN PLANEACIÓN"),("Jime","PAGO PLANEACIÓN"),
                                       ("Lesly","ELABORACIÓN PLATINA"),("Vero","LISTO P/CONFECCIÓN")])
def test_selected_case_actions_stay_on_same_page(user,status):
    from streamlit.testing.v1 import AppTest
    sample = case(status=status)
    timing = log(status=status, PUEDE_AVANZAR="Sí", PAGO_REQUERIDO="Sí", TIPO_PAGO_REQUERIDO="Planeación")
    script = f'''
import sys
sys.path.insert(0, {str(Path(__file__).resolve().parents[1])!r})
import lab_pg as app
import pandas as pd
import streamlit as st
app.require_authenticated_user = lambda: {user!r}
app.ensure_tiempos_headers = lambda: None
app.read_sheet_df = lambda name: pd.DataFrame([{sample!r}]) if name == app.SHEET_ESTATUS else pd.DataFrame([{timing!r}])
app.get_latest_estefano_files = lambda _: ""
app.get_active_tiempo_row = lambda _: (2,{timing!r})
st.session_state["status_change_success_message"] = "Prueba de confirmación anterior"
app.main()
'''
    at = AppTest.from_string(script, default_timeout=15).run()
    key = at.session_state["workbench_editor_key"]
    at.session_state[key] = {"edited_rows":{0:{"SELECCIONAR":True}},"added_rows":[],"deleted_rows":[]}
    at.run()
    assert not at.exception
    assert not at.tabs
    assert at.text_input(key="workbench_search").disabled is False
    assert any("Pedido 001" in item.value for item in at.markdown)
