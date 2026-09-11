"""Pruebas de la mesa unificada; no acceden a Sheets ni S3."""
from datetime import datetime, timedelta
import sys
from pathlib import Path
import json

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


def grid_event(at, edits):
    rows = at.session_state["workbench_editor_baseline"].to_dict("records")
    for row in rows:
        row.update(edits.get(row[app.ID_COLUMN], {}))
    return {"rows": rows}


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


def test_visual_stage_labels_roundtrip_without_false_changes():
    for index, status in enumerate(app.STATUS_DISPLAY):
        original = pd.DataFrame([case(str(index),status=status)])
        displayed = app.workbench_display_df(original)
        assert displayed.iloc[0]["STATUS"] == app.display_selectbox_value("STATUS",status)
        assert app.workbench_changes(original,displayed) == []
        assert app.workbench_cell_value("STATUS",displayed.iloc[0]["STATUS"]) == app.normalize_status_alias(status)
    original = pd.DataFrame([case()])
    displayed = app.workbench_display_df(original)
    displayed.loc[0,"STATUS"] = "🔴 ESCANEO MAL (EN REPETICIÓN)"
    assert app.workbench_changes(original,displayed) == [("001", {"STATUS":"ESCANEO MAL (EN REPETICIÓN)"})]


def test_labeled_metadata_is_saved_without_emojis():
    original = pd.DataFrame([case(**{"SERVICIO":"CONFECCIÓN","VENDEDOR":"JIMENA","ARCHIVOS RECIBIDOS":"STL"})])
    displayed = app.workbench_display_df(original)
    displayed.loc[0,"SERVICIO"] = "🔵 PLANEACIÓN & CONFECCIÓN"
    displayed.loc[0,"VENDEDOR"] = "👨 JUAN"
    displayed.loc[0,"ARCHIVOS RECIBIDOS"] = "📁🩻 STL+TOMO"
    assert app.workbench_changes(original,displayed) == [("001",{
        "SERVICIO":"PLANEACIÓN & CONFECCIÓN","VENDEDOR":"JUAN","ARCHIVOS RECIBIDOS":"STL+TOMO"})]


def test_status_options_are_specific_to_each_saved_stage_and_apparatus():
    source = pd.DataFrame([
        case("001", status="REVISIÓN DE ARCHIVOS", apparatus="MSE"),
        case("002", status="LISTO P/CONFECCIÓN", apparatus="HYRAX"),
    ])
    grid = app.workbench_display_df(source)
    grid.insert(0, "SELECCIONAR", False)
    options = app.workbench_grid_options(grid, source, "Admin")["context"]["stageOptions"]
    assert options["001"] == [
        app.display_selectbox_value(app.STATUS_COLUMN, value)
        for value in app.get_allowed_next_statuses("MSE", "REVISIÓN DE ARCHIVOS")
    ]
    assert options["002"] == [
        app.display_selectbox_value(app.STATUS_COLUMN, value)
        for value in app.get_allowed_next_statuses("HYRAX", "LISTO P/CONFECCIÓN")
    ]
    assert app.display_selectbox_value(app.STATUS_COLUMN, "PULIDO (EN CONFECCIÓN)") not in options["001"]


def test_status_options_also_apply_user_and_printing_permissions():
    jime_row = pd.Series(case(status="REVISIÓN DE ARCHIVOS"))
    assert app.display_selectbox_value(app.STATUS_COLUMN, "CANCELO") not in app.workbench_stage_options(jime_row, "Jime")
    lesly_row = pd.Series(case(status="LISTO P/SINTERIZADO"))
    assert app.workbench_stage_options(lesly_row, "Lesly") == [
        app.display_selectbox_value(app.STATUS_COLUMN, "LISTO P/SINTERIZADO")
    ]
    lesly_row[app.ESTATUS_PRINT_DATE_COLUMN] = "2026/09/03"
    assert app.display_selectbox_value(app.STATUS_COLUMN, "ELABORACIÓN PLATINA") in app.workbench_stage_options(lesly_row, "Lesly")


@pytest.mark.parametrize("user", ["Admin", "Jime", "Lesly", "Vero"])
def test_editable_dates_use_calendar_and_now_shortcut_for_every_user(user):
    source = pd.DataFrame([case(**{
        "FECHA DE RECEPCIÓN": "3 septiembre 2026",
        "FECHA/HORA ENVÍO STEFANO": "3 septiembre 2026 14:25",
    })])
    grid = app.workbench_display_df(source)
    grid.insert(0, "SELECCIONAR", False)
    options = app.workbench_grid_options(grid, source, user)
    columns = {column["field"]: column for column in options["columnDefs"]}
    date_config = columns["FECHA DE RECEPCIÓN"]
    datetime_config = columns["FECHA/HORA ENVÍO STEFANO"]
    assert date_config["cellEditorParams"]["withTime"] is False
    assert date_config["cellEditorParams"]["timeZone"] == "America/Mexico_City"
    assert date_config["cellEditorParams"]["initialValues"]["3 septiembre 2026"] == "2026-09-03"
    assert "Ahora" in date_config["cellEditor"].js_code
    assert 'addInput("date", "Fecha"' in date_config["cellEditor"].js_code
    assert 'if (this.withTime) this.time = addInput("time", "Hora"' in date_config["cellEditor"].js_code
    assert datetime_config["editable"] is True
    assert datetime_config["cellEditorParams"]["withTime"] is True


def test_status_menu_and_columns_expose_visual_categories():
    source = pd.DataFrame([case(**{
        "NOMBRE PACIENTE": "Paciente", "FECHA DE RECEPCIÓN": "2026/09/03",
        "SEMÁFORO": "⚪ Sin medición", "RESPONSABLE": "JIME",
    })])
    grid = app.workbench_display_df(source)
    grid.insert(0, "SELECCIONAR", False)
    options = app.workbench_grid_options(
        grid, source, "Admin", preferred_order=["NOMBRE PACIENTE", app.APARATO_COLUMN],
        hidden_columns={"RESPONSABLE"},
    )
    columns = {column["field"]: column for column in options["columnDefs"]}
    order = [column["field"] for column in options["columnDefs"]]
    assert order[:5] == ["SELECCIONAR", app.ID_COLUMN, "SEMÁFORO", "NOMBRE PACIENTE", app.APARATO_COLUMN]
    assert columns[app.ID_COLUMN]["suppressMovable"] is True
    assert columns["NOMBRE PACIENTE"]["headerClass"] == "lab-header-editable"
    assert columns["PAGO"]["headerClass"] == "lab-header-editable"
    assert columns["RESPONSABLE"]["headerClass"] == "lab-header-automatic"
    assert columns["RESPONSABLE"]["hide"] is True
    assert "WorkbenchStatusEditor" in columns[app.STATUS_COLUMN]["cellEditor"].js_code
    assert "backgroundColor" in columns[app.STATUS_COLUMN]["cellEditor"].js_code


def test_column_order_is_normalized_and_persisted_per_user(monkeypatch):
    columns = ["SELECCIONAR", app.ID_COLUMN, "SEMÁFORO", "APARATO", "STATUS", "NOMBRE DOCTOR"]
    assert app.normalize_column_order(["STATUS", "STATUS", app.ID_COLUMN, "OTRA"], columns) == [
        "STATUS", "APARATO", "NOMBRE DOCTOR"
    ]
    updated = []
    class Worksheet:
        def get_all_values(self):
            return [app.USER_PREFERENCE_HEADERS, ["Jime", '["STATUS", "APARATO"]', "2026/09/03 10:00"]]
        def update_cells(self, cells, **kwargs):
            updated.extend(cells)
        def append_row(self, row, **kwargs):
            pytest.fail("Jime ya debe actualizar su fila")
    monkeypatch.setattr(app, "get_user_preferences_worksheet", lambda: Worksheet())
    assert app.read_user_column_order("Jime", columns) == ["STATUS", "APARATO", "NOMBRE DOCTOR"]
    ok, _ = app.save_user_column_order("Jime", ["NOMBRE DOCTOR", "STATUS"], columns)
    assert ok
    assert [cell.value for cell in updated][:2] == ["Jime", '["NOMBRE DOCTOR", "STATUS", "APARATO"]']


def test_passwords_are_loaded_only_from_secrets_for_allowed_users():
    assert app.APP_USERS == ("Admin", "Jime", "Lesly", "Vero")
    assert "Estefano" not in app.USER_VISIBLE_TABS
    assert "Estefano" not in app.USER_ALLOWED_TRANSITIONS
    assert app.get_user_passwords({}) == {}
    configured = app.get_user_passwords({
        "auth": {"passwords": {"Admin": "admin-hash", "Jime": "jime-hash", "Estefano": "omitido"}},
        "user_passwords": {"Lesly": "lesly-hash", "Vero": "vero-hash", "Otro": "omitido"},
    })
    assert configured == {
        "Admin": "admin-hash", "Jime": "jime-hash", "Lesly": "lesly-hash", "Vero": "vero-hash"
    }
    assert app.missing_user_passwords(configured) == []
    assert app.missing_user_passwords({"Jime": "hash"}) == ["Admin", "Lesly", "Vero"]


def test_pbkdf2_password_verification_remains_supported():
    test_hash = "pbkdf2_sha256$1$salt$8fa3d7592a7aedf30af5a656525188ac17c254fbc8cab1c56610278639fb64e2"
    assert app.password_matches("prueba", test_hash)
    assert not app.password_matches("incorrecta", test_hash)
    assert app.password_matches("configurado", "configurado")


@pytest.mark.parametrize("user,column,value", [("Lesly", app.ID_COLUMN, "002"),
                                             ("Vero", "SEMÁFORO", "🔴 Atrasado"),
                                             ("Jime", "RESPONSABLE", "OTRO"),
                                             ("Admin", "HORAS EN ETAPA", "1")])
def test_fixed_and_computed_columns_are_enforced_server_side(user,column,value):
    original = pd.DataFrame([case()])
    errors = app.validate_workbench_changes(original, original, [("001",{column:value})],user)
    assert errors and "no puede editar" in errors[0]


@pytest.mark.parametrize("user", ["Admin", "Jime", "Lesly", "Vero"])
def test_every_user_can_edit_manual_fields(user):
    original = pd.DataFrame([case(**{"FECHA DE RECEPCIÓN": "2026/09/03"})])
    changes = [("001", {"PAGO": "TOTAL", "FECHA DE RECEPCIÓN": "2026/09/04"})]
    assert not app.validate_workbench_changes(original, original, changes, user)


@pytest.mark.parametrize("user", ["Admin", "Jime", "Lesly", "Vero"])
def test_every_user_can_correct_sheet_columns_including_automatic_fields(user):
    original = pd.DataFrame([case(**{"FECHA PARA ENTREGA": "2026/09/20"})])
    changes = [("001", {
        "NOMBRE DOCTOR": "Otro",
        app.APARATO_COLUMN: "TIGER",
        "FECHA PARA ENTREGA": "2026/09/21",
    })]
    assert not app.validate_workbench_changes(original, original, changes, user)


def test_jime_owns_planning_and_is_default_responsible_filter():
    for status in app.PLANNING_STATUSES:
        assert app.get_process_responsible(status) == "JIME"
        assert app.get_status_tab_owner(status) == "Jime"
    owners = ["JIME", "LESLY", "VERO"]
    assert app.workbench_default_owner("Jime", owners) == "JIME"
    assert app.workbench_default_owner("Lesly", owners) == "LESLY"
    assert app.workbench_default_owner("Admin", owners) == "Todos"


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
    errors = app.validate_workbench_changes(original,fresh,[("001",{"PAGO":"TOTAL"})],"Admin")
    assert errors and "otro usuario" in errors[0]


def test_batch_preflight_does_not_write_any_row_on_error(monkeypatch):
    original = pd.DataFrame([case("001"),case("002")])
    edited = original.copy()
    edited.loc[0,"PAGO"] = "TOTAL"
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
    edited.loc[0,"PAGO"] = "TOTAL"
    calls=[]
    monkeypatch.setattr(app,"clear_sheet_data_cache",lambda: None)
    monkeypatch.setattr(app,"reset_workbench",lambda: None)
    monkeypatch.setattr(app,"read_sheet_df",lambda _: original)
    def save(identifier, changes, **kwargs):
        calls.append((identifier,changes,kwargs))
        return {"success":True,"skipped_columns":[],"error":""}
    monkeypatch.setattr(app,"update_row_by_columna_1",save)
    assert app.save_workbench_changes(original,edited,"Admin") == (["001"],[])
    assert calls[0][1] == {"PAGO":"TOTAL"}
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
    headers = ["ID_LOG",app.ID_COLUMN,"STATUS","FECHA_FIN","FECHA_INICIO","TIEMPO_MAXIMO_HORAS",
               "USUARIO","PUEDE_AVANZAR","PUEDE_AVANZAR"]
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
    assert saved[0][6] == "Jime"
    assert saved[0][7] == saved[0][8]


@pytest.mark.parametrize("user",["Admin","Jime","Lesly","Vero"])
def test_followup_tabs_keep_one_operational_table(user):
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
    assert len(at.tabs[0].get("component_instance")) == 1
    expected_tabs = {
        "Admin": ["📋 Seguimiento", "➕ Nuevo pedido", "📨 Respuestas de Forms", "⚙️ Procesos y plazos"],
        "Jime": ["📋 Seguimiento", "➕ Nuevo pedido", "📨 Respuestas de Forms"],
        "Lesly": ["📋 Seguimiento"], "Vero": ["📋 Seguimiento"],
    }
    assert [tab.label for tab in at.tabs] == expected_tabs[user]
    assert not at.toggle
    assert not at.get("segmented_control")
    assert at.checkbox(key="workbench_hide_automatic").label == "Ocultar automáticas"
    assert at.selectbox(key="workbench_owner").value == ("JIME" if user == "Jime" else "Todos")
    assert any("Fijas: Folio y Semáforo" in item.value for item in at.tabs[0].markdown)
    assert any("Editable automática" in item.value for item in at.tabs[0].markdown)
    assert at.metric[0].value == "1"
    at.text_input(key="workbench_search").set_value("no existe").run()
    assert not at.exception
    assert len(at.tabs[0].get("component_instance")) == 0


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
    delta = grid_event(at, {"001": {"PAGO":"🟢 TOTAL"}})
    at.session_state[key] = delta
    at.run()
    assert not at.exception
    assert at.text_input(key="workbench_search").disabled
    assert next(button for button in at.button if button.label == "Actualizar datos").disabled
    next(button for button in at.button if button.label == "Guardar cambios").click()
    # Simula el mensaje del componente sin conectar a Sheets/S3.
    at.session_state[key] = delta
    at.run()
    assert not at.exception
    assert at.session_state["demo_rows"][0][app.ID_COLUMN] == "001"
    assert at.session_state["demo_rows"][0]["PAGO"] == "TOTAL"
    assert not at.text_input(key="workbench_search").disabled
    assert any("Guardado: 001" in message.value for message in at.get("toast"))


def test_editing_stage_keeps_editor_identity_and_revert_unlocks_filters():
    from streamlit.testing.v1 import AppTest
    script = f'''
import sys
sys.path.insert(0, {str(Path(__file__).resolve().parents[1])!r})
import lab_pg as app
import pandas as pd
app.require_authenticated_user = lambda: "Admin"
app.ensure_tiempos_headers = lambda: None
app.read_sheet_df = lambda name: pd.DataFrame([{case()!r}]) if name == app.SHEET_ESTATUS else pd.DataFrame()
app.main()
'''
    at = AppTest.from_string(script,default_timeout=15).run()
    initial_id = at.tabs[0].get("component_instance")[0].proto.id
    initial_height = json.loads(at.tabs[0].get("component_instance")[0].proto.json_args)["height"]
    key = at.session_state["workbench_editor_key"]
    for status, pending in [("🔴 ESCANEO MAL (EN REPETICIÓN)", True), ("🔵 REVISIÓN DE ARCHIVOS", False)]:
        at.session_state[key] = grid_event(at, {"001": {"STATUS": status}})
        at.run()
        assert not at.exception
        assert at.session_state["workbench_editor_key"] == key
        assert at.tabs[0].get("component_instance")[0].proto.id == initial_id
        assert json.loads(at.tabs[0].get("component_instance")[0].proto.json_args)["height"] == initial_height
        # La cuadrícula conserva identidad y altura; AppTest representa el valor del
        # componente como un nodo adicional sólo antes de su primer evento.
        assert at.text_input(key="workbench_search").disabled == pending
        bars = [item.value for item in at.tabs[0].markdown if 'class="lab-edit-bar' in item.value]
        assert len(bars) == 1
        assert not any("Tienes celdas editadas" in item.value for item in at.info)
        if pending:
            assert not at.tabs[1].get("form")


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
    at.session_state[key] = grid_event(at, {"001": {"SELECCIONAR": True}})
    at.run()
    assert not at.exception
    assert at.tabs[0].label == "📋 Seguimiento"
    assert at.text_input(key="workbench_search").disabled is False
    assert any("Pedido 001" in item.value for item in at.markdown)
