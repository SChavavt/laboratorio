"""Pruebas de la mesa unificada; no acceden a Sheets ni S3."""
from datetime import datetime, timedelta
import sys
from pathlib import Path
import json

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import lab_pg as app
from workbench_grid import build_grid_options


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


def test_legacy_numeric_log_matches_folio_with_zero():
    result = app.build_workbench_table(pd.DataFrame([case(identifier='012345')]),
                                      pd.DataFrame([log(identifier='12345')]))
    assert result.iloc[0]['SEMÁFORO'] == '🟢 En tiempo'
    assert result.iloc[0]['HORAS EN ETAPA'] == 2


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
    assert list(result[app.ID_COLUMN]) == ["5"]
    assert result.iloc[0][app.STATUS_COLUMN] == "REVISIÓN DE ARCHIVOS"


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
    original = pd.DataFrame([case(**{"SERVICIO":"CONFECCIÓN","VENDEDOR":"ALEJANDRO","ARCHIVOS RECIBIDOS":"STL"})])
    displayed = app.workbench_display_df(original)
    displayed.loc[0,"SERVICIO"] = "🔵 PLANEACIÓN & CONFECCIÓN"
    displayed.loc[0,"VENDEDOR"] = "👨 SANTIAGO"
    displayed.loc[0,"ARCHIVOS RECIBIDOS"] = "📁🩻 STL+TOMO"
    assert app.workbench_changes(original,displayed) == [("001",{
        "SERVICIO":"PLANEACIÓN & CONFECCIÓN","VENDEDOR":"SANTIAGO","ARCHIVOS RECIBIDOS":"STL+TOMO"})]


def test_vendor_catalog_contains_only_active_sellers():
    assert app.VENDEDOR_OPTIONS == [
        "ALEJANDRO",
        "ANA KAREN",
        "BLANCA",
        "CASSANDRA",
        "DANIELA",
        "SOFI",
        "MICHELLE",
        "KAREN JACQUIE",
        "NORMA",
        "PAULINA",
        "SANTIAGO",
        "JIME",
        "LESLY",
    ]
    assert list(app.VENDEDOR_DISPLAY) == app.VENDEDOR_OPTIONS


def test_sheet_vendor_list_also_offers_jime_and_lesly(monkeypatch):
    app.st.session_state.pop("apparatus_form_catalog", None)
    monkeypatch.setattr(app, "read_workbench_form_catalog", lambda _: {"VENDEDOR": ["NORMA", "LESLY"]})
    monkeypatch.setattr(app.st, "secrets", {"gsheets": {"sheet_id": "demo"}})
    try:
        assert app.get_workbench_form_catalog()["VENDEDOR"] == ["NORMA", "LESLY", "JIME"]
    finally:
        app.st.session_state.pop("apparatus_form_catalog", None)


def test_stefano_selected_day_survives_stage_autofill(monkeypatch):
    clock = datetime(2026, 10, 7, 11, 45)
    monkeypatch.setattr(app, 'app_now', lambda: clock)
    writes = []
    monkeypatch.setattr(app, 'update_row_by_columna_1',
                        lambda identifier, changes, **kwargs: writes.append(changes) or
                        {'success': True, 'skipped_columns': []})
    monkeypatch.setattr(app, 'register_status_change', lambda **kwargs: None)
    monkeypatch.setattr(app, 'clear_sheet_data_cache', lambda: None)
    monkeypatch.setattr(app, 'reset_workbench', lambda: None)
    assert app.advance_case_status(
        identifier='001', row=pd.Series(case(status='EN PLANEACIÓN')),
        new_status='REVISIÓN DISEÑO DOCTOR', current_user='Jime',
        extra_changes={'FECHA/HORA ENVÍO STEFANO': '1 septiembre 2026 09:15'},
    )
    assert app.parse_spanish_datetime(writes[0]['FECHA/HORA ENVÍO STEFANO']) == datetime(2026, 9, 1, 11, 45)


def test_outsourced_work_datetime_labels_reference_stefano():
    assert app.display_field_label("FECHA/HORA ENVÍO STEFANO") == "🚚 FECHA/HORA ENVÍO STEFANO"
    assert app.display_field_label("FECHA/HORA ENTREGA STEFANO") == "📬 FECHA/HORA ENTREGA STEFANO"

    grid = pd.DataFrame(columns=["FECHA/HORA ENVÍO STEFANO", "FECHA/HORA ENTREGA STEFANO"])
    options = build_grid_options(
        grid,
        editable=set(),
        automatic=set(),
        stage_options={},
        select_options={},
        date_values={},
        datetime_columns=set(),
        palettes={},
        time_zone="UTC",
    )
    headers = {column["field"]: column["headerName"] for column in options["columnDefs"]}
    assert headers == {
        "FECHA/HORA ENVÍO STEFANO": "Fecha/hora envío Stefano",
        "FECHA/HORA ENTREGA STEFANO": "Fecha/hora entrega Stefano",
    }


def test_forms_file_links_are_split_deduplicated_and_kept_in_order():
    first = "https://drive.google.com/open?id=primero"
    second = "https://drive.google.com/open?id=segundo"

    assert app.get_forms_file_links(f"{first}, {second}\\n{first}") == [first, second]


def test_forms_file_links_ignore_empty_or_invalid_values():
    assert app.get_forms_file_links("") == []
    assert app.get_forms_file_links("archivo sin enlace") == []


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
        for value in app.get_allowed_next_statuses("MSE", "REVISIÓN DE ARCHIVOS", "Admin")
    ]
    assert options["002"] == [
        app.display_selectbox_value(app.STATUS_COLUMN, value)
        for value in app.get_allowed_next_statuses("HYRAX", "LISTO P/CONFECCIÓN", "Admin")
    ]
    # Sólo se ofrecen etapas del flujo de cada aparato.
    assert app.display_selectbox_value(app.STATUS_COLUMN, "SOLICITUD GUÍA PSM + PSM") not in options["001"]
    assert app.display_selectbox_value(app.STATUS_COLUMN, "REVISIÓN DISEÑO DOCTOR") not in options["002"]


@pytest.mark.parametrize("user", ["Admin", "Jime", "Lesly"])
def test_admin_jime_and_lesly_see_flow_targets_and_planning_shortcut(user):
    row = pd.Series(case(status="REVISIÓN DE ARCHIVOS"))
    options = app.workbench_stage_options(row, user)
    assert options[0] == app.display_selectbox_value(app.STATUS_COLUMN, "REVISIÓN DE ARCHIVOS")
    for status in ("ESCANEO MAL (EN REPETICIÓN)", "REALIZAR SOLICITUD PAGO PLANEACIÓN", "EN PLANEACIÓN", "CANCELO", app.PAUSED_STATUS):
        assert app.display_selectbox_value(app.STATUS_COLUMN, status) in options
    for status in ("PAGO PLANEACIÓN", "ORDEN RECIBIDA", "LISTO P/SINTERIZADO"):
        assert app.display_selectbox_value(app.STATUS_COLUMN, status) not in options
    original = pd.DataFrame([case()])
    assert not app.validate_workbench_changes(original, original, [("001", {"STATUS": "EN PLANEACIÓN"})], user)
    assert not app.validate_workbench_changes(original, original, [("001", {"STATUS": "ESCANEO MAL (EN REPETICIÓN)"})], user)
    valid, _ = app.validate_status_change(identifier="001", apparatus="MSE",
        previous_status="REVISIÓN DE ARCHIVOS", new_status="EN PLANEACIÓN", current_user=user)
    assert valid


def test_vero_keeps_the_step_by_step_flow():
    row = pd.Series(case(status="REVISIÓN DE ARCHIVOS"))
    assert app.display_selectbox_value(app.STATUS_COLUMN, "EN PLANEACIÓN") not in app.workbench_stage_options(row, "Vero")
    original = pd.DataFrame([case()])
    assert app.validate_workbench_changes(original, original, [("001", {"STATUS": "EN PLANEACIÓN"})], "Vero")


@pytest.mark.parametrize('user', ['Admin', 'Jime', 'Lesly'])
def test_manual_stage_correction_is_explicit_and_remains_in_the_apparatus_flow(user):
    source = pd.DataFrame([case()])
    backwards = [('001', {'STATUS': 'ORDEN RECIBIDA'})]
    assert app.validate_workbench_changes(source, source, backwards, user)
    assert not app.validate_workbench_changes(source, source, backwards, user, manual_stage=True)
    assert not app.validate_status_change(identifier='001', apparatus='MSE', previous_status='REVISIÓN DE ARCHIVOS',
        new_status='ORDEN RECIBIDA', current_user=user)[0]
    assert app.validate_status_change(identifier='001', apparatus='MSE', previous_status='REVISIÓN DE ARCHIVOS',
        new_status='ORDEN RECIBIDA', current_user=user, manual_stage=True)[0]
    foreign = [('001', {'STATUS': 'SOLICITUD GUÍA PSM + PSM'})]
    assert app.validate_workbench_changes(source, source, foreign, user, manual_stage=True)
    assert 'SOLICITUD GUÍA PSM + PSM' not in app.get_manual_stage_statuses('MSE', 'REVISIÓN DE ARCHIVOS', user)


def test_manual_stage_correction_preserves_roles_conflicts_and_stage_only_changes():
    source = pd.DataFrame([case()])
    change = [('001', {'STATUS': 'ORDEN RECIBIDA'})]
    assert app.validate_workbench_changes(source, source, change, 'Vero', manual_stage=True)
    assert not app.validate_status_change(identifier='001', apparatus='MSE', previous_status='REVISIÓN DE ARCHIVOS',
        new_status='ORDEN RECIBIDA', current_user='Vero', manual_stage=True)[0]
    fresh = pd.DataFrame([case(status='EN PLANEACIÓN')])
    assert 'otro usuario' in app.validate_workbench_changes(source, fresh, change, 'Admin', manual_stage=True)[0]
    extra = [('001', {'STATUS': 'ORDEN RECIBIDA', 'NOMBRE DOCTOR': 'Otro'})]
    assert 'sólo se puede cambiar la etapa' in app.validate_workbench_changes(source, source, extra, 'Admin', manual_stage=True)[0]
    sent = pd.DataFrame([case(status='PRODUCTO ENVIADO')])
    assert app.validate_workbench_changes(sent, sent, change, 'Admin', manual_stage=True)
    ready = pd.DataFrame([case(status='LISTO P/SINTERIZADO')])
    print_change = [('001', {'STATUS': 'ELABORACIÓN PLATINA'})]
    assert 'impresión' in app.validate_workbench_changes(ready, ready, print_change, 'Lesly', manual_stage=True)[0]


def test_manual_stage_save_records_its_own_comment(monkeypatch):
    source = pd.DataFrame([case()])
    edited = source.copy()
    edited.loc[0, 'STATUS'] = 'ORDEN RECIBIDA'
    writes, logs = [], []
    monkeypatch.setattr(app, 'clear_sheet_data_cache', lambda: None)
    monkeypatch.setattr(app, 'reset_workbench', lambda: None)
    monkeypatch.setattr(app, 'read_sheet_df', lambda _: source)
    monkeypatch.setattr(app, 'update_row_by_columna_1', lambda identifier, changes, **kwargs:
                        writes.append((identifier, changes, kwargs)) or {'success': True, 'skipped_columns': []})
    monkeypatch.setattr(app, 'register_status_change', lambda **kwargs: logs.append(kwargs))
    assert app.save_workbench_changes(source, edited, 'Admin', manual_stage=True) == (['001'], [])
    assert writes[0][0] == '001'
    assert writes[0][1] == {'STATUS': 'ORDEN RECIBIDA'}
    assert writes[0][2]['expected_values']['STATUS'] == 'REVISIÓN DE ARCHIVOS'
    assert logs[0]['change_comment'] == 'Cambio manual de etapa desde la ficha del pedido.'
    assert logs[0]['previous_status'] == 'REVISIÓN DE ARCHIVOS'
    assert logs[0]['new_status'] == 'ORDEN RECIBIDA'


def test_status_submenu_saves_manual_and_regular_rows_together_with_normal_field_edits(monkeypatch):
    source = pd.DataFrame([case('001'), case('002')])
    edited = source.copy()
    edited.loc[0, 'STATUS'] = app.display_selectbox_value('STATUS', 'ORDEN RECIBIDA')
    edited.loc[0, 'NOMBRE DOCTOR'] = 'Cliente corregido'
    edited.loc[1, 'STATUS'] = 'EN PLANEACIÓN'
    edited.attrs['manualStageTargets'] = {'001': app.display_selectbox_value('STATUS', 'ORDEN RECIBIDA')}
    writes, logs = [], []
    monkeypatch.setattr(app, 'clear_sheet_data_cache', lambda: None)
    monkeypatch.setattr(app, 'reset_workbench', lambda: None)
    monkeypatch.setattr(app, 'read_sheet_df', lambda _: source)
    monkeypatch.setattr(app, 'update_row_by_columna_1', lambda identifier, changes, **kwargs:
                        writes.append((identifier, changes, kwargs)) or {'success': True, 'skipped_columns': []})
    monkeypatch.setattr(app, 'register_status_change', lambda **kwargs: logs.append(kwargs))
    assert app.save_workbench_changes(source, edited, 'Admin') == (['001', '002'], [])
    assert writes[0][1] == {'STATUS': 'ORDEN RECIBIDA', 'NOMBRE DOCTOR': 'Cliente corregido'}
    assert writes[1][1]['STATUS'] == 'EN PLANEACIÓN'
    assert 'manual' in logs[0]['change_comment']
    assert logs[1]['change_comment'] == 'Actualización desde la tabla de trabajo'
    assert writes[0][2]['expected_values']['NOMBRE DOCTOR'] == source.iloc[0]['NOMBRE DOCTOR']


@pytest.mark.parametrize('user,target,intent', [
    ('Vero', 'ORDEN RECIBIDA', 'ORDEN RECIBIDA'),
    ('Admin', 'ORDEN RECIBIDA', 'EN PLANEACIÓN'),
    ('Admin', 'SOLICITUD GUÍA PSM + PSM', 'SOLICITUD GUÍA PSM + PSM'),
    ('Admin', '__all_stages__', '__all_stages__'),
])
def test_status_submenu_rejects_invalid_role_target_or_navigation_before_writing(monkeypatch, user, target, intent):
    source = pd.DataFrame([case()])
    edited = source.copy()
    edited.loc[0, 'STATUS'] = target
    edited.attrs['manualStageTargets'] = {'001': intent}
    writes = []
    monkeypatch.setattr(app, 'clear_sheet_data_cache', lambda: None)
    monkeypatch.setattr(app, 'read_sheet_df', lambda _: source)
    monkeypatch.setattr(app, 'update_row_by_columna_1', lambda *args, **kwargs: writes.append(args))
    saved, errors = app.save_workbench_changes(source, edited, user)
    assert not saved and errors and not writes


def test_manual_intent_is_per_row_and_preserves_conflict_print_and_archive_validation():
    source = pd.DataFrame([case('001'), case('002')])
    changes = [('001', {'STATUS': 'ORDEN RECIBIDA'}), ('002', {'STATUS': 'ORDEN RECIBIDA'})]
    errors = app.validate_workbench_changes(source, source, changes, 'Admin', manual_stage_ids={'001'})
    assert len(errors) == 1 and errors[0].startswith('002:')
    fresh = source.copy()
    fresh.loc[0, 'NOMBRE DOCTOR'] = 'Editado por otro usuario'
    changed = [('001', {'STATUS': 'ORDEN RECIBIDA', 'NOMBRE DOCTOR': 'Nueva edición'})]
    assert 'otro usuario' in app.validate_workbench_changes(source, fresh, changed, 'Admin', manual_stage_ids={'001'})[0]
    ready = pd.DataFrame([case(status='LISTO P/SINTERIZADO')])
    assert 'impresión' in app.validate_workbench_changes(ready, ready,
        [('001', {'STATUS': 'ELABORACIÓN PLATINA'})], 'Lesly', manual_stage_ids={'001'})[0]
    sent = pd.DataFrame([case(status='PRODUCTO ENVIADO')])
    assert app.validate_workbench_changes(sent, sent, changes[:1], 'Admin', manual_stage_ids={'001'})
    assert app.validate_workbench_changes(sent, sent, changes[:1], 'Admin', manual_stage_ids={'001'}, reactivate=True)


@pytest.mark.parametrize('user', ['Admin', 'Jime', 'Lesly', 'Vero'])
def test_grid_all_stages_follow_each_apparatus_and_user(user):
    source = pd.DataFrame([case()])
    context = app.workbench_grid_options(app.workbench_display_df(source), source, user)['context']
    labels = context['manualStageOptions']['001']
    backwards = app.display_selectbox_value('STATUS', 'ORDEN RECIBIDA')
    assert (backwards in labels) == (user != 'Vero')
    assert app.display_selectbox_value('STATUS', 'SOLICITUD GUÍA PSM + PSM') not in labels
    for apparatus in ('MSE', 'LEONE', 'HYRAX'):
        signature = context['apparatusFlowKeys'][apparatus]
        row = pd.Series(case(apparatus=apparatus))
        assert context['apparatusManualStageOptions']['001'][signature] == app.workbench_manual_stage_options(row, user)


def test_planning_shortcut_requires_a_planning_stage_in_the_apparatus_flow(monkeypatch):
    flow = [('REVISIÓN DE ARCHIVOS', None), ('PAGO CONFECCIÓN', None), ('ELABORACIÓN PLATINA', None)]
    monkeypatch.setattr(app, 'get_process_flow', lambda _: flow)
    row = pd.Series(case(status='REVISIÓN DE ARCHIVOS', apparatus='HYRAX'))
    assert app.display_selectbox_value(app.STATUS_COLUMN, 'EN PLANEACIÓN') not in app.workbench_stage_options(row, 'Admin')
    assert not app.validate_status_change(identifier='001', apparatus='HYRAX',
        previous_status='REVISIÓN DE ARCHIVOS', new_status='EN PLANEACIÓN', current_user='Admin')[0]


@pytest.mark.parametrize('user', ['Admin', 'Jime', 'Lesly'])
@pytest.mark.parametrize('current,branch,next_normal', [
    ('REVISIÓN DE ARCHIVOS', 'ESCANEO MAL (EN REPETICIÓN)', 'PAGO PLANEACIÓN'),
    ('REVISIÓN PLAN DOCTOR', 'SOLICITUD DE CAMBIOS', 'PAGO CONFECCIÓN'),
])
def test_sheet_optional_branches_also_allow_the_next_normal_stage(monkeypatch, user, current, branch, next_normal):
    matrix = [['MSE', ''], ['Fases', 'Tiempo'],
              ['ORDEN RECIBIDA', ''], ['REVISIÓN DE ARCHIVOS', ''],
              ['ESCANEO MAL (EN REPETICIÓN)', ''], ['PAGO PLANEACIÓN', ''],
              ['EN PLANEACIÓN', '<3 dias'], ['REVISIÓN PLAN DOCTOR', ''],
              ['SOLICITUD DE CAMBIOS', '<3 dias'], ['PAGO CONFECCIÓN', ''],
              ['ELABORACIÓN PLATINA', '<1 hr']]
    flow = app.parse_aparato_process_matrix(matrix)['MSE']
    monkeypatch.setattr(app, 'get_process_flow', lambda _: flow)
    row = pd.Series(case(status=current))
    options = app.workbench_stage_options(row, user)
    for target in (branch, next_normal):
        assert app.display_selectbox_value(app.STATUS_COLUMN, target) in options
        source = pd.DataFrame([row])
        assert not app.validate_workbench_changes(source, source, [('001', {'STATUS': target})], user)
        assert app.validate_status_change(identifier='001', apparatus='MSE', previous_status=current,
            new_status=target, current_user=user)[0]
    if current == 'REVISIÓN DE ARCHIVOS':
        assert app.display_selectbox_value(app.STATUS_COLUMN, 'EN PLANEACIÓN') in options
        assert app.display_selectbox_value(app.STATUS_COLUMN, 'ELABORACIÓN PLATINA') not in options
        source = pd.DataFrame([row])
        assert app.validate_workbench_changes(source, source, [('001', {'STATUS': 'ELABORACIÓN PLATINA'})], user)


def test_jime_and_lesly_have_full_editing_permissions_like_admin():
    """Jime y Lesly editan cualquier etapa, igual que Admin (Vero conserva sus límites)."""
    jime_row = pd.Series(case(status="REVISIÓN DE ARCHIVOS"))
    assert app.display_selectbox_value(app.STATUS_COLUMN, "CANCELO") in app.workbench_stage_options(jime_row, "Jime")
    assert app.is_transition_allowed_for_user("Jime", "PULIDO (EN CONFECCIÓN)", "SOLDADURA (EN CONFECCIÓN)", "MSE")
    assert app.is_transition_allowed_for_user("Lesly", "ORDEN RECIBIDA", "REVISIÓN DE ARCHIVOS", "MSE")
    assert not app.is_transition_allowed_for_user("Vero", "ORDEN RECIBIDA", "REVISIÓN DE ARCHIVOS", "MSE")
    assert app.user_can_edit_tab("Lesly", "Jime") is True
    assert app.user_can_edit_tab("Vero", "Jime") is False
    assert all(app.get_user_operational_statuses(user) == [] for user in app.ADMIN_ACCESS_USERS)


def test_printing_mark_still_gates_lesly_despite_full_permissions():
    """La marca de impresión sigue exigida a Lesly aunque ahora edite todo."""
    lesly_row = pd.Series(case(status="LISTO P/SINTERIZADO"))
    assert app.display_selectbox_value(app.STATUS_COLUMN, "ELABORACIÓN PLATINA") not in app.workbench_stage_options(lesly_row, "Lesly")
    lesly_row[app.ESTATUS_PRINT_DATE_COLUMN] = "2026/09/03"
    assert app.display_selectbox_value(app.STATUS_COLUMN, "ELABORACIÓN PLATINA") in app.workbench_stage_options(lesly_row, "Lesly")


def test_paused_cases_are_archived_and_can_return_to_any_stage_in_their_flow():
    source = pd.DataFrame([
        case("active", status="ORDEN RECIBIDA", apparatus="MSE"),
        case("paused", status=app.PAUSED_STATUS, apparatus="HYRAX"),
    ])

    assert list(app.active_workbench_cases(source)[app.ID_COLUMN]) == ["active"]
    assert list(app.paused_workbench_cases(source)[app.ID_COLUMN]) == ["paused"]
    options = app.get_allowed_next_statuses("HYRAX", app.PAUSED_STATUS)
    assert options[0] == app.PAUSED_STATUS
    assert "ORDEN RECIBIDA" in options
    assert "LISTO P/CONFECCIÓN" in options
    assert all(
        app.is_transition_allowed_for_user(user, app.PAUSED_STATUS, "ORDEN RECIBIDA", "HYRAX")
        for user in app.APP_USERS
    )


def test_sent_cases_are_listed_apart_from_the_active_table():
    source = pd.DataFrame([
        case("1", "ENVIADO"), case("2", "ENVIO DE ENCUESTA"), case("3", "CANCELO"),
        case("4", "PRODUCTO ENVIADO"), case("5"), {app.ID_COLUMN: "", app.STATUS_COLUMN: "ENVIADO"},
    ])
    sent = app.sent_workbench_cases(source)
    assert list(sent[app.ID_COLUMN]) == ["1", "2", "4"]
    assert list(sent[app.STATUS_COLUMN]) == ["ENVIADO", "ENVÍO DE ENCUESTA", "PRODUCTO ENVIADO"]
    assert set(app.active_workbench_cases(source)[app.ID_COLUMN]).isdisjoint(sent[app.ID_COLUMN])


def test_sent_cases_return_to_any_open_stage_of_their_flow():
    for status in app.WORKBENCH_SENT_STATUSES:
        options = app.workbench_reactivation_statuses("HYRAX", status)
        assert options[0] == status
        assert {"ORDEN RECIBIDA", "LISTO P/CONFECCIÓN", "PRODUCTO ENVIADO"} <= set(options)
        assert not {"CANCELO", app.PAUSED_STATUS} & set(options)
        assert "ENVÍO DE ENCUESTA" in options
    assert "EN DISEÑO" in app.workbench_reactivation_statuses("TIGER", "ENVIADO")
    # Sólo los enviados usan esta ruta; los demás conservan su flujo normal.
    assert app.workbench_reactivation_statuses("MSE", "EN PLANEACIÓN") == ["EN PLANEACIÓN"]


def test_sent_reactivation_only_changes_the_stage_for_unrestricted_users():
    original = pd.DataFrame([case("1", "ENVIADO")])
    reactivate = [("1", {app.STATUS_COLUMN: "EN PLANEACIÓN"})]
    for user in ("Admin", "Jime", "Lesly"):
        assert not app.validate_workbench_changes(original, original, reactivate, user, reactivate=True)
    assert "no puede reactivar" in app.validate_workbench_changes(
        original, original, reactivate, "Vero", reactivate=True)[0]
    assert "no reactiva" in app.validate_workbench_changes(
        original, original, [("1", {app.STATUS_COLUMN: "CANCELO"})], "Admin", reactivate=True)[0]
    assert "sólo se puede cambiar la etapa" in app.validate_workbench_changes(
        original, original, [("1", {app.STATUS_COLUMN: "EN PLANEACIÓN", "NOMBRE DOCTOR": "Otro"})],
        "Admin", reactivate=True)[0]
    # Si otro usuario ya lo reactivó, no se vuelve a mover desde el histórico.
    fresh = pd.DataFrame([case("1", "EN PLANEACIÓN")])
    assert "otro usuario" in app.validate_workbench_changes(
        original, fresh, reactivate, "Admin", reactivate=True)[0]


def test_sent_grid_only_edits_stage_with_reactivation_options():
    source = pd.DataFrame([case("1", "ENVIADO"), case("2", "ENVIADO"), case("2", "ENVIADO")])
    grid = app.workbench_display_df(source)
    options = app.workbench_sent_grid_options(grid, source, "Jime")
    columns = {item["field"]: item for item in options["columnDefs"]}
    assert columns["NOMBRE DOCTOR"]["editable"] is False
    assert columns["PAGO"]["editable"] is False
    assert columns[app.STATUS_COLUMN]["editable"] is not False
    stages = options["context"]["stageOptions"]
    assert app.display_selectbox_value(app.STATUS_COLUMN, "EN PLANEACIÓN") in stages["1"]
    assert app.display_selectbox_value(app.STATUS_COLUMN, "CANCELO") not in stages["1"]
    # Un folio repetido se ve, pero no se puede reactivar.
    assert stages["2"] == ["ENVIADO"]
    vero = {item["field"]: item for item in app.workbench_sent_grid_options(grid, source, "Vero")["columnDefs"]}
    assert vero[app.STATUS_COLUMN]["editable"] is False


def test_sent_reactivation_saves_the_stage_with_its_own_comment(monkeypatch):
    original = app.workbench_display_df(pd.DataFrame([case("1", "ENVIADO")]))
    edited = original.copy()
    edited.loc[0, app.STATUS_COLUMN] = app.display_selectbox_value(app.STATUS_COLUMN, "EN PLANEACIÓN")
    calls = []
    monkeypatch.setattr(app, "clear_sheet_data_cache", lambda: None)
    monkeypatch.setattr(app, "reset_workbench", lambda: None)
    monkeypatch.setattr(app, "read_sheet_df", lambda _: pd.DataFrame([case("1", "ENVIADO")]))
    monkeypatch.setattr(app, "advance_case_status", lambda **kwargs: calls.append(kwargs) or True)
    assert app.save_workbench_changes(original, edited, "Admin", reactivate=True) == (["1"], [])
    assert calls[0]["new_status"] == "EN PLANEACIÓN"
    assert calls[0]["comment"] == "Reactivado desde el histórico de Enviados."
    assert calls[0]["extra_changes"] == {}


def test_repeated_sent_folios_do_not_block_other_reactivations(monkeypatch):
    baseline = app.workbench_display_df(
        pd.DataFrame([case("1", "ENVIADO"), case("2", "ENVIADO"), case("2", "ENVIADO")])
    )
    edited = baseline.copy()
    edited.loc[0, app.STATUS_COLUMN] = app.display_selectbox_value(app.STATUS_COLUMN, "EN PLANEACIÓN")
    with pytest.raises(ValueError):
        app.workbench_changes(baseline, edited)
    assert app.workbench_changes(*app.without_repeated_folios(baseline, edited)) == [
        ("1", {app.STATUS_COLUMN: "EN PLANEACIÓN"})
    ]

    monkeypatch.setattr(app.st, "session_state", {
        "workbench_sent_editor_key": "sent_grid",
        "workbench_sent_editor_baseline": baseline,
        "sent_grid": {"rows": edited.to_dict("records")},
    })
    assert app.workbench_sent_pending_count() == 1
    assert app.workbench_pending_count() == 1
    app.discard_sent_workbench_changes()
    assert app.workbench_pending_count() == 0


@pytest.mark.parametrize("user", ["Admin", "Vero"])
def test_sent_archive_opens_with_search_and_apparatus_filters(user):
    from streamlit.testing.v1 import AppTest
    script = f'''
import sys
sys.path.insert(0, {str(Path(__file__).resolve().parents[1])!r})
import lab_pg as app
import pandas as pd
import streamlit as st
from unittest.mock import patch
rows = [{case()!r}, {case("900", "ENVIADO", "TIGER")!r}, {case("901", "ENVÍO DE ENCUESTA", "HYRAX")!r}]
if st.session_state.get("test_open"):
    st.session_state["workbench_sent_expanded"] = True
with patch.multiple(
    app,
    require_authenticated_user=lambda: {user!r},
    workbench_tab_options=lambda _: ["📋 Seguimiento"],
    ensure_tiempos_headers=lambda: None,
    read_sheet_df=lambda name: pd.DataFrame(rows) if name == app.SHEET_ESTATUS else pd.DataFrame(),
):
    app.main()
'''
    at = AppTest.from_string(script, default_timeout=15).run()
    assert not at.exception
    assert any(item.label == "🚚 Enviados · 2 pedido(s)" for item in at.expander)
    assert all(item.key != "workbench_sent_search" for item in at.text_input)
    assert at.button(key="workbench_signal_card_total").label.endswith("**1**")

    at.session_state["test_open"] = True
    at.run()
    assert not at.exception
    search = at.text_input(key="workbench_sent_search")
    assert search.placeholder == "Folio, doctor, paciente o aparato"
    assert at.multiselect(key="workbench_sent_apparatus").options == ["TIGER", "HYRAX"]
    assert at.button(key="workbench_sent_save").disabled
    assert any("Sólo Admin, Jime y Lesly" in item.value for item in at.caption) == (user == "Vero")
    search.set_value("900").run()
    assert not at.exception
    assert app.ID_COLUMN in at.session_state["workbench_sent_editor_baseline"]
    assert list(at.session_state["workbench_sent_editor_baseline"][app.ID_COLUMN]) == ["900"]


@pytest.mark.parametrize("target", ["EN DISEÑO", "ENVÍO DE ENCUESTA"])
def test_sent_stage_change_blocks_refresh_and_updates_the_correct_table(target):
    from streamlit.testing.v1 import AppTest
    script = f'''
import sys
sys.path.insert(0, {str(Path(__file__).resolve().parents[1])!r})
import lab_pg as app
import pandas as pd
import streamlit as st
if "demo_rows" not in st.session_state:
    st.session_state.demo_rows = [{case()!r}, {case("900", "PRODUCTO ENVIADO", "TIGER")!r}]
    st.session_state.demo_logs = []
    st.session_state.workbench_sent_expanded = True
def update(identifier, changes, **kwargs):
    for row in st.session_state.demo_rows:
        if row[app.ID_COLUMN] == identifier:
            row.update(changes)
    return {{"success":True,"updated_columns":list(changes),"skipped_columns":[],"error":""}}
from unittest.mock import patch
with patch.multiple(
    app,
    require_authenticated_user=lambda: "Jime",
    workbench_tab_options=lambda _: ["📋 Seguimiento"],
    ensure_tiempos_headers=lambda: None,
    clear_sheet_data_cache=lambda: None,
    read_sheet_df=lambda name: pd.DataFrame(st.session_state.demo_rows) if name == app.SHEET_ESTATUS else pd.DataFrame(),
    register_status_change=lambda **kwargs: st.session_state.demo_logs.append(kwargs),
    update_row_by_columna_1=update,
):
    app.main()
'''
    at = AppTest.from_string(script, default_timeout=15).run()
    assert not at.exception
    assert at.button(key="workbench_signal_card_total").label.endswith("**1**")
    assert any(item.label == "🚚 Enviados · 1 pedido(s)" for item in at.expander)
    key = at.session_state["workbench_sent_editor_key"]
    rows = at.session_state["workbench_sent_editor_baseline"].to_dict("records")
    rows[0][app.STATUS_COLUMN] = app.display_selectbox_value(app.STATUS_COLUMN, target)
    at.session_state[key] = {"rows": rows}
    at.run()
    assert not at.exception
    assert next(button for button in at.button if button.label == "Actualizar datos").disabled
    assert next(button for button in at.button if button.label == "Guardar cambios").disabled
    assert at.text_input(key="workbench_sent_search").disabled
    save = at.button(key="workbench_sent_save")
    assert not save.disabled
    save.click()
    at.session_state[key] = {"rows": rows}
    at.run()
    assert not at.exception
    assert at.session_state["demo_rows"][1][app.STATUS_COLUMN] == target
    archived = target == "ENVÍO DE ENCUESTA"
    comment = ("Etapa actualizada desde el histórico de Enviados." if archived
               else "Reactivado desde el histórico de Enviados.")
    assert at.session_state["demo_logs"][0]["change_comment"] == comment
    assert at.button(key="workbench_signal_card_total").label.endswith(f"**{1 if archived else 2}**")
    assert any(item.label == f"🚚 Enviados · {1 if archived else 0} pedido(s)" for item in at.expander)
    if archived:
        assert at.button(key="workbench_sent_save").label == "Guardar etapas"


@pytest.mark.parametrize("sent_archive_exists", [False, True])
def test_workbench_rebuilds_a_snapshot_created_before_archive_changes(monkeypatch, sent_archive_exists):
    legacy_table = pd.DataFrame([case("shipped", status="PRODUCTO ENVIADO")])
    app.st.session_state["workbench_snapshot"] = {
        "user": "Admin",
        "table": legacy_table,
        "at": NOW,
    }
    if sent_archive_exists:
        app.st.session_state["workbench_snapshot"].update({
            "paused_table": pd.DataFrame(), "sent_table": pd.DataFrame(), "times": pd.DataFrame(),
        })
    calls = []

    monkeypatch.setattr(app, "read_sheet_df", lambda sheet: pd.DataFrame())
    monkeypatch.setattr(
        app,
        "build_workbench_table",
        lambda *_args, paused_only=False: calls.append(paused_only) or pd.DataFrame(),
    )
    monkeypatch.setattr(app, "workbench_pending_count", lambda: 0)
    monkeypatch.setattr(app, "render_workbench_signal_cards", lambda *_args: "Todos")
    monkeypatch.setattr(app, "render_paused_workbench", lambda *_args, **_kwargs: None)

    # Se detiene después de reconstruir la fotografía: sólo interesa verificar
    # que no se reutilice el formato viejo que originaba contador cero.
    monkeypatch.setattr(app.st, "columns", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("stop")))
    with pytest.raises(RuntimeError, match="stop"):
        app.render_workbench("Admin")

    assert calls == [False, True]
    assert "paused_table" in app.st.session_state["workbench_snapshot"]
    assert app.st.session_state["workbench_snapshot"]["sent_statuses"] == app.WORKBENCH_SENT_STATUSES


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
    assert order[:7] == [
        "SELECCIONAR",
        app.ID_COLUMN,
        "SEMÁFORO",
        app.APARATO_COLUMN,
        app.STATUS_COLUMN,
        "NOMBRE DOCTOR",
        "NOMBRE PACIENTE",
    ]
    assert columns[app.ID_COLUMN]["suppressMovable"] is True
    assert columns[app.APARATO_COLUMN]["pinned"] == "left"
    assert columns[app.STATUS_COLUMN]["pinned"] == "left"
    assert options["autoSizeStrategy"]["type"] == "fitCellContents"
    assert columns["NOMBRE PACIENTE"]["headerClass"] == "lab-header-editable"
    assert columns["PAGO"]["headerClass"] == "lab-header-editable"
    assert columns["RESPONSABLE"]["headerClass"] == "lab-header-automatic"
    assert columns["RESPONSABLE"]["hide"] is True
    assert "WorkbenchStatusEditor" in columns[app.STATUS_COLUMN]["cellEditor"].js_code
    assert "backgroundColor" in columns[app.STATUS_COLUMN]["cellEditor"].js_code


def test_grid_recalculates_layout_when_a_closed_expander_becomes_visible():
    source = pd.DataFrame([case()])
    grid = app.workbench_display_df(source)
    grid.insert(0, "SELECCIONAR", False)

    callback = app.workbench_grid_options(grid, source, "Admin")["onGridReady"].js_code

    assert "ResizeObserver" in callback
    assert "params.api.sizeColumnsToFit()" in callback
    assert "params.api.redrawRows()" in callback


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
    original = pd.DataFrame([case(**{"FECHA ENVÍO": "2026/09/20"})])
    changes = [("001", {
        "NOMBRE DOCTOR": "Otro",
        app.APARATO_COLUMN: "TIGER",
        "FECHA ENVÍO": "2026/09/21",
    })]
    assert not app.validate_workbench_changes(original, original, changes, user)


@pytest.mark.parametrize("user", ["Admin", "Jime", "Lesly", "Vero"])
def test_nobody_edits_sheet_formula_delivery_date(user):
    # FECHA PARA ENTREGA es una fórmula de la hoja (envío a Stefano + 6 días
    # hábiles): escribir en su rango la rompería para todos los pedidos.
    original = pd.DataFrame([case(**{"FECHA PARA ENTREGA": "2026/09/20"})])
    assert "FECHA PARA ENTREGA" not in app.workbench_editable_columns(user, original.columns)
    assert app.validate_workbench_changes(
        original, original, [("001", {"FECHA PARA ENTREGA": "2026/09/21"})], user)


def test_admin_access_users_see_every_responsible_by_default():
    for status in app.PLANNING_STATUSES:
        assert app.get_process_responsible(status) == "JIME"
        assert app.get_status_tab_owner(status) == "Jime"
    owners = ["JIME", "LESLY", "VERO"]
    for user in app.ADMIN_ACCESS_USERS:
        assert app.workbench_default_owner(user, owners) == "Todos"
    assert app.workbench_default_owner("Vero", owners) == "VERO"


def test_skip_stage_and_foreign_role_rejected():
    original = pd.DataFrame([case()])
    assert app.validate_workbench_changes(original,original,[("001",{"STATUS":"PRODUCTO ENVIADO"})],"Admin")
    assert app.validate_workbench_changes(original,original,[("001",{"STATUS":"PDTE ENVIAR GUÍA PSM + PSM"})],"Admin")
    assert app.validate_workbench_changes(original,original,[("001",{"STATUS":"PRODUCTO ENVIADO"})],"Vero")
    assert app.validate_workbench_changes(original,original,[("001",{"STATUS":"ESCANEO MAL (EN REPETICIÓN)"})],"Vero")
    assert not app.validate_workbench_changes(original,original,[("001",{"STATUS":"ESCANEO MAL (EN REPETICIÓN)"})],"Jime")


def test_lesly_cannot_advance_before_printing():
    original = pd.DataFrame([case(status="LISTO P/SINTERIZADO")])
    delta = [("001", {"STATUS":"ELABORACIÓN PLATINA"})]
    assert "impresión" in app.validate_workbench_changes(original,original,delta,"Lesly")[0]
    original[app.ESTATUS_PRINT_DATE_COLUMN] = "2026-09-03"
    assert not app.validate_workbench_changes(original,original,delta,"Lesly")


def test_payment_blocks_advancement_without_authorization(monkeypatch):
    monkeypatch.setattr(app,"get_active_tiempo_row",lambda _: (2,{"PUEDE_AVANZAR":"No"}))
    monkeypatch.setattr(app,"get_case_commercial_payment_status",lambda _: "SIN PAGO")
    allowed, reason = app.can_advance_from_payment("001","PAGO PLANEACIÓN","Vero")
    assert not allowed and "pago" in reason
    monkeypatch.setattr(app,"get_active_tiempo_row",lambda _: (2,{"PUEDE_AVANZAR":"Sí"}))
    assert app.can_advance_from_payment("001","PAGO PLANEACIÓN","Vero") == (True,"")


def test_admin_jime_and_lesly_are_not_blocked_by_pending_payment(monkeypatch):
    original = pd.DataFrame([case(status="PAGO PLANEACIÓN")])
    delta = [("001", {"STATUS":"EN PLANEACIÓN"})]
    monkeypatch.setattr(app,"get_active_tiempo_row",lambda _: (2,{"PUEDE_AVANZAR":"No"}))
    for user in ("Admin", "Jime", "Lesly"):
        assert not app.validate_workbench_changes(original,original,delta,user)
        assert app.can_advance_from_payment("001","PAGO PLANEACIÓN",user) == (True,"")


def test_concurrent_status_change_blocks_even_metadata_edit():
    original = pd.DataFrame([case()])
    fresh = pd.DataFrame([case(status="EN PLANEACIÓN")])
    errors = app.validate_workbench_changes(original,fresh,[("001",{"PAGO":"TOTAL"})],"Admin")
    assert errors and "otro usuario" in errors[0]


def test_batch_preflight_does_not_write_any_row_on_error(monkeypatch):
    original = pd.DataFrame([case("001"),case("002")])
    edited = original.copy()
    edited.loc[0,"PAGO"] = "TOTAL"
    edited.loc[1,"STATUS"] = "PDTE ENVIAR GUÍA PSM + PSM"
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
    options=[]
    class Sheet:
        def row_values(self, _): return headers
        def append_row(self, row, **kwargs):
            saved.append(row)
            options.append(kwargs)
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
    assert saved[0][1] == '001'
    assert options == [{'value_input_option': 'RAW', 'insert_data_option': 'INSERT_ROWS'}]


@pytest.mark.parametrize("user",["Admin","Jime","Lesly","Vero"])
def test_apparatus_tabs_match_each_user(user):
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
    expected_tabs = {
        "Admin": ["📋 Seguimiento", "📥 Recibidos de Forms"],
        "Jime": ["📋 Seguimiento", "📥 Recibidos de Forms"],
        "Lesly": ["📋 Seguimiento", "📥 Recibidos de Forms"],
        "Vero": ["📋 Seguimiento", "🛠️ Confección y Calidad"],
    }
    assert [tab.label for tab in at.tabs] == expected_tabs[user]
    new_order = [item for item in at.expander if item.label == "➕ Nuevo pedido"]
    assert bool(new_order) == (user != "Vero")
    if new_order:
        assert not new_order[0].proto.expanded
        assert not new_order[0].text_input
    assert not at.toggle
    assert not at.get("segmented_control")
    assert len(at.tabs[0].get("component_instance")) == 1
    assert at.checkbox(key="workbench_hide_automatic").label == "Ocultar automáticas"
    assert at.selectbox(key="workbench_owner").value == "Todos"
    assert any("Fijas: Folio, Semáforo y columnas principales" in item.value for item in at.tabs[0].markdown)
    assert any("Editable automática" in item.value for item in at.tabs[0].markdown)
    assert all(item.key != "workbench_signal" for item in at.selectbox)
    assert at.button(key="workbench_signal_card_total").label.startswith("🦷 Pedidos activos")
    assert at.multiselect(key=f"workbench_filter_{app.APARATO_COLUMN}").label == "Aparato"
    at.button(key="workbench_signal_card_gray").click().run()
    assert at.session_state[app.WORKBENCH_SIGNAL_FILTER_KEY] == "⚪ Sin medición"
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
app.workbench_tab_options = lambda _: ["📋 Seguimiento"]
app.ensure_tiempos_headers = lambda: None
app.read_sheet_df = lambda name: pd.DataFrame()
app.main()
'''
    at = AppTest.from_string(script).run()
    assert not at.exception
    assert at.button(key="workbench_signal_card_total").label.endswith("**0**")


def test_only_selected_lab_tab_executes():
    from streamlit.testing.v1 import AppTest

    script = f'''
import sys
sys.path.insert(0, {str(Path(__file__).resolve().parents[1])!r})
import streamlit as st
import lab_pg as app
from unittest.mock import patch
with (
    patch.object(app, "require_authenticated_user", lambda: "Admin"),
    patch.object(app, "ensure_tiempos_headers", lambda: None),
    patch.object(app, "workbench_tab_options", lambda _: ["📋 Seguimiento", "📥 Recibidos de Forms"]),
    patch.object(app, "render_workbench", lambda _: st.write("seguimiento ejecutado")),
    patch.object(app, "render_workbench_auxiliary", lambda label, _: st.write(f"auxiliar ejecutado: {{label}}")),
):
    app.main()
'''
    at = AppTest.from_string(script, default_timeout=15).run()
    assert not at.exception
    assert [tab.label for tab in at.tabs] == ["📋 Seguimiento", "📥 Recibidos de Forms"]
    assert [item.value for item in at.tabs[0].markdown] == ["seguimiento ejecutado"]
    assert all(not tab.markdown for tab in at.tabs[1:])

    at.session_state["lab_primary_tabs_Admin"] = "📥 Recibidos de Forms"
    at.run()
    assert not at.exception
    assert not at.tabs[0].markdown
    assert [item.value for item in at.tabs[1].markdown] == [
        "auxiliar ejecutado: 📥 Recibidos de Forms"
    ]


def test_forms_tab_shows_only_forms_without_document_shipping():
    from streamlit.testing.v1 import AppTest

    script = f'''
import sys
sys.path.insert(0, {str(Path(__file__).resolve().parents[1])!r})
import streamlit as st
import lab_pg as app
from unittest.mock import patch
with (
    patch.object(app, "require_authenticated_user", lambda: "Jime"),
    patch.object(app, "ensure_tiempos_headers", lambda: None),
    patch.object(app, "workbench_tab_options", lambda _: ["📋 Seguimiento", app.APP_TAB_OPTIONS["estefano"]]),
    patch.object(app, "render_workbench", lambda _: st.write("seguimiento ejecutado")),
    patch.object(app, "render_estefano_forms_review", lambda can_edit: st.write(f"forms ejecutado: {{can_edit}}")),
    patch.object(app, "render_estefano_shipping_tab", lambda *args, **kwargs: st.write("envío ejecutado")),
):
    app.main()
'''
    at = AppTest.from_string(script, default_timeout=15).run()
    at.session_state["lab_primary_tabs_Jime"] = "📥 Recibidos de Forms"
    at.run()
    assert not at.exception
    # Sin subpestañas: sólo existen las pestañas principales.
    assert [tab.label for tab in at.tabs] == ["📋 Seguimiento", "📥 Recibidos de Forms"]
    assert [item.value for item in at.tabs[1].subheader] == ["📥 Recibidos de Forms"]
    assert [item.value for item in at.tabs[1].markdown] == ["forms ejecutado: True"]
    assert not at.tabs[1].warning
    assert all("envío ejecutado" not in item.value for item in at.markdown)


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
app.workbench_tab_options = lambda _: ["📋 Seguimiento"]
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


def test_save_remembers_edited_row_so_the_grid_can_scroll_back_to_it():
    """Guardar reconstruye la grilla (nueva clave); debe apuntar al folio editado."""
    from streamlit.testing.v1 import AppTest
    script = f'''
import sys
sys.path.insert(0, {str(Path(__file__).resolve().parents[1])!r})
import lab_pg as app
import pandas as pd
import streamlit as st
from workbench_grid import render_grid as real_render_grid

if "demo_rows" not in st.session_state:
    st.session_state.demo_rows = [{case()!r}]
if "captured_scroll_targets" not in st.session_state:
    st.session_state.captured_scroll_targets = []

app.require_authenticated_user = lambda: "Admin"
app.workbench_tab_options = lambda _: ["📋 Seguimiento"]
app.ensure_tiempos_headers = lambda: None
app.clear_sheet_data_cache = lambda: None
app.read_sheet_df = lambda name: pd.DataFrame(st.session_state.demo_rows) if name == app.SHEET_ESTATUS else pd.DataFrame()
def update(identifier, changes, **kwargs):
    for row in st.session_state.demo_rows:
        if row[app.ID_COLUMN] == identifier:
            row.update(changes)
    return {{"success":True,"updated_columns":list(changes),"skipped_columns":[],"error":""}}
app.update_row_by_columna_1 = update

def spy_render_grid(grid, options, key):
    st.session_state.captured_scroll_targets.append(options["context"].get("scrollToId"))
    return real_render_grid(grid, options, key)
app.render_grid = spy_render_grid

app.main()
'''
    # El script exec'd importa el mismo módulo lab_pg ya cargado (no una copia),
    # así que este monkeypatch sobrevive fuera de la prueba si no se restaura.
    original_render_grid = app.render_grid
    try:
        at = AppTest.from_string(script, default_timeout=15).run()
        key = at.session_state["workbench_editor_key"]
        delta = grid_event(at, {"001": {"PAGO": "🟢 TOTAL"}})
        at.session_state[key] = delta
        at.run()
        assert not at.exception
        next(button for button in at.button if button.label == "Guardar cambios").click()
        at.session_state[key] = delta
        at.run()
        assert not at.exception
        targets = at.session_state["captured_scroll_targets"]
        # Antes de guardar nadie pide scroll; justo después, la grilla reconstruida
        # apunta al folio que se acaba de editar.
        assert targets[0] is None
        assert targets[-1] == "001"
    finally:
        app.render_grid = original_render_grid


def test_editing_stage_keeps_editor_identity_and_revert_unlocks_filters():
    from streamlit.testing.v1 import AppTest
    script = f'''
import sys
sys.path.insert(0, {str(Path(__file__).resolve().parents[1])!r})
import lab_pg as app
import pandas as pd
app.require_authenticated_user = lambda: "Admin"
app.workbench_tab_options = lambda _: ["📋 Seguimiento"]
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
app.workbench_tab_options = lambda _: ["📋 Seguimiento"]
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
    assert any("Pedido · Doctora ejemplo" in item.value for item in at.subheader)


def test_remembered_lab_user_is_signed_and_cannot_be_renamed():
    passwords = {"Admin": "admin-secret", "Jime": "jime-secret"}
    signature = app.login_url_signature("Admin", passwords)
    assert app.valid_url_login("Admin", signature, passwords)
    assert not app.valid_url_login("Jime", signature, passwords)
    assert not app.valid_url_login("Admin", "altered", passwords)


def test_unified_app_switches_to_aligners_without_loading_apparatus():
    from streamlit.testing.v1 import AppTest

    script = f"""
import sys
sys.path.insert(0, {str(Path(__file__).resolve().parents[1])!r})
import streamlit as st
import lab_pg as app
from unittest.mock import patch

st.session_state[app.LAB_WORKSPACE_STATE_KEY] = app.LAB_VIEW_ALIGNERS
st.session_state["apparatus_loaded"] = False
st.session_state["aligners_loaded"] = ""

with (
    patch.object(app, "require_authenticated_user", lambda: "Admin"),
    patch.object(app, "ensure_tiempos_headers", lambda: st.session_state.__setitem__("apparatus_loaded", True)),
    patch.object(app.alineadores_pg, "apply_custom_css", lambda: None),
    patch.object(
        app.alineadores_pg,
        "render_embedded_workspace",
        lambda user: st.session_state.__setitem__("aligners_loaded", user),
    ),
):
    app.main()
"""
    at = AppTest.from_string(script, default_timeout=15).run()
    assert not at.exception
    assert at.session_state["aligners_loaded"] == "Admin"
    assert at.session_state["apparatus_loaded"] is False
    assert any("Control de alineadores" in markdown.value for markdown in at.markdown)


PROCESOS_APARATO_VALUES = [
    ["MSE", "", "BOTÓN DE NANCE", ""],
    ["Fases", "Tiempo", "Fases", "Tiempo"],
    ["ORDEN RECIBIDA", "", "ORDEN RECIBIDA", ""],
    ["REVISIÓN DE ARCHIVOS", "<5 hrs", "REVISIÓN DE ARCHIVOS", "<5 hrs"],
    ["PAGO CONFECCIÓN", "", "PAGO CONFECCIÓN", ""],
    ["LISTO P/SINTERIZADO", "<1 dia", "LISTO P/SINTERIZADO", "<1 dia"],
    # "VERO" ocupa una celda sobrante de MSE (su flujo ya terminó) para anotar
    # quién es responsable de la fila; no debe colarse como un status de MSE.
    ["VERO", "", "PULIDO / EN CONFECCIÓN", "<3 hrs"],
    ["", "", "ENVIO DE ENCUESTA", "<3 dias"],
]


def test_parse_aparato_process_matrix_reads_pairs_and_skips_responsible_notes():
    flows = app.parse_aparato_process_matrix(PROCESOS_APARATO_VALUES)

    assert list(flows) == ["MSE", "BOTÓN DE NANCE"]
    assert flows["MSE"] == [
        ("ORDEN RECIBIDA", None),
        ("REVISIÓN DE ARCHIVOS", "<5 hrs"),
        ("PAGO CONFECCIÓN", None),
        ("LISTO P/SINTERIZADO", "<1 dia"),
    ]
    # PULIDO / EN CONFECCIÓN y ENVIO DE ENCUESTA llegan con el alias vigente.
    assert flows["BOTÓN DE NANCE"][-2:] == [
        ("PULIDO (EN CONFECCIÓN)", "<3 hrs"),
        ("ENVÍO DE ENCUESTA", "<3 dias"),
    ]


def test_parse_aparato_process_matrix_ignores_short_or_empty_sheets():
    assert app.parse_aparato_process_matrix([]) == {}
    assert app.parse_aparato_process_matrix([["MSE"], ["Fases", "Tiempo"]]) == {}


def test_merge_dynamic_process_flows_adds_new_apparatus_without_mutating_globals(monkeypatch):
    baseline_config = {"MSE": [("ORDEN RECIBIDA", None)]}
    baseline_options = ["MSE"]
    monkeypatch.setattr(app, "PROCESS_CONFIG", baseline_config)
    monkeypatch.setattr(app, "APARATO_OPTIONS", baseline_options)

    merged_config, merged_options, changed = app.merge_dynamic_process_flows(
        {"BOTÓN DE NANCE": [("ORDEN RECIBIDA", None), ("PAGO CONFECCIÓN", None)]}
    )

    assert changed is True
    assert merged_config["BOTÓN DE NANCE"] == [("ORDEN RECIBIDA", None), ("PAGO CONFECCIÓN", None)]
    assert merged_options == ["MSE", "BOTÓN DE NANCE"]
    # merge_dynamic_process_flows es puro: no toca los globales que le pasamos.
    assert baseline_config == {"MSE": [("ORDEN RECIBIDA", None)]}
    assert baseline_options == ["MSE"]


def test_merge_dynamic_process_flows_updates_existing_apparatus_time_limit(monkeypatch):
    monkeypatch.setattr(app, "PROCESS_CONFIG", {"MSE": [("REVISIÓN DE ARCHIVOS", "<5 hrs")]})
    monkeypatch.setattr(app, "APARATO_OPTIONS", ["MSE"])

    merged_config, _, changed = app.merge_dynamic_process_flows(
        {"MSE": [("REVISIÓN DE ARCHIVOS", "<8 hrs")]}
    )

    assert changed is True
    assert merged_config["MSE"] == [("REVISIÓN DE ARCHIVOS", "<8 hrs")]


def test_merge_dynamic_process_flows_reports_no_change_when_sheet_matches_code(monkeypatch):
    monkeypatch.setattr(app, "PROCESS_CONFIG", {"MSE": [("ORDEN RECIBIDA", None)]})
    monkeypatch.setattr(app, "APARATO_OPTIONS", ["MSE"])

    _, _, changed = app.merge_dynamic_process_flows({"MSE": [("ORDEN RECIBIDA", None)]})

    assert changed is False


def test_merge_dynamic_process_flows_matches_existing_apparatus_regardless_of_case(monkeypatch):
    monkeypatch.setattr(app, "PROCESS_CONFIG", {"HYRAX": [("ORDEN RECIBIDA", None)]})
    monkeypatch.setattr(app, "APARATO_OPTIONS", ["HYRAX"])

    # La hoja trae "Hyrax" con otra capitalización: debe actualizar la
    # entrada existente, no crear un "Hyrax" duplicado junto a "HYRAX".
    merged_config, merged_options, changed = app.merge_dynamic_process_flows(
        {"Hyrax": [("ORDEN RECIBIDA", None), ("REVISIÓN DE ARCHIVOS", "<5 hrs")]}
    )

    assert changed is True
    assert merged_options == ["HYRAX"]
    assert merged_config["HYRAX"] == [("ORDEN RECIBIDA", None), ("REVISIÓN DE ARCHIVOS", "<5 hrs")]
    assert "Hyrax" not in merged_config


def test_apparatus_with_own_sheet_column_ignores_process_alias(monkeypatch):
    # La hoja trae columna Hyrax (con EN PLANEACIÓN) y PIEZA SINTERIZADA (sin
    # ella): HYRAX debe seguir su propia columna, no la del alias.
    hyrax = [("REVISIÓN DE ARCHIVOS", "<5 hrs"), ("PAGO CONFECCIÓN", None),
             ("EN PLANEACIÓN", "<3 dias"), ("ELABORACIÓN PLATINA", "<1 hr")]
    pieza = [status for status in hyrax if status[0] != "EN PLANEACIÓN"]
    monkeypatch.setattr(app, "PROCESS_CONFIG", {"PIEZA SINTERIZADA": app.PIEZA_SINTERIZADA_FLOW})
    monkeypatch.setattr(app, "APARATO_OPTIONS", ["HYRAX", "TRAMPA LINGUAL"])
    merged_config, _, _ = app.merge_dynamic_process_flows(
        {"Hyrax": hyrax, "PIEZA SINTERIZADA": pieza}
    )
    monkeypatch.setattr(app, "PROCESS_CONFIG", merged_config)
    app._cached_process_flow.cache_clear()
    try:
        assert app.get_process_flow("HYRAX") == hyrax
        assert "EN PLANEACIÓN" in app.get_allowed_next_statuses(
            "HYRAX", "REVISIÓN DE ARCHIVOS", "Admin"
        )
        assert "EN PLANEACIÓN" in app.get_allowed_next_statuses(
            "HYRAX", "PAGO CONFECCIÓN", "Admin"
        )
        # Sin columna propia, TRAMPA LINGUAL sigue usando PIEZA SINTERIZADA.
        assert app.get_process_flow("TRAMPA LINGUAL") == pieza
    finally:
        app._cached_process_flow.cache_clear()


def test_merge_dynamic_process_flows_uppercases_genuinely_new_apparatus(monkeypatch):
    monkeypatch.setattr(app, "PROCESS_CONFIG", {})
    monkeypatch.setattr(app, "APARATO_OPTIONS", [])

    merged_config, merged_options, changed = app.merge_dynamic_process_flows(
        {"Botón de Nance": [("ORDEN RECIBIDA", None)]}
    )

    assert changed is True
    assert merged_options == ["BOTÓN DE NANCE"]
    assert merged_config == {"BOTÓN DE NANCE": [("ORDEN RECIBIDA", None)]}


def test_refresh_dynamic_process_catalog_gives_each_new_apparatus_a_distinct_color(monkeypatch):
    original_config = dict(app.PROCESS_CONFIG)
    original_options = list(app.APARATO_OPTIONS)
    original_palette = dict(app.SHEET_STYLE_COLORS[app.APARATO_COLUMN])
    try:
        monkeypatch.setattr(
            app,
            "read_process_matrix_values",
            lambda: [
                ["BOTÓN DE NANCE", "", "ARCO TRANSPALATINO", ""],
                ["Fases", "Tiempo", "Fases", "Tiempo"],
                ["ORDEN RECIBIDA", "", "ORDEN RECIBIDA", ""],
            ],
        )

        app.refresh_dynamic_process_catalog()

        palette = app.SHEET_STYLE_COLORS[app.APARATO_COLUMN]
        botom_color = palette["BOTÓN DE NANCE"]
        arco_color = palette["ARCO TRANSPALATINO"]
        assert botom_color != arco_color
        # Ninguno de los dos se queda en el gris plano que se veía "en blanco".
        assert botom_color != ("#E6E6E6", "#333333")
        assert arco_color != ("#E6E6E6", "#333333")
    finally:
        app.PROCESS_CONFIG.clear()
        app.PROCESS_CONFIG.update(original_config)
        app.APARATO_OPTIONS[:] = original_options
        app.SHEET_STYLE_COLORS[app.APARATO_COLUMN].clear()
        app.SHEET_STYLE_COLORS[app.APARATO_COLUMN].update(original_palette)
        app.PROCESS_STATUS_VALUES[:] = [
            *dict.fromkeys(status for flow in app.PROCESS_CONFIG.values() for status, _ in flow),
            "CANCELO",
        ]
        app._apparatus_components_from_text.cache_clear()
        app._canonical_apparatus_from_text.cache_clear()
        app._cached_process_flow.cache_clear()
        app.apparatus_flow_catalog.cache_clear()


def test_refresh_dynamic_process_catalog_adds_apparatus_and_clears_flow_caches(monkeypatch):
    original_config = dict(app.PROCESS_CONFIG)
    original_options = list(app.APARATO_OPTIONS)
    try:
        monkeypatch.setattr(
            app,
            "read_process_matrix_values",
            lambda: [
                ["ARCO TRANSPALATINO", ""],
                ["Fases", "Tiempo"],
                ["ORDEN RECIBIDA", ""],
                ["REVISIÓN DE ARCHIVOS", "<5 hrs"],
            ],
        )
        app.get_process_flow("ARCO TRANSPALATINO")  # llena los lru_cache con el flujo vacío previo

        app.refresh_dynamic_process_catalog()

        assert app.PROCESS_CONFIG["ARCO TRANSPALATINO"] == [
            ("ORDEN RECIBIDA", None),
            ("REVISIÓN DE ARCHIVOS", "<5 hrs"),
        ]
        assert "ARCO TRANSPALATINO" in app.APARATO_OPTIONS
        assert app.get_process_flow("ARCO TRANSPALATINO") == [
            ("ORDEN RECIBIDA", None),
            ("REVISIÓN DE ARCHIVOS", "<5 hrs"),
        ]
    finally:
        app.PROCESS_CONFIG.clear()
        app.PROCESS_CONFIG.update(original_config)
        app.APARATO_OPTIONS[:] = original_options
        app.PROCESS_STATUS_VALUES[:] = [
            *dict.fromkeys(status for flow in app.PROCESS_CONFIG.values() for status, _ in flow),
            "CANCELO",
        ]
        app._apparatus_components_from_text.cache_clear()
        app._canonical_apparatus_from_text.cache_clear()
        app._cached_process_flow.cache_clear()
        app.apparatus_flow_catalog.cache_clear()


def test_refresh_dynamic_process_catalog_keeps_static_config_when_sheet_fails(monkeypatch):
    original_config = dict(app.PROCESS_CONFIG)
    monkeypatch.setattr(app, "read_process_matrix_values", lambda: (_ for _ in ()).throw(RuntimeError("boom")))

    app.refresh_dynamic_process_catalog()

    assert app.PROCESS_CONFIG == original_config


# --- Agenda del pedido: siguiente etapa, próxima fecha y entrega estimada ---

# Flujo de MSE tal como está hoy en PROCESOS POR APARATO (no el del código).
LIVE_MSE_FLOW = [
    ("ORDEN RECIBIDA", None), ("REVISIÓN DE ARCHIVOS", "<5 hrs"),
    ("ESCANEO MAL (EN REPETICIÓN)", None), ("PAGO PLANEACIÓN", None),
    ("EN PLANEACIÓN", "<3 dias"), ("REVISIÓN PLAN DOCTOR", None),
    ("SOLICITUD DE CAMBIOS", "<3 dias"), ("PAGO CONFECCIÓN", None),
    ("ELABORACIÓN PLATINA", "<1 hr"), ("EN SINTERIZADO Y HORNEADO", "<1 dia"),
    ("LISTO P/CONFECCIÓN", "<1 dia"), ("EN CONFECCIÓN", "<3 hrs"),
    ("CONTROL DE CALIDAD Y FOTOEVIDENCIA", "<1 hr"), ("LISTO P/EMPAQUETADO", "<1 hr"),
    ("GENERACIÓN DE GUÍA", "<1 hr"), ("EMPACADO/LISTO P/ENVÍO", "<1 hr"),
    ("PRODUCTO ENVIADO", "<1 hr"), ("ENVÍO DE ENCUESTA", "<3 dias"),
]
OCT_NOW = datetime(2026, 10, 7, 16)  # miércoles


def _clear_flow_caches():
    app._apparatus_components_from_text.cache_clear()
    app._canonical_apparatus_from_text.cache_clear()
    app._cached_process_flow.cache_clear()
    app.apparatus_flow_catalog.cache_clear()


@pytest.fixture
def live_mse(monkeypatch):
    monkeypatch.setattr(app, "PROCESS_CONFIG", {**app.PROCESS_CONFIG, "MSE": LIVE_MSE_FLOW})
    monkeypatch.setattr(app, "app_now", lambda: OCT_NOW)
    _clear_flow_caches()
    yield
    _clear_flow_caches()


def stage_log(status, start, limit="", configured="", identifier="001"):
    return {app.ID_COLUMN: identifier, app.STATUS_COLUMN: status, "FECHA_INICIO": start.strftime("%Y-%m-%d"),
            "HORA_INICIO": start.strftime("%H:%M:%S"), "FECHA_FIN": "",
            "TIEMPO_MAXIMO_HORAS": limit, "TIEMPO_CONFIGURADO": configured}


def schedule_row(status, log_entry, **extra):
    table = app.build_workbench_table(pd.DataFrame([case(status=status, **extra)]),
                                      pd.DataFrame([log_entry] if log_entry else []))
    return table.iloc[0]


def test_planning_shows_stefano_return_and_estimated_delivery(live_mse):
    row = schedule_row("EN PLANEACIÓN", stage_log("EN PLANEACIÓN", datetime(2026, 10, 6, 13, 56, 54),
                                                    "72", "<3 dias"))
    assert row["SIGUIENTE ETAPA"] == "REVISIÓN PLAN DOCTOR"
    # Martes 06/10 13:56 + 3 días hábiles = viernes 09/10 13:56.
    assert row["PRÓXIMA FECHA"] == "Regresa Stefano: vie 09/10 13:56"
    assert row["LÍMITE ETAPA"] == "2026-10-09 13:56:54"
    assert row["SEMÁFORO"] == "🟢 En tiempo"
    # Desde vie 09/10 13:56: platina +1 h (14:56), sinterizado +1 día (lun 12/10),
    # listo p/confección +1 día (mar 13/10 14:56), confección +3 h (17:56) y
    # calidad, empaquetado, guía y empacado +1 h c/u (21:56). Revisión del doctor
    # y pagos suman 0; SOLICITUD DE CAMBIOS es opcional y no se cuenta.
    assert row["ENTREGA ESTIMADA"] == "mar 13/10 (+ revisión doctor)"


def test_stefano_return_counts_from_recorded_send_day(live_mse, monkeypatch):
    monkeypatch.setattr(app, "app_now", lambda: datetime(2026, 10, 9, 15))
    started = stage_log("EN PLANEACIÓN", datetime(2026, 10, 6, 13, 56, 54), "72", "<3 dias")
    late = schedule_row("EN PLANEACIÓN", started)
    assert late["SEMÁFORO"] == "🔴 Atrasado"
    sent = schedule_row("EN PLANEACIÓN", started, **{"FECHA/HORA ENVÍO STEFANO": "2026/10/07 10:00"})
    # Jime registró el envío el miércoles: regresa el lunes y aún va a tiempo.
    assert sent["PRÓXIMA FECHA"] == "Regresa Stefano: lun 12/10 10:00"
    assert sent["LÍMITE ETAPA"] == "2026-10-12 10:00:00"
    assert sent["HORAS EN ETAPA"] == 53
    assert sent["SEMÁFORO"] == "🟢 En tiempo"


@pytest.mark.parametrize("sent", ["2026/10/01 09:00", "25 NOVIEMBRE 9:42 AM", "camilo"])
def test_stale_future_or_garbled_stefano_send_is_ignored(live_mse, sent):
    row = schedule_row("EN PLANEACIÓN", stage_log("EN PLANEACIÓN", datetime(2026, 10, 6, 13, 56, 54),
                                                    "72", "<3 dias"),
                       **{"FECHA/HORA ENVÍO STEFANO": sent})
    assert row["PRÓXIMA FECHA"] == "Regresa Stefano: vie 09/10 13:56"


def test_doctor_wait_is_never_late_and_shows_since_when(live_mse):
    # Aunque la hoja le diera 5 horas a la revisión, esperar al doctor no se pinta.
    row = schedule_row("REVISIÓN PLAN DOCTOR",
                       stage_log("REVISIÓN PLAN DOCTOR", datetime(2026, 10, 5, 15, 21), "5", "<5 hrs"))
    assert row["SEMÁFORO"] == "⚪ Sin medición"
    assert "Esperando doctor" in row["DETALLE SEMÁFORO"]
    assert row["PRÓXIMA FECHA"] == "Esperando doctor desde lun 05/10 (2 días hábiles)"
    assert row["LÍMITE ETAPA"] == "—"
    assert row["SIGUIENTE ETAPA"] == "PAGO CONFECCIÓN"
    assert row["ENTREGA ESTIMADA"].endswith("(+ revisión doctor)")


def test_doctor_wait_label_uses_singular_day(live_mse):
    row = schedule_row("REVISIÓN PLAN DOCTOR",
                       stage_log("REVISIÓN PLAN DOCTOR", datetime(2026, 10, 6, 15, 0)))
    assert row["PRÓXIMA FECHA"] == "Esperando doctor desde mar 06/10 (1 día hábil)"


def test_payment_does_not_stop_estimated_delivery(live_mse):
    row = schedule_row("PAGO CONFECCIÓN", stage_log("PAGO CONFECCIÓN", datetime(2026, 10, 6, 9)))
    assert row["PRÓXIMA FECHA"] == "Esperando pago desde mar 06/10"
    # Desde ahora (mié 07/10 16:00): platina 17:00, +1 día jue, +1 día vie 17:00,
    # +3 h 20:00 y +4 h hábiles que cruzan el fin de semana: lunes 12/10.
    assert row["ENTREGA ESTIMADA"] == "lun 12/10"


def test_optional_change_request_counts_as_stefano_round(live_mse):
    row = schedule_row("SOLICITUD DE CAMBIOS",
                       stage_log("SOLICITUD DE CAMBIOS", datetime(2026, 10, 7, 9), "72", "<3 dias"))
    assert row["SIGUIENTE ETAPA"] == "PAGO CONFECCIÓN"
    assert row["PRÓXIMA FECHA"] == "Regresa Stefano: lun 12/10 09:00"
    assert row["ENTREGA ESTIMADA"] == "mié 14/10"


def test_lab_stage_without_log_projects_from_now(live_mse):
    row = schedule_row("LISTO P/CONFECCIÓN", None)
    assert row["PRÓXIMA FECHA"] == "—"
    assert row["SIGUIENTE ETAPA"] == "EN CONFECCIÓN"
    # La etapa actual cuenta completa desde ahora: +1 día (jue 16:00), +3 h y +4 h.
    assert row["ENTREGA ESTIMADA"] == "jue 08/10"


@pytest.mark.parametrize("status,expected", [
    ("CANCELO", "—"), (app.PAUSED_STATUS, "En pausa"), ("ETAPA QUE NO EXISTE", "—"),
])
def test_estimated_delivery_for_closed_or_unknown_status(live_mse, status, expected):
    timing = app.current_stage_timing("MSE", status, {}, now=OCT_NOW)
    assert app.estimated_delivery_label("MSE", status, timing, OCT_NOW) == expected


def test_shipped_order_shows_ship_day(live_mse):
    timing = app.current_stage_timing("MSE", "PRODUCTO ENVIADO", {}, now=OCT_NOW)
    assert app.estimated_delivery_label("MSE", "PRODUCTO ENVIADO", timing, OCT_NOW,
                                        "2026/10/05") == "Enviado lun 05/10"
    assert app.get_next_normal_status("MSE", "ENVÍO DE ENCUESTA") == ""


def test_schedule_columns_are_read_only_and_follow_stage_limit():
    columns = [*app.WORKBENCH_COMPUTED_COLUMNS, "PAGO", "FECHA PARA ENTREGA"]
    editable = app.workbench_editable_columns("Admin", columns)
    assert not editable & {*app.WORKBENCH_SCHEDULE_COLUMNS, "FECHA PARA ENTREGA"}
    position = app.WORKBENCH_COMPUTED_COLUMNS.index("LÍMITE ETAPA")
    assert app.WORKBENCH_COMPUTED_COLUMNS[position + 1:position + 4] == app.WORKBENCH_SCHEDULE_COLUMNS


def test_saved_column_order_places_new_schedule_columns_after_stage_limit():
    available = ["PAGO", "STATUS", "HORAS EN ETAPA", "LÍMITE ETAPA", *app.WORKBENCH_SCHEDULE_COLUMNS,
                 "DETALLE SEMÁFORO", "NOMBRE DOCTOR"]
    saved = ["PAGO", "STATUS", "LÍMITE ETAPA", "DETALLE SEMÁFORO", "NOMBRE DOCTOR", "HORAS EN ETAPA"]
    order = app.normalize_column_order(saved, available)
    anchor = order.index("LÍMITE ETAPA")
    assert order[anchor + 1:anchor + 4] == app.WORKBENCH_SCHEDULE_COLUMNS
    # Quien ya movió una columna nueva conserva su lugar.
    moved = app.normalize_column_order([*saved, "ENTREGA ESTIMADA"], available)
    assert moved[-1] == "ENTREGA ESTIMADA"


def test_payment_save_never_writes_delivery_date():
    changes = app.build_payment_estatus_changes(current_status="PAGO CONFECCIÓN", payment_status="TOTAL",
                                                can_advance=True, today=datetime(2026, 10, 7).date())
    assert changes == {"PAGO": "TOTAL", "FECHA PAGO CONFECCION": "2026/10/07"}


# Otras pruebas (AppTest) reemplazan estas funciones en el módulo; se guardan
# las originales al importar.
ORIGINAL_UPDATE_ROW = app.update_row_by_columna_1
ORIGINAL_APPEND_ROW = app.append_estatus_row


def test_sheet_writers_drop_formula_delivery_column(monkeypatch):
    writes = []
    headers = [app.ID_COLUMN, "APARATO", "STATUS", "NOMBRE DOCTOR", "FECHA PARA ENTREGA"]
    class Sheet:
        def get_all_values(self):
            return [["titulo"], headers, ["001", "MSE", "EN PLANEACIÓN", "Original", "2026/10/14"], [""]]
        def update_cells(self, cells, **kwargs): writes.extend(cells)
    monkeypatch.setattr(app, "get_worksheet", lambda _: Sheet())
    monkeypatch.setattr(app, "apply_estatus_row_styles", lambda *args: None)
    result = ORIGINAL_UPDATE_ROW("001", {"NOMBRE DOCTOR": "Otro", "FECHA PARA ENTREGA": "2026/10/20"})
    assert result["success"] and result["protected_columns"] == ["FECHA PARA ENTREGA"]
    assert [(cell.row, cell.col) for cell in writes] == [(3, 4)]
    only_formula = ORIGINAL_UPDATE_ROW("001", {"FECHA PARA ENTREGA": "2026/10/20"})
    assert not only_formula["success"] and "la calcula la hoja" in only_formula["error"]
    writes.clear()
    ORIGINAL_APPEND_ROW({app.ID_COLUMN: "002", "APARATO": "MSE", "FECHA PARA ENTREGA": ""})
    assert all(cell.col != 5 for cell in writes) and writes


def test_doctor_wait_label_says_today_on_same_day(live_mse):
    row = schedule_row("REVISIÓN PLAN DOCTOR",
                       stage_log("REVISIÓN PLAN DOCTOR", datetime(2026, 10, 7, 10, 42)))
    assert row["PRÓXIMA FECHA"] == "Esperando doctor desde mié 07/10 (hoy)"


def test_date_only_stefano_send_shows_day_only(live_mse):
    # Sin registro de tiempos, el envío capturado sólo con el día marca el inicio.
    row = schedule_row("EN PLANEACIÓN", None, **{"FECHA/HORA ENVÍO STEFANO": "2026/10/06"})
    assert row["PRÓXIMA FECHA"] == "Regresa Stefano: vie 09/10"


def test_special_payment_alert_does_not_paint_doctor_wait(live_mse, monkeypatch):
    monkeypatch.setattr(app, "get_special_payment_sla_alert_state",
                        lambda *args: "Atrasado - Pago planeación > 10 días hábiles sin GUÍA PSM + PSM ENVIADA")
    waiting = schedule_row("REVISIÓN PLAN DOCTOR", stage_log("REVISIÓN PLAN DOCTOR", datetime(2026, 10, 5, 9)))
    assert waiting["SEMÁFORO"] == "⚪ Sin medición"
    assert "Pago planeación" in waiting["DETALLE SEMÁFORO"]
    working = schedule_row("EN PLANEACIÓN", stage_log("EN PLANEACIÓN", datetime(2026, 10, 7, 9), "72", "<3 dias"))
    assert working["SEMÁFORO"] == "🔴 Atrasado"
