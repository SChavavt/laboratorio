"""Regresiones de edición por identidad, borradores y permisos de acciones en lote."""
from datetime import date
from pathlib import Path
import sys

import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import order_detail as detail
import lab_pg as lab
import alineadores_pg as align
from workbench_grid import BUSINESS_ORDER as APPARATUS_PINNED
from aligners_grid import BUSINESS_ORDER as ALIGNERS_PINNED


ROOT = str(Path(__file__).resolve().parents[1])


@pytest.fixture(autouse=True)
def restore_demo_overrides():
    # AppTest comparte los módulos importados con pytest; sus fakes no deben
    # sobrevivir al caso que los instaló ni sustituir el backend de otras pruebas.
    overrides = {
        lab: ('require_authenticated_user', 'workbench_tab_options', 'ensure_tiempos_headers',
              'read_sheet_df', 'get_latest_estefano_files', 'get_workbench_form_catalog', 'save_workbench_changes'),
        align: ('get_order_form_catalog', 'save_workbench_changes'),
    }
    original = {(module, name): getattr(module, name) for module, names in overrides.items() for name in names}
    yield
    for (module, name), value in original.items():
        setattr(module, name, value)


def test_fields_start_with_pinned_business_columns_and_exclude_computed_and_totals():
    columns = [lab.ID_COLUMN, *APPARATUS_PINNED, 'VENDEDOR', 'FECHA ENVÍO', 'SEMÁFORO', '_SHEET_ROW']
    assert detail.detail_columns(columns, lab.workbench_editable_columns('Jime', columns), APPARATUS_PINNED) == [*APPARATUS_PINNED, 'VENDEDOR', 'FECHA ENVÍO']
    columns = [align.ID_COLUMN, *ALIGNERS_PINNED, 'SERVICIO', 'FECHA ENVÍO_2', 'Total_2', '_SHEET_ROW']
    assert detail.detail_columns(columns, align.aligners_editable_columns(columns), ALIGNERS_PINNED) == [*ALIGNERS_PINNED, 'SERVICIO', 'FECHA ENVÍO_2']


def test_catalog_reads_unused_values_and_cell_validation_without_private_row_values():
    metadata = {'sheets': [{'properties': {'title': 'Pedidos'}, 'tables': [{
        'range': {'startRowIndex': 1}, 'columnProperties': [{'columnName': 'SERVICIO',
        'dataValidationRule': {'condition': {'type': 'ONE_OF_LIST', 'values': [
            {'userEnteredValue': 'Nuevo servicio'}, {'userEnteredValue': 'No aplica '}]}}}]}],
        'data': [{'startRow': 1, 'rowData': [{'values': [{'formattedValue': 'VENDEDOR'}]},
            {'values': [{'formattedValue': 'Dato de pedido', 'dataValidation': {'condition': {
                'type': 'ONE_OF_LIST', 'values': [{'userEnteredValue': 'Vendedor nuevo'}]}}}]}]}]}]}
    assert detail.parse_sheet_catalogs(metadata)['Pedidos'] == {
        'SERVICIO': ['Nuevo servicio', 'No aplica '], 'VENDEDOR': ['Vendedor nuevo']}


def test_common_stages_respect_every_product_and_user():
    definitions = align.parse_process_matrix([
        ['Graphy', '', 'CONVENC.', ''], ['Fases', 'Tiempo', 'Fases', 'Tiempo'],
        ['REVISIÓN DE ARCHIVOS', '1 día', 'REVISIÓN DE ARCHIVOS', '1 día'],
        ['EN PLANEACIÓN', '1 día', 'POR HACER SETUP', '1 día'],
        ['IMPRESIÓN EN PAUSA', '', 'IMPRESIÓN EN PAUSA', '']])
    rows = pd.DataFrame([
        {align.ID_COLUMN: '001', align.PRODUCT_COLUMN: product, align.STATUS_COLUMN: 'REVISIÓN DE ARCHIVOS'}
        for product in ['Graphy', 'CONVENC.']])
    options = {align.status_key(value) for value in align.aligners_batch_stages(rows, definitions)}
    assert 'EN PLANEACION' not in options
    assert 'POR HACER SETUP' not in options
    assert 'IMPRESION EN PAUSA' in options
    rows = pd.DataFrame([{lab.ID_COLUMN: str(i), lab.APARATO_COLUMN: 'MSE', lab.STATUS_COLUMN: stage}
                         for i, stage in enumerate(['REVISIÓN DE ARCHIVOS', 'EN PLANEACIÓN'])])
    for user in lab.APP_USERS:
        for target in lab.workbench_batch_stages(rows, user):
            for _, row in rows.iterrows():
                assert target in [lab.normalize_status_alias(value) for value in lab.workbench_stage_options(row, user)]


def test_invalid_bulk_is_rejected_before_first_write(monkeypatch):
    definitions = align.parse_process_matrix([['Graphy', ''], ['Fases', 'Tiempo'],
        ['REVISIÓN DE ARCHIVOS', '1 día'], ['EN PLANEACIÓN', '1 día'], ['ENVIADO', '']])
    source = pd.DataFrame([
        {align.ID_COLUMN: '001', align.PRODUCT_COLUMN: 'Graphy', align.STATUS_COLUMN: 'REVISIÓN DE ARCHIVOS'},
        {align.ID_COLUMN: '002', align.PRODUCT_COLUMN: 'Graphy', align.STATUS_COLUMN: 'ENVIADO'}])
    edited = source.copy()
    edited[align.STATUS_COLUMN] = 'EN PLANEACIÓN'
    writes = []
    monkeypatch.setattr(align, 'update_order_row', lambda *a, **k: writes.append(a))
    saved, errors = align.save_workbench_changes(source, source, edited, 'Jime', definitions)
    assert not saved and errors and writes == []


def test_sheet_catalog_allows_new_option_and_rejects_arbitrary_value(monkeypatch):
    row = {lab.ID_COLUMN: '001', lab.APARATO_COLUMN: 'MSE', lab.STATUS_COLUMN: 'REVISIÓN DE ARCHIVOS',
           'SERVICIO': 'PLANEACIÓN'}
    source = pd.DataFrame([row])
    catalog = {'SERVICIO': ['PLANEACIÓN', 'SERVICIO NUEVO']}
    assert lab.validate_workbench_changes(source, source, [('001', {'SERVICIO': 'SERVICIO NUEVO'})], 'Jime', select_catalog=catalog) == []
    assert lab.validate_workbench_changes(source, source, [('001', {'SERVICIO': 'INVALIDO'})], 'Jime', select_catalog=catalog)


def button(at, label):
    return next(item for item in at.button if item.label == label)


def field(at, label):
    return next(item for item in at.selectbox if label in item.label)


APPARATUS_SCRIPT = f'''
import sys
sys.path.insert(0, {ROOT!r})
import pandas as pd
import streamlit as st
import lab_pg as app
st.session_state.setdefault("demo_rows", [
    {{app.ID_COLUMN: "002", app.APARATO_COLUMN: "MSE", app.STATUS_COLUMN: "REVISIÓN DE ARCHIVOS",
      "NOMBRE DOCTOR": "Cliente B", "NOMBRE PACIENTE": "Paciente B", "VENDEDOR": "JIMENA",
      "SERVICIO": "PLANEACIÓN", "FECHA ENVÍO": "", "FECHA/HORA ENVÍO STEFANO": "",
      "DETALLES & COMENTARIOS FINALES": "Sin cambio"}},
    {{app.ID_COLUMN: "001", app.APARATO_COLUMN: "MSE", app.STATUS_COLUMN: "REVISIÓN DE ARCHIVOS",
      "NOMBRE DOCTOR": "Cliente A", "NOMBRE PACIENTE": "Paciente A", "VENDEDOR": "JIMENA",
      "SERVICIO": "PLANEACIÓN", "FECHA ENVÍO": "valor antiguo", "FECHA/HORA ENVÍO STEFANO": "",
      "DETALLES & COMENTARIOS FINALES": "Sin cambio"}}])
app.require_authenticated_user = lambda: "Jime"
app.workbench_tab_options = lambda _: ["📋 Seguimiento"]
app.ensure_tiempos_headers = lambda: None
app.read_sheet_df = lambda name: pd.DataFrame(st.session_state.demo_rows) if name == app.SHEET_ESTATUS else pd.DataFrame()
app.read_sheet_df.clear = lambda: None
app.get_latest_estefano_files = lambda _: ""
app.get_workbench_form_catalog = lambda: {{"VENDEDOR": ["JIMENA", "MICHELLE"], "SERVICIO": ["PLANEACIÓN", "SERVICIO NUEVO"]}}
def save(original, edited, user, **kwargs):
    changes = app.workbench_changes(original, edited)
    st.session_state["attempt"] = changes
    if st.session_state.get("fail_save"):
        return [], ["Otro usuario cambió el pedido"]
    for identifier, delta in changes:
        for row in st.session_state.demo_rows:
            if row[app.ID_COLUMN] == identifier:
                row.update(delta)
    app.reset_workbench()
    return [identifier for identifier, _ in changes], []
app.save_workbench_changes = save
app.main()
'''


def apparatus_app(selected=('001',)):
    at = AppTest.from_string(APPARATUS_SCRIPT, default_timeout=15).run()
    key = at.session_state['workbench_editor_key']
    rows = at.session_state['workbench_editor_baseline'].to_dict('records')
    for row in rows:
        row['SELECCIONAR'] = row[lab.ID_COLUMN] in selected
    at.session_state[key] = {'rows': rows}
    return at.run()


def test_form_saves_only_selected_id_and_changed_fields_then_stays_open():
    at = apparatus_app()
    assert not at.exception
    assert any(item.value == 'Pedido · Cliente A' for item in at.subheader)
    assert button(at, '💾 Guardar este pedido').disabled
    assert any('NOMBRE DOCTOR' in item.label for item in at.text_input)
    field(at, 'VENDEDOR').select('MICHELLE').run()
    assert not at.exception
    assert at.text_input(key='workbench_search').disabled
    assert button(at, 'Confección en pausa').disabled
    assert not button(at, '💾 Guardar este pedido').disabled
    button(at, '💾 Guardar este pedido').click().run()
    assert not at.exception
    assert at.session_state['attempt'] == [('001', {'VENDEDOR': 'MICHELLE'})]
    assert at.session_state['demo_rows'][0]['VENDEDOR'] == 'JIMENA'
    assert any(item.value == 'Pedido · Cliente A' for item in at.subheader)
    assert button(at, '💾 Guardar este pedido').disabled
    assert not at.text_input(key='workbench_search').disabled


def test_draft_survives_uncheck_and_failed_save_and_can_be_discarded():
    at = apparatus_app()
    field(at, 'VENDEDOR').select('MICHELLE').run()
    key = at.session_state['workbench_editor_key']
    rows = at.session_state['workbench_editor_baseline'].to_dict('records')
    for row in rows:
        row['SELECCIONAR'] = False
    at.session_state[key] = {'rows': rows}
    at.run()
    assert field(at, 'VENDEDOR').value == 'MICHELLE'
    at.session_state['fail_save'] = True
    button(at, '💾 Guardar este pedido').click().run()
    assert not at.exception
    assert field(at, 'VENDEDOR').value == 'MICHELLE'
    assert any('Otro usuario' in item.value for item in at.error)
    button(at, '↩️ Descartar esta edición').click().run()
    assert not at.exception
    assert not at.text_input(key='workbench_search').disabled
    assert at.session_state['demo_rows'][1]['VENDEDOR'] == 'JIMENA'


def test_multi_selection_can_choose_one_form_without_clearing_other_checks():
    at = apparatus_app(('001', '002'))
    assert not at.exception
    chooser = field(at, 'Pedido para editar')
    chooser.select('001').run()
    assert any(item.value == 'Pedido · Cliente A' for item in at.subheader)
    assert at.session_state['workbench_selected_ids'] == ['002', '001']
    assert '002' not in next(item for item in at.subheader if item.value.startswith('Pedido')).value
    field(at, 'VENDEDOR').select('MICHELLE').run()
    assert field(at, 'Pedido para editar').disabled
    assert not any(item.key == 'apparatus_bulk_apply' for item in at.button)
    button(at, '↩️ Descartar esta edición').click().run()
    assert field(at, 'VENDEDOR').value == 'JIMENA'
    assert button(at, '💾 Guardar este pedido').disabled


def test_refresh_reloads_clean_widgets_and_now_sets_only_one_datetime():
    at = apparatus_app()
    rows = at.session_state['demo_rows']
    rows[1]['VENDEDOR'] = 'MICHELLE'
    at.session_state['demo_rows'] = rows
    button(at, 'Actualizar datos').click().run()
    assert not at.exception
    assert field(at, 'VENDEDOR').value == 'MICHELLE'
    assert button(at, '💾 Guardar este pedido').disabled
    button(at, 'Ahora').click().run()
    button(at, '💾 Guardar este pedido').click().run()
    assert not at.exception
    [(identifier, delta)] = at.session_state['attempt']
    assert identifier == '001'
    assert set(delta) == {'FECHA/HORA ENVÍO STEFANO'}
    assert lab.parse_spanish_datetime(delta['FECHA/HORA ENVÍO STEFANO']) is not None


@pytest.mark.parametrize('sheet', [align.SHEET_ORDERS, align.SHEET_POLANCO])
def test_aligner_editor_scoped_to_current_sheet_with_calendar_and_raw_options(sheet):
    script = f'''
import sys
sys.path.insert(0, {ROOT!r})
import pandas as pd
import streamlit as st
import alineadores_pg as app
st.session_state["aligners_order_sheet"] = {sheet!r}
st.session_state.setdefault("row", {{app.ID_COLUMN: "0001", app.STATUS_COLUMN: "REVISIÓN DE ARCHIVOS",
    app.PRODUCT_COLUMN: "GRAPHY", "NOMBRE DOCTOR": "Cliente demo", "NOMBRE PACIENTE": "Paciente demo",
    "VENDEDOR": "JIMENA", "PAQUETE MARCA BLANCA": "Lite", "FECHA OBJETIVO ENVÍO": "", "ADEUDO": "200"}})
app.get_order_form_catalog = lambda: {{"VENDEDOR": ["JIMENA", "MICHELLE"], "PAQUETE MARCA BLANCA": ["Lite", "NO APLICA "]}}
def save(source, baseline, edited, user, definitions, **kwargs):
    changes = app.grid_changes(baseline, edited, definitions)
    app.order_detail.restore_catalog_values(changes, kwargs.get("select_catalog"))
    st.session_state["attempt"] = (app.current_order_sheet(), changes)
    st.session_state.row.update(dict(changes)["0001"])
    return ["0001"], []
app.save_workbench_changes = save
app.render_case_actions(pd.DataFrame([st.session_state.row]), pd.DataFrame(), "Jime", {{}}, False)
'''
    at = AppTest.from_string(script, default_timeout=15).run()
    assert not at.exception
    assert any(item.value == 'Pedido · Cliente demo' for item in at.subheader)
    assert button(at, '💾 Guardar este pedido').disabled
    field(at, 'PAQUETE MARCA BLANCA').select('NO APLICA ').run()
    at.date_input[0].set_value(date(2026, 10, 5)).run()
    button(at, '💾 Guardar este pedido').click().run()
    assert not at.exception
    actual_sheet, changes = at.session_state['attempt']
    assert actual_sheet == sheet
    assert changes == [('0001', {'PAQUETE MARCA BLANCA': 'NO APLICA ', 'FECHA OBJETIVO ENVÍO': '2026-10-05'})]
    assert button(at, '💾 Guardar este pedido').disabled


def test_equal_folios_have_isolated_drafts_between_areas(monkeypatch):
    monkeypatch.setattr(detail.st, 'session_state', {})
    for namespace in ['apparatus', 'aligners', 'polanco_aligners']:
        detail.st.session_state[detail.draft_key(namespace)] = {'001': {'baseline': {}, 'changes': {'VENDEDOR': namespace}}}
    detail.clear_drafts('polanco_aligners')
    assert detail.pending_count('apparatus') == detail.pending_count('aligners') == 1
    assert detail.pending_count('polanco_aligners') == 0


def test_compact_selection_opens_last_checked_order_and_retains_manual_choice():
    at = AppTest.from_string(APPARATUS_SCRIPT, default_timeout=15).run()
    key = at.session_state['workbench_editor_key']
    component = at.get('component_instance')[0].proto.id
    for identifiers, active, customer in [(['001'], '001', 'Cliente A'),
            (['001', '002'], '002', 'Cliente B'), (['001'], None, 'Cliente A'),
            ([], None, None), (['002'], '002', 'Cliente B')]:
        at.session_state[key] = {'selectedIds': identifiers, 'activeId': active, 'editedRows': []}
        at.run()
        assert not at.exception
        assert at.get('component_instance')[0].proto.id == component
        assert not at.text_input(key='workbench_search').disabled
        assert at.session_state['workbench_selected_ids'] == [i for i in ['002', '001'] if i in identifiers]
        if customer:
            assert any(item.value == 'Pedido · ' + customer for item in at.subheader)
        else:
            assert not any(item.value.startswith('Pedido · ') for item in at.subheader)
    at.session_state[key] = {'selectedIds': ['001', '002'], 'activeId': '001', 'editedRows': []}
    at.run()
    field(at, 'Pedido para editar').select('002').run()
    assert any(item.value == 'Pedido · Cliente B' for item in at.subheader)
    # La respuesta de la tabla sigue trayendo activeId=001 en corridas ajenas;
    # no debe deshacer la elección manual del selector de fichas.
    at.run()
    assert any(item.value == 'Pedido · Cliente B' for item in at.subheader)


def test_selecting_planning_order_does_not_fetch_closed_sections():
    script = APPARATUS_SCRIPT.replace('app.main()', '''
app.render_estefano_shipping_tab = lambda *a, **k: (_ for _ in ()).throw(AssertionError("Sección cerrada"))
app.render_pagos_tab = lambda *a, **k: (_ for _ in ()).throw(AssertionError("Sección cerrada"))
def read_counted(name):
    st.session_state['reads'] = st.session_state.get('reads', 0) + 1
    return pd.DataFrame(st.session_state.demo_rows) if name == app.SHEET_ESTATUS else pd.DataFrame()
app.read_sheet_df = read_counted
app.main()
''').replace('"REVISIÓN DE ARCHIVOS"', '"EN PLANEACIÓN"')
    originals = lab.render_estefano_shipping_tab, lab.render_pagos_tab
    try:
        at = AppTest.from_string(script, default_timeout=15).run()
        reads = at.session_state['reads']
        key = at.session_state['workbench_editor_key']
        for identifier in ['001', '002', '001']:
            at.session_state[key] = {'selectedIds': [identifier], 'editedRows': []}
            at.run()
            assert not at.exception
        assert at.session_state['reads'] == reads
    finally:
        lab.render_estefano_shipping_tab, lab.render_pagos_tab = originals


@pytest.mark.parametrize('sheet', [align.SHEET_ORDERS, align.SHEET_POLANCO])
def test_files_form_adds_and_removes_options_without_overwriting_other_fields(sheet):
    script = f'''
import sys
sys.path.insert(0, {ROOT!r})
import pandas as pd
import streamlit as st
import alineadores_pg as app
st.session_state['aligners_order_sheet'] = {sheet!r}
st.session_state.setdefault('row', {{app.ID_COLUMN: '001', app.STATUS_COLUMN: 'REVISIÓN DE ARCHIVOS',
    app.PRODUCT_COLUMN: 'GRAPHY', 'NOMBRE DOCTOR': 'Cliente demo', 'ARCHIVOS RECIBIDOS': 'STLs, TOMO',
    'VENDEDOR': 'JIMENA'}})
app.get_order_form_catalog = lambda: {{'ARCHIVOS RECIBIDOS': ['STLs', 'TOMO', 'FOTO INTRA'], 'VENDEDOR': ['JIMENA', 'MICHELLE']}}
def save(source, baseline, edited, user, definitions, **kwargs):
    changes = app.grid_changes(baseline, edited, definitions)
    st.session_state['attempt'] = changes
    st.session_state.row.update(dict(changes)['001'])
    return ['001'], []
app.save_workbench_changes = save
app.render_aligner_order_editor(pd.Series(st.session_state.row), 'Jime', {{}})
'''
    at = AppTest.from_string(script, default_timeout=15).run()
    assert not at.exception
    files = next(item for item in at.multiselect if 'ARCHIVOS RECIBIDOS' in item.label)
    assert files.value == ['STLs', 'TOMO']
    files.select('FOTO INTRA').unselect('TOMO').run()
    button(at, '💾 Guardar este pedido').click().run()
    assert not at.exception
    assert at.session_state['attempt'] == [('001', {'ARCHIVOS RECIBIDOS': 'STLs, FOTO INTRA'})]
    field(at, 'VENDEDOR').select('MICHELLE').run()
    button(at, '💾 Guardar este pedido').click().run()
    assert at.session_state['attempt'] == [('001', {'VENDEDOR': 'MICHELLE'})]
    assert at.session_state['row']['ARCHIVOS RECIBIDOS'] == 'STLs, FOTO INTRA'


def test_apparatus_primary_fields_and_combination_save_with_secondary_fields():
    at = apparatus_app()
    sections = [item for item in at.expander if item.label in {
        '📋 Servicio y archivos', '📅 Fechas y entrega', '📝 Notas', '💳 Pagos', '📦 Otros datos'}]
    assert sections and all(not item.proto.expanded for item in sections)
    assert not any('NOMBRE DOCTOR' in widget.label for section in sections for widget in section.text_input)
    doctor = next(item for item in at.text_input if 'NOMBRE DOCTOR' in item.label)
    doctor.input('Cliente corregido').run()
    apparatus = next(item for item in at.multiselect if 'APARATO' in item.label)
    apparatus.select('TIGER').run()
    field(at, 'VENDEDOR').select('MICHELLE').run()
    assert not at.exception
    combination = lab.canonical_apparatus_value('MSE + TIGER')
    candidate = pd.Series({lab.ID_COLUMN: '001', lab.APARATO_COLUMN: combination,
                          lab.STATUS_COLUMN: 'REVISIÓN DE ARCHIVOS'})
    expected = lab.workbench_stage_options(candidate, 'Jime')
    assert field(at, 'STATUS').options == [lab.workbench_form_option_label(lab.STATUS_COLUMN, lab.normalize_status_alias(value)) for value in expected]
    button(at, '💾 Guardar este pedido').click().run()
    assert not at.exception
    assert at.session_state['attempt'] == [('001', {
        'APARATO': combination, 'NOMBRE DOCTOR': 'Cliente corregido', 'VENDEDOR': 'MICHELLE'})]
    assert any(item.value == 'Pedido · Cliente corregido' for item in at.subheader)


@pytest.mark.parametrize('sheet', [align.SHEET_ORDERS, align.SHEET_POLANCO])
def test_aligner_primary_product_and_status_follow_the_new_product_flow(sheet):
    matrix = [['Graphy', '', 'CONVENC.', ''], ['Fases', 'Tiempo', 'Fases', 'Tiempo'],
              ['REVISIÓN DE ARCHIVOS', '1 día', 'REVISIÓN DE ARCHIVOS', '1 día'],
              ['EN PLANEACIÓN', '1 día', 'POR HACER SETUP', '1 día']]
    script = f'''
import sys
sys.path.insert(0, {ROOT!r})
import pandas as pd
import streamlit as st
import alineadores_pg as app
st.session_state['aligners_order_sheet'] = {sheet!r}
definitions = app.parse_process_matrix({matrix!r})
st.session_state.setdefault('row', {{app.ID_COLUMN: '001', app.STATUS_COLUMN: 'REVISIÓN DE ARCHIVOS',
    app.PRODUCT_COLUMN: 'GRAPHY', 'NOMBRE DOCTOR': 'Cliente demo', 'NOMBRE PACIENTE': 'Paciente demo',
    'TIPO': 'ARTTDLAB', 'ETAPA SOLICITUD': 'NUEVO', 'DETALLE COMENTARIOS': '',
    'ARCHIVOS RECIBIDOS': 'STLs', 'FECHA OBJETIVO ENVÍO': ''}})
app.get_order_form_catalog = lambda: {{'PRODUCTO': ['GRAPHY', 'CONVENC.'], 'TIPO': ['ARTTDLAB', 'MARCA BLANCA'],
    'ETAPA SOLICITUD': ['NUEVO', 'REFIN'], 'ARCHIVOS RECIBIDOS': ['STLs', 'TOMO']}}
def save(source, baseline, edited, user, definitions, **kwargs):
    changes = app.grid_changes(baseline, edited, definitions)
    errors = app.validate_delta(source.iloc[0], dict(changes)['001'], definitions)
    if errors:
        return [], errors
    st.session_state['attempt'] = changes
    st.session_state.row.update(dict(changes)['001'])
    return ['001'], []
app.save_workbench_changes = save
app.render_aligner_order_editor(pd.Series(st.session_state.row), 'Jime', definitions)
'''
    at = AppTest.from_string(script, default_timeout=15).run()
    assert not at.exception
    assert len(at.expander) == 2 and all(not item.proto.expanded for item in at.expander)
    field(at, 'STATUS').select('EN PLANEACIÓN').run()
    field(at, 'PRODUCTO').select('CONVENC.').run()
    assert not at.exception
    assert field(at, 'STATUS').value == 'REVISIÓN DE ARCHIVOS'
    assert not any('EN PLANEACIÓN' in option for option in field(at, 'STATUS').options)
    field(at, 'STATUS').select('POR HACER SETUP').run()
    next(item for item in at.text_input if 'NOMBRE PACIENTE' in item.label).input('Paciente corregido').run()
    next(item for item in at.multiselect if 'ARCHIVOS RECIBIDOS' in item.label).select('TOMO').run()
    button(at, '💾 Guardar este pedido').click().run()
    assert not at.exception and not at.error
    assert at.session_state['row']['PRODUCTO'] == 'CONVENC.'
    assert at.session_state['row']['STATUS'] == 'POR HACER SETUP'
    assert at.session_state['row']['NOMBRE PACIENTE'] == 'Paciente corregido'
    assert at.session_state['row']['ARCHIVOS RECIBIDOS'] == 'STLs, TOMO'
