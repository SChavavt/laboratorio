"""Consulta y altas de Juan/Paula, sin escrituras operativas ni servicios reales."""
from pathlib import Path
import sys

import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

ROOT = str(Path(__file__).resolve().parents[1])
sys.path.insert(0, ROOT)
import lab_pg as lab
import alineadores_pg as align
from user_permissions import APP_USERS, can_create_orders, can_edit_orders


@pytest.fixture
def definitions():
    return align.parse_process_matrix([
        ['Graphy', ''], ['Fases', 'Tiempo'],
        ['REVISIÓN DE ARCHIVOS', '<1 dia'], ['EN PLANEACIÓN', '<1 dia'], ['ENVIADO', ''],
    ])


def forbidden(*args, **kwargs):
    raise AssertionError('Un usuario de consulta intentó acceder a una escritura.')


@pytest.mark.parametrize('user', ['Juan', 'Paula'])
@pytest.mark.parametrize('delta', [
    {'NOMBRE DOCTOR': 'Cambio'}, {'STATUS': 'EN PLANEACIÓN'},
    {'STATUS': 'CANCELADO'}, {'FECHA ENVÍO': '2026-10-15'},
])
def test_limited_users_cannot_save_existing_orders(user, delta, definitions, monkeypatch):
    apparatus = pd.DataFrame([{lab.ID_COLUMN: '001', 'APARATO': 'MSE',
        'STATUS': 'REVISIÓN DE ARCHIVOS', 'NOMBRE DOCTOR': 'Original', 'FECHA ENVÍO': ''}])
    aligners = pd.DataFrame([{align.ID_COLUMN: '001', 'PRODUCTO': 'Graphy',
        'STATUS': 'REVISIÓN DE ARCHIVOS', 'NOMBRE DOCTOR': 'Original', 'FECHA ENVÍO': ''}])
    monkeypatch.setattr(lab, 'read_sheet_df', forbidden)
    monkeypatch.setattr(lab, 'update_row_by_columna_1', forbidden)
    monkeypatch.setattr(align, 'update_order_row', forbidden)
    for module, source in [(lab, apparatus), (align, aligners)]:
        edited = source.copy()
        for column, value in delta.items():
            edited.loc[0, column] = value
        saved, errors = (module.save_workbench_changes(source, edited, user)
            if module is lab else module.save_workbench_changes(source, source, edited, user, definitions))
        assert not saved and any('permiso' in error for error in errors)


@pytest.mark.parametrize('user', ['Juan', 'Paula'])
def test_limited_users_cannot_pause_reactivate_print_or_repair(user, definitions, monkeypatch):
    monkeypatch.setattr(lab, 'read_sheet_df', forbidden)
    monkeypatch.setattr(align, 'get_worksheet', forbidden)
    row = pd.Series({lab.ID_COLUMN: '001', 'APARATO': 'MSE', 'STATUS': 'LISTO P/SINTERIZADO'})
    assert lab.pause_workbench_cases(['001'], pd.DataFrame([row]), user)[1]
    assert not lab.mark_case_for_printing('001', row, user)['success']
    assert not lab.validate_status_change(identifier='001', apparatus='MSE',
        previous_status='PRODUCTO ENVIADO', new_status='ORDEN RECIBIDA',
        current_user=user, from_sent_archive=True)[0]
    assert not lab.validate_status_change(identifier='001', apparatus='MSE',
        previous_status='REVISIÓN DE ARCHIVOS', new_status='REVISIÓN DE ARCHIVOS', current_user=user)[0]
    with pytest.raises(ValueError, match='permiso'):
        align.initialize_order_timer(pd.Series({align.ID_COLUMN: '001'}), user, definitions)
    with pytest.raises(ValueError, match='permiso'):
        align.register_status_change(identifier='001', product='Graphy', previous_status='EN PLANEACIÓN',
            new_status='ENVIADO', current_user=user, definitions=definitions)


@pytest.mark.parametrize('user', ['Juan', 'Paula'])
@pytest.mark.parametrize('reactivate', [False, True])
def test_grids_are_readonly_and_selection_remains_available(user, reactivate, definitions):
    apparatus = pd.DataFrame([{'SELECCIONAR': False, lab.ID_COLUMN: '001',
        'APARATO': 'MSE', 'STATUS': 'REVISIÓN DE ARCHIVOS', 'NOMBRE DOCTOR': 'Original'}])
    aligners = pd.DataFrame([{'SELECCIONAR': False, align.ID_COLUMN: '001',
        'PRODUCTO': 'Graphy', 'STATUS': 'REVISIÓN DE ARCHIVOS', 'NOMBRE DOCTOR': 'Original'}])
    options = [lab.workbench_grid_options(apparatus, apparatus, user),
        align.build_grid_configuration(aligners, aligners, definitions, current_user=user, reactivate=reactivate)]
    for grid in options:
        for column in grid['columnDefs']:
            assert column['editable'] is (column['field'] == 'SELECCIONAR')
    assert lab.workbench_manual_stage_options(apparatus.iloc[0], user) == []
    assert not lab.is_transition_allowed_for_user(user, lab.PAUSED_STATUS, 'ORDEN RECIBIDA', 'MSE')


@pytest.mark.parametrize('module_name', ['lab_pg', 'alineadores_pg'])
def test_unconfigured_new_accounts_do_not_block_existing_login(module_name):
    script = f'''
import sys
sys.path.insert(0, {ROOT!r})
import {module_name} as app
app.require_authenticated_user()
'''
    at = AppTest.from_string(script, default_timeout=15)
    at.secrets['auth'] = {'passwords': {'Admin': 'demo-admin'}}
    at.run()
    assert not at.exception
    assert at.selectbox[0].options == ['Admin']
    assert any('Juan' in warning.value and 'Paula' in warning.value for warning in at.warning)
    at.text_input[0].input('demo-admin')
    at.button[0].click().run()
    assert not at.exception
    assert any('Sesión activa: Admin' in success.value for success in at.success)


@pytest.mark.parametrize('module_name', ['lab_pg', 'alineadores_pg'])
@pytest.mark.parametrize('user', ['Juan', 'Paula'])
def test_new_accounts_require_their_own_password_and_signed_identity(module_name, user):
    script = f'''
import sys
sys.path.insert(0, {ROOT!r})
import {module_name} as app
app.require_authenticated_user()
'''
    at = AppTest.from_string(script, default_timeout=15)
    at.secrets['auth'] = {'passwords': {name: f'demo-{name}' for name in APP_USERS}}
    at.run()
    assert not at.exception and at.selectbox[0].options == list(APP_USERS)
    at.selectbox[0].select(user)
    at.text_input[0].input('incorrecta')
    at.button[0].click().run()
    assert any('incorrectos' in error.value for error in at.error)
    at.text_input[0].input(f'demo-{user}')
    at.button[0].click().run()
    assert not at.exception and any(f'Sesión activa: {user}' in success.value for success in at.success)
    module = lab if module_name == 'lab_pg' else align
    passwords = {name: f'demo-{name}' for name in APP_USERS}
    signature = module.login_url_signature(user, passwords)
    assert module.valid_url_login(user, signature, passwords)
    assert not module.valid_url_login('Admin', signature, passwords)


@pytest.mark.parametrize('user', ['Juan', 'Paula'])
def test_readonly_order_detail_and_guides_offer_no_mutating_actions(user):
    script = f'''
import sys
sys.path.insert(0, {ROOT!r})
from unittest.mock import patch
import pandas as pd
import streamlit as st
import lab_pg as lab
import alineadores_pg as align
import guias_pg as guides
row = pd.Series({{'Columna 1': '001', 'No. Orden': '002', 'APARATO': 'MSE', 'PRODUCTO': 'Graphy',
    'STATUS': 'REVISIÓN DE ARCHIVOS', 'NOMBRE DOCTOR': 'Doctor consulta', 'NOMBRE PACIENTE': 'Paciente consulta',
    'SERVICIO': 'PLANEACIÓN', 'TEMP. SUP': '2', 'FECHA ENVÍO': '2026-10-14'}})
with patch.object(lab, 'get_workbench_form_catalog', lambda: {{}}), \\
     patch.object(align, 'get_order_form_catalog', lambda: {{}}):
    lab.render_workbench_order_editor(row, {user!r})
    align.render_aligner_order_editor(row, {user!r}, {{}})
guides.render_request_tab({user!r})
'''
    at = AppTest.from_string(script, default_timeout=15).run()
    assert not at.exception
    assert at.text_input and all(widget.disabled for widget in at.text_input)
    assert any(widget.value == 'Doctor consulta' for widget in at.text_input)
    assert any(widget.value == '2' for widget in at.text_input)
    assert not at.selectbox and not at.file_uploader and not at.button
    assert any('sólo lectura' in message.value for message in at.info)


def test_existing_users_keep_edit_access():
    for user in ('Admin', 'Jime', 'Lesly', 'Vero'):
        assert can_edit_orders(user) and can_create_orders(user)
    for user in ('Juan', 'Paula'):
        assert not can_edit_orders(user) and can_create_orders(user)
    assert not can_edit_orders('Invitado') and not can_create_orders('Invitado')


@pytest.mark.parametrize('user', ['Juan', 'Paula'])
def test_limited_users_can_select_and_consult_any_forms_response(user):
    script = f'''
import sys
sys.path.insert(0, {ROOT!r})
from unittest.mock import patch
import pandas as pd
import lab_pg as app
responses = pd.DataFrame([
    {{'Marca temporal': '2026-10-14 10:00:00', 'NOMBRE PACIENTE': 'Paciente uno'}},
    {{'Marca temporal': '2026-10-14 11:00:00', 'NOMBRE PACIENTE': 'Paciente dos'}},
])
with patch.object(app, 'get_forms_config', lambda: {{'sheet_id': 'demo', 'worksheet': 'demo'}}), \\
     patch.object(app, 'read_forms_responses_df', lambda *args: responses):
    app.render_estefano_tab({user!r})
'''
    at = AppTest.from_string(script, default_timeout=15).run()
    assert not at.exception
    selector = at.selectbox(key='forms_response_selector_estefano')
    assert not selector.disabled and len(selector.options) == 2
    selector.set_value(2).run()
    assert not at.exception
    assert any('Paciente uno' in text.value for text in at.markdown)
    assert not at.file_uploader and not at.button
