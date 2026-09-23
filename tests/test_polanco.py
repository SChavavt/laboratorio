"""Regresiones de aislamiento de hojas y alta de órdenes, sin datos ni writes reales."""
import copy
import sys
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import alineadores_pg as app


class Sheet:
    def __init__(self, values):
        self.values = copy.deepcopy(values)
        self.appends = []

    def get_all_values(self):
        return copy.deepcopy(self.values)

    def row_values(self, index):
        return self.values[index - 1] if len(self.values) >= index else []

    def append_row(self, row, **kwargs):
        self.values.append(row)
        self.appends.append(kwargs)

    def update_cells(self, cells, **kwargs):
        for cell in cells:
            row = self.values[cell.row - 1]
            row.extend([''] * max(0, cell.col - len(row)))
            row[cell.col - 1] = cell.value

    def batch_format(self, formats):
        pass


@pytest.fixture
def sheets(monkeypatch):
    headers = [app.ID_COLUMN, app.STATUS_COLUMN, app.PRODUCT_COLUMN,
               'NOMBRE DOCTOR', 'NOMBRE PACIENTE', 'ADEUDO', 'FECHA ENVÍO',
               'FECHA ENVÍO', 'FECHA DE RECEPCIÓN']
    order = ['001', 'REVISIÓN DE ARCHIVOS', 'Graphy', 'Doctor demo', 'Paciente demo',
             '500', 'primero', 'segundo', '2026-09-23']
    data = {name: Sheet([[], headers, order]) for name in (app.SHEET_ORDERS, app.SHEET_POLANCO)}
    data.update({name: Sheet([app.TIMES_HEADERS]) for name in (app.SHEET_TIMES, app.SHEET_POLANCO_TIMES)})
    monkeypatch.setattr(app.st, 'session_state', {})
    monkeypatch.setattr(app, 'get_worksheet', data.__getitem__)
    app.read_order_values.clear()
    app.read_times_values.clear()
    app._ensure_times_headers.clear()
    yield data
    app.read_order_values.clear()
    app.read_times_values.clear()
    app._ensure_times_headers.clear()


def definitions():
    return app.parse_process_matrix([['Graphy', ''], ['Fases', 'Tiempo'],
                                    ['REVISIÓN DE ARCHIVOS', '<1 dia'], ['ENVIADO', '']])


def test_polanco_edit_and_create_never_write_other_sheet(sheets):
    before = copy.deepcopy(sheets[app.SHEET_ORDERS].values)
    app.st.session_state['aligners_order_sheet'] = app.SHEET_POLANCO
    result = app.update_order_row('001', {'ADEUDO': '250', 'FECHA ENVÍO_2': 'tercero'})
    assert result['success']
    assert sheets[app.SHEET_POLANCO].values[2][5:8] == ['250', 'primero', 'tercero']
    identifier, warning = app.create_order({app.ID_COLUMN: '002', app.PRODUCT_COLUMN: 'Graphy',
        'NOMBRE DOCTOR': 'Doctor ejemplo', 'NOMBRE PACIENTE': 'Paciente ejemplo', 'ADEUDO': '700'},
        'Jime', definitions())
    assert (identifier, warning) == ('002', '')
    assert sheets[app.SHEET_POLANCO].values[-1][5] == '700'
    assert len(sheets[app.SHEET_POLANCO_TIMES].values) == 2
    assert len(sheets[app.SHEET_TIMES].values) == 1
    assert sheets[app.SHEET_ORDERS].values == before
    assert sheets[app.SHEET_POLANCO].appends[0]['value_input_option'] == 'RAW'
    with pytest.raises(ValueError, match='ya existe'):
        app.create_order({app.ID_COLUMN: '002', app.PRODUCT_COLUMN: 'Graphy',
            'NOMBRE DOCTOR': 'Doctor ejemplo', 'NOMBRE PACIENTE': 'Paciente ejemplo'}, 'Jime', definitions())
    assert len(sheets[app.SHEET_POLANCO].values) == 4


def test_cached_reads_snapshots_and_resets_are_isolated(sheets, monkeypatch):
    sheets[app.SHEET_POLANCO].values[2][5] = 'POLANCO'
    monkeypatch.setattr(app, 'read_process_definitions', definitions)
    normal = app.get_snapshot('Jime')
    app.st.session_state['aligners_order_sheet'] = app.SHEET_POLANCO
    polanco = app.get_snapshot('Jime')
    assert normal['orders'].iloc[0]['ADEUDO'] == '500'
    assert polanco['orders'].iloc[0]['ADEUDO'] == 'POLANCO'
    assert 'ADEUDO' in app.tracking_columns(polanco['table'].columns)
    assert '_SHEET_ROW' not in app.tracking_columns(polanco['table'].columns)
    app.reset_workbench()
    assert app.st.session_state['aligners_snapshot'] is normal
    assert 'polanco_aligners_snapshot' not in app.st.session_state
    app.st.session_state['aligners_order_sheet'] = app.SHEET_ORDERS
    assert app.get_snapshot('Jime') is normal


def test_create_without_folio_and_log_failure_does_not_claim_order_failed(sheets, monkeypatch):
    def fail(**kwargs):
        raise RuntimeError('bitácora no disponible')
    monkeypatch.setattr(app, 'register_status_change', fail)
    identifier, warning = app.create_order({app.PRODUCT_COLUMN: 'Graphy',
        'NOMBRE DOCTOR': 'Doctor ejemplo', 'NOMBRE PACIENTE': 'Paciente ejemplo'}, 'Jime', definitions())
    assert identifier.startswith('ALI-')
    assert 'sí se guardó' in warning
    assert len(sheets[app.SHEET_ORDERS].values) == 4
    assert len(sheets[app.SHEET_POLANCO].values) == 3


def test_pending_polanco_changes_block_tab_switch(sheets):
    app.st.session_state.update({'aligners_order_sheet': app.SHEET_POLANCO,
        'aligners_active_tab': '📋 Seguimiento Polanco',
        'aligners_primary_tabs_Jime': '📋 Seguimiento',
        'polanco_aligners_editor_key': 'polanco_grid',
        'polanco_aligners_editor_baseline': pd.DataFrame([{app.ID_COLUMN: '001', 'ADEUDO': '500'}]),
        'polanco_grid': {'rows': [{app.ID_COLUMN: '001', 'ADEUDO': '250'}]}})
    app.guard_tracking_tab_change('Jime')
    assert app.st.session_state['aligners_primary_tabs_Jime'] == '📋 Seguimiento Polanco'
    assert app.st.session_state['aligners_tab_warning']


def test_empty_polanco_has_closed_form_and_creates_on_correct_sheet():
    from streamlit.testing.v1 import AppTest
    script = f'''
import sys
sys.path.insert(0, {str(Path(__file__).resolve().parents[1])!r})
import streamlit as st
import pandas as pd
import alineadores_pg as app
from unittest.mock import patch
snapshot = {{'user': 'Jime', 'definitions': app.parse_process_matrix(
    [['Graphy', ''], ['Fases', 'Tiempo'], ['REVISIÓN DE ARCHIVOS', '<1 dia']]),
    'orders': pd.DataFrame(), 'times': pd.DataFrame(), 'table': pd.DataFrame(columns=app.COMPUTED_COLUMNS), 'at': app.app_now()}}
def create(values, user, definitions):
    st.session_state['created_in'] = app.current_order_sheet()
    st.session_state['created_values'] = values
    return 'POL-DEMO', ''
with patch.object(app, 'ensure_times_headers', lambda: None), \\
     patch.object(app, 'get_snapshot', lambda _: snapshot), \\
     patch.object(app, 'create_order', create):
    st.session_state['aligners_primary_tabs_Jime'] = st.session_state.get('test_tab', '📋 Seguimiento')
    if st.session_state.get('test_open'):
        st.session_state['polanco_aligners_new_expander'] = True
    app.render_app_tabs('Jime')
'''
    at = AppTest.from_string(script, default_timeout=15).run()
    assert not at.exception
    assert not at.text_input
    at.session_state['test_tab'] = '📋 Seguimiento Polanco'
    at.run()
    assert not at.exception
    assert not at.text_input
    at.session_state['test_open'] = True
    at.run()
    assert not at.exception
    assert any(item.label == 'ADEUDO' for item in at.text_input)
    for item in at.text_input:
        if item.label in ('NOMBRE DOCTOR *', 'NOMBRE PACIENTE *'):
            item.input('Ejemplo')
    next(button for button in at.button if button.label == '💾 Guardar nueva orden').click().run()
    assert not at.exception
    assert at.session_state['created_in'] == app.SHEET_POLANCO
    assert at.session_state['created_values']['NOMBRE PACIENTE'] == 'Ejemplo'


@pytest.mark.parametrize('sheet_name,times_name,other_name', [
    (app.SHEET_ORDERS, app.SHEET_TIMES, app.SHEET_POLANCO),
    (app.SHEET_POLANCO, app.SHEET_POLANCO_TIMES, app.SHEET_ORDERS),
])
def test_reactivating_sent_order_returns_it_to_active_table(
        sheets, monkeypatch, sheet_name, times_name, other_name):
    monkeypatch.setattr(app, 'read_process_definitions', definitions)
    sheets[sheet_name].values[2][1] = 'ENVIADO'
    other_before = copy.deepcopy(sheets[other_name].values)
    app.st.session_state['aligners_order_sheet'] = sheet_name
    snapshot = app.get_snapshot('Jime')
    assert snapshot['table'].empty
    assert list(snapshot['sent'][app.ID_COLUMN]) == ['001']

    grid = app.display_workbench_df(snapshot['sent'], snapshot['definitions'])
    edited = grid.copy()
    edited.loc[0, app.STATUS_COLUMN] = app.status_display_value('REVISIÓN DE ARCHIVOS')
    saved, errors = app.save_workbench_changes(
        snapshot['sent'], grid, edited, 'Jime', snapshot['definitions'], reactivate=True)

    assert (saved, errors) == (['001'], [])
    assert sheets[sheet_name].values[2][1] == 'REVISIÓN DE ARCHIVOS'
    log = dict(zip(app.TIMES_HEADERS, sheets[times_name].values[-1]))
    assert log[app.STATUS_COLUMN] == 'REVISIÓN DE ARCHIVOS'
    assert 'ENVIADO → REVISIÓN DE ARCHIVOS' in log['COMENTARIOS_CAMBIO']
    assert 'histórico de Enviados' in log['COMENTARIOS_CAMBIO']
    assert sheets[other_name].values == other_before
    refreshed = app.get_snapshot('Jime')
    assert list(refreshed['table'][app.ID_COLUMN]) == ['001']
    assert refreshed['sent'].empty


def test_old_snapshot_without_sent_archive_is_rebuilt(sheets, monkeypatch):
    monkeypatch.setattr(app, 'read_process_definitions', definitions)
    app.st.session_state['aligners_snapshot'] = {'user': 'Jime', 'table': pd.DataFrame()}
    assert 'sent' in app.get_snapshot('Jime')
