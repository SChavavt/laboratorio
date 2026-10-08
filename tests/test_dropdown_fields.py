"""Regresiones de los valores compuestos y de los editores usados en el navegador."""
import json
import shutil
import subprocess
from pathlib import Path
import sys

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import dropdown_fields as dropdown
import grid_interactions as interactions
import alineadores_pg as align
import workbench_grid as apparatus_grid
import aligners_grid


def run_js(code, **values):
    node = shutil.which('node')
    if not node:
        pytest.skip('Node no está instalado')
    result = subprocess.run([node, '-e', 'const data = ' + json.dumps(values) + ';\n' + code],
                            capture_output=True, text=True, check=True)
    return json.loads(result.stdout)


def javascript(js):
    return js.js_code.replace('::JSCODE::', '')


def test_compound_values_preserve_native_spaces_and_commas_inside_options():
    options = ['STLs', 'TOMO', 'NO APLICA ', 'Foto, lateral']
    assert dropdown.split_values('STLs, NO APLICA , Foto, lateral', options) == ['STLs', 'NO APLICA ', 'Foto, lateral']
    assert dropdown.join_values(['STLs', 'NO APLICA ', 'STLs']) == 'STLs, NO APLICA '
    assert dropdown.valid_value('STLs, TOMO', options, multiple=True)
    assert not dropdown.valid_value('STLs, INVÁLIDO', options, multiple=True)
    assert not dropdown.valid_value('STLs, TOMO', options)
    assert dropdown.valid_value('STLs, Antiguo', options, multiple=True, previous='Antiguo')
    source = pd.DataFrame({'Archivos': ['STLs, TOMO'], 'Servicio': ['PLAN+CONFEC.']})
    assert dropdown.multiple_columns({'Archivos': options, 'Servicio': ['PLAN+CONFEC.']}, source) == {'Archivos'}


def test_grid_uses_unused_native_choices_and_separate_colored_chips():
    source = pd.DataFrame([{align.ID_COLUMN: '001', align.STATUS_COLUMN: 'REVISIÓN DE ARCHIVOS',
        align.PRODUCT_COLUMN: 'GRAPHY', 'ARCHIVOS RECIBIDOS': 'STLs, TOMO', 'SERVICIO': 'PLANEACIÓN',
        'TIPO': 'ARTTDLAB', 'FECHA ENVÍO_2': '2026-10-05'}])
    catalog = {'ARCHIVOS RECIBIDOS': ['STLs', 'TOMO', 'FOTO INTRA'],
               'SERVICIO': ['PLANEACIÓN', 'NUEVO SERVICIO'], 'TIPO': ['ARTTDLAB', 'MARCA BLANCA']}
    options = align.build_grid_configuration(source, source, {}, select_catalog=catalog)
    spec = options['context']['dropdowns']['ARCHIVOS RECIBIDOS']
    assert spec['values'] == catalog['ARCHIVOS RECIBIDOS']
    assert spec['multiple']
    assert 'STLs, TOMO' not in spec['values']
    assert spec['labels']['STLs'].startswith('📁')
    assert all(len(colors) == 2 for colors in spec['colors'].values())
    columns = {column['field']: column for column in options['columnDefs']}
    assert columns['ARCHIVOS RECIBIDOS']['autoHeight']
    assert 'NUEVO SERVICIO' in options['context']['dropdowns']['SERVICIO']['values']
    assert columns['FECHA ENVÍO_2']['cellEditorParams']['initialValues']['2026-10-05'] == '2026-10-05'


def test_selection_events_are_compact_and_preserve_edits_after_multiple_clicks():
    result = run_js('''
const collect = eval('(' + data.collect + ')');
const rows = Array.from({length: 1000}, (_, i) => ({id: String(i), SELECCIONAR: false, notes: 'x'.repeat(100)}));
const api = {getGridOption: () => ({idColumn: 'id'}), forEachNode: callback => rows.forEach(data => callback({data})),
             getColumnState: () => [{colId: 'id'}]};
const event = (index, field, value) => { rows[index][field] = value; return collect({eventData: {
    api, data: rows[index], colDef: {field}, type: 'cellValueChanged', newValue: value}}); };
const first = event(700, 'SELECCIONAR', true);
event(700, 'notes', 'Cambio local');
event(1, 'SELECCIONAR', true);
const last = event(700, 'SELECCIONAR', false);
console.log(JSON.stringify({first, last, bytes: JSON.stringify(first).length}));
''', collect=javascript(interactions.COLLECT_ROWS))
    assert result['first']['selectedIds'] == ['700']
    assert result['first']['activeId'] == '700'
    assert result['bytes'] < 300
    assert result['last']['selectedIds'] == ['1']
    assert result['last']['editedRows'][0]['notes'] == 'Cambio local'
    grid = pd.DataFrame([{'id': '700', 'SELECCIONAR': False, 'notes': 'Original'},
                         {'id': '1', 'SELECCIONAR': False, 'notes': 'Otro'}])
    edited = interactions.response_frame(grid, result['last'], 'id')
    assert edited.loc[0, 'notes'] == 'Cambio local'
    assert edited.loc[1, 'SELECCIONAR'] and not edited.loc[0, 'SELECCIONAR']
    assert edited.loc[1, 'notes'] == 'Otro'


FAKE_DOM = '''
class Element {
 constructor(tag) { this.tag = tag; this.children = []; this.style = {}; this.listeners = {}; this.attrs = {}; this.value = ''; }
 appendChild(child) { this.children.push(child); }
 replaceChildren() { this.children = []; }
 setAttribute(name, value) { this.attrs[name] = value; }
 addEventListener(name, fn) { this.listeners[name] = fn; }
 focus() {}
}
const document = {createElement: tag => new Element(tag)};
'''


def test_status_submenu_navigates_without_saving_and_marks_only_manual_choices():
    result = run_js(FAKE_DOM + '''
const Editor = eval('(' + data.editor + ')');
const commits = [];
const row = {'Columna 1': '001', APARATO: 'MSE'};
const params = {value: 'Revisión', data: row, stopEditing: cancelled => commits.push(Boolean(cancelled)),
 context: {stageOptions: {'001': ['Revisión', 'Pago']}, manualStageOptions: {'001': ['Otra etapa ajena']},
 apparatusFlowKeys: {MSE: 'mse'}, apparatusManualStageOptions: {'001': {mse: ['Revisión', 'Pago', 'Planeación']}},
 palettes: {STATUS: {'Planeación': ['#ABCDEF', '#123456']}}}};
const editor = new Editor(); editor.init(params);
const labels = () => editor.buttons.map(button => button.textContent);
const click = label => editor.buttons.find(button => button.textContent === label).listeners.click();
const initial = labels();
click('Ver todas las etapas… →');
const full = labels();
const beforeChoice = {commits: commits.length, value: editor.getValue(), target: row.__manualStageTarget || null};
const color = editor.buttons.find(button => button.textContent === 'Planeación').style.backgroundColor;
click('← Volver a etapas sugeridas');
const returned = labels();
click('Ver todas las etapas… →');
editor.gui.listeners.keydown({key: 'Escape', preventDefault() {}});
const cancelled = editor.getValue();
click('Planeación');
const chosen = {value: editor.getValue(), target: row.__manualStageTarget};
editor.render(false);
click('Pago');
console.log(JSON.stringify({initial, full, beforeChoice, color, returned, cancelled, chosen,
 recommended: {value: editor.getValue(), target: row.__manualStageTarget}, commits}));
''', editor=javascript(apparatus_grid.status_editor()))
    assert result['initial'] == ['Revisión', 'Pago', 'Ver todas las etapas… →']
    assert result['full'] == ['← Volver a etapas sugeridas', 'Revisión', 'Pago', 'Planeación']
    assert result['beforeChoice'] == {'commits': 0, 'value': 'Revisión', 'target': None}
    assert result['returned'] == result['initial']
    assert result['color'] == '#ABCDEF'
    assert result['cancelled'] == 'Revisión'
    assert result['chosen'] == {'value': 'Planeación', 'target': 'Planeación'}
    assert result['recommended'] == {'value': 'Pago', 'target': None}
    assert result['commits'] == [True, False, False]


def test_status_without_extra_permissions_has_no_all_stages_menu():
    result = run_js(FAKE_DOM + '''
const Editor = eval('(' + data.editor + ')');
const editor = new Editor();
editor.init({value: 'Revisión', data: {'Columna 1': '001', APARATO: 'MSE'}, stopEditing() {},
 context: {stageOptions: {'001': ['Revisión', 'Pago']}, palettes: {}}});
console.log(JSON.stringify(editor.buttons.map(button => button.textContent)));
''', editor=javascript(apparatus_grid.status_editor()))
    assert result == ['Revisión', 'Pago']


def test_aligners_status_submenu_matches_apparatus_behaviour():
    result = run_js(FAKE_DOM + '''
const Editor = eval('(' + data.editor + ')');
const commits = [];
const row = {'No. Orden': '001'};
const params = {value: 'Revisión', data: row, stopEditing: cancelled => commits.push(Boolean(cancelled)),
 context: {idColumn: 'No. Orden', stageOptions: {'001': ['Revisión', 'Planeación']},
 manualStageOptions: {'001': ['Revisión', 'Planeación', 'En impresión']}, palettes: {STATUS: {}}}};
const editor = new Editor(); editor.init(params);
const labels = () => editor.buttons.map(button => button.textContent);
const click = label => editor.buttons.find(button => button.textContent === label).listeners.click();
const initial = labels();
click('Ver todas las etapas… →');
const full = labels();
click('En impresión');
const chosen = {value: editor.getValue(), target: row.__manualStageTarget};
const plain = new Editor();
plain.init({value: 'Revisión', data: {'No. Orden': '002'}, stopEditing() {},
 context: {idColumn: 'No. Orden', stageOptions: {'002': ['Revisión', 'Planeación']}, palettes: {}}});
console.log(JSON.stringify({initial, full, chosen, commits,
 plain: plain.buttons.map(button => button.textContent)}));
''', editor=javascript(aligners_grid.aligners_status_editor()))
    assert result['initial'] == ['Revisión', 'Planeación', 'Ver todas las etapas… →']
    assert result['full'] == ['← Volver a etapas sugeridas', 'Revisión', 'Planeación', 'En impresión']
    assert result['chosen'] == {'value': 'En impresión', 'target': 'En impresión'}
    assert result['commits'] == [False]
    assert result['plain'] == ['Revisión', 'Planeación']


def test_grid_event_keeps_manual_intent_by_id_without_adding_sheet_columns():
    grid = pd.DataFrame([{'id': '001', 'STATUS': 'Revisión', 'notes': 'Original', 'SELECCIONAR': False}])
    response = {'editedRows': [{'id': '001', 'STATUS': 'Planeación', 'notes': 'Editado',
                                interactions.MANUAL_STAGE_FIELD: 'Planeación'}], 'selectedIds': ['001']}
    edited = interactions.response_frame(grid, response, 'id')
    assert edited.attrs['manualStageTargets'] == {'001': 'Planeación'}
    assert list(edited.columns) == list(grid.columns)
    assert edited.iloc[0]['STATUS'] == 'Planeación' and edited.iloc[0]['SELECCIONAR']
    response['editedRows'][0][interactions.MANUAL_STAGE_FIELD] = None
    assert interactions.response_frame(grid, response, 'id').attrs['manualStageTargets'] == {}


def test_checkbox_toggles_once_without_entering_cell_edit_mode():
    result = run_js(FAKE_DOM + '''
const Renderer = eval('(' + data.renderer + ')');
const renderer = new Renderer();
const writes = [];
const params = {value: false, data: {'NOMBRE DOCTOR': 'Ejemplo'}, node: {setDataValue: (field, value) => writes.push([field, value])}};
renderer.init(params);
const input = renderer.getGui();
for (const checked of [true, false, true]) {
 input.checked = checked;
 input.listeners.click({stopPropagation() {}});
 input.listeners.change();
 renderer.refresh({...params, value: checked});
}
console.log(JSON.stringify(writes));
''', renderer=javascript(interactions.selection_renderer()))
    assert result == [['SELECCIONAR', True], ['SELECCIONAR', False], ['SELECCIONAR', True]]


def test_multiselect_editor_keeps_checked_options_until_apply_and_can_cancel():
    result = run_js(FAKE_DOM + '''
const Editor = eval('(' + data.editor + ')');
const Chips = eval('(' + data.renderer + ')');
const values = ['STLs', 'TOMO', 'Foto, lateral', 'NO APLICA '];
const spec = {multiple: true, values, labels: Object.fromEntries(values.map(v => [v, '📁 ' + v])),
 colors: Object.fromEntries(values.map(v => [v, ['#DBEAFE', '#1E40AF']])), aliases: {}};
let commits = 0;
const params = {value: 'STLs, Foto, lateral', context: {dropdowns: {files: spec}}, colDef: {field: 'files'}, stopEditing: () => commits++};
const editor = new Editor(); editor.init(params);
editor.list.children[1].listeners.click();
const added = editor.getValue();
const beforeApply = commits;
editor.gui.children[3].children[1].listeners.click();
const chips = new Chips(); chips.init({...params, value: added});
editor.gui.children[3].children[2].listeners.click();
console.log(JSON.stringify({added, beforeApply, commits, cancelled: editor.getValue(),
 chips: chips.gui.children.map(c => ({label: c.textContent, color: c.style.backgroundColor}))}));
''', editor=javascript(dropdown.dropdown_js('editor')), renderer=javascript(dropdown.dropdown_js('renderer')))
    assert result['added'] == 'STLs, Foto, lateral, TOMO'
    assert result['beforeApply'] == 0
    assert result['cancelled'] == 'STLs, Foto, lateral'
    assert [chip['label'] for chip in result['chips']] == ['📁 STLs', '📁 Foto, lateral', '📁 TOMO']
    assert all(chip['color'] == '#DBEAFE' for chip in result['chips'])
