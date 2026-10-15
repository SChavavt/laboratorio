"""Consulta del nuevo formulario AERIS con respuestas simuladas, sin red."""
import sys
from pathlib import Path

from streamlit.testing.v1 import AppTest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import alineadores_pg as app

UPLOAD_COLUMN = "Favor de subir archivos:\n\n- Escaneos STL superior e inferior (libres de interferencias)."


def test_aeris_form_supports_separate_response_source_overrides(monkeypatch):
    form = next(form for form in app.ALIGNERS_FORMS if form["key"] == "aeris")
    monkeypatch.setattr(app.st, "secrets", {"google_forms_alineadores": {"aeris": {
        "sheet_id": "custom-responses", "worksheet": "Respuestas AERIS", "form_id": "custom-form",
    }}})
    config = app.get_aligners_form_config(form)
    assert (config["sheet_id"], config["worksheet"], config["form_id"]) == (
        "custom-responses", "Respuestas AERIS", "custom-form")
    other = next(form for form in app.ALIGNERS_FORMS if form["key"] == "td")
    assert app.get_aligners_form_config(other)["sheet_id"] == other["default_sheet_id"]


def test_aeris_forms_tab_reads_only_its_response_sheet_and_offers_files_and_pdf():
    script = f'''
import sys
sys.path.insert(0, {str(ROOT)!r})
from unittest.mock import patch
import pandas as pd
import streamlit as st
import alineadores_pg as app
st.session_state[app.FORMS_SUB_TABS_KEY] = 'AERIS'
rows = pd.DataFrame([{{'Marca temporal': '14/10/2026 12:00:00',
    'Nombre completo del paciente:': 'Paciente demo',
    'Producto solicitado:': 'AERIS', 'Servicio del producto solicitado:': 'CONFECCIÓN',
    {UPLOAD_COLUMN!r}: 'https://drive.google.com/open?id=demo-stl'}}])
def responses(sheet_id, worksheet):
    st.session_state.setdefault('response_reads', []).append((sheet_id, worksheet))
    return rows
def sections(form_id):
    st.session_state['structure_read'] = form_id
    return [], ''
with patch.object(app, 'read_forms_responses_df', responses), \\
     patch.object(app, 'read_form_sections', sections):
    app.render_aligners_forms_tab()
'''
    at = AppTest.from_string(script, default_timeout=15)
    at.secrets['google_forms_alineadores'] = {'aeris': {
        'sheet_id': 'aeris-responses', 'form_id': 'aeris-form'}}
    at.run()
    assert not at.exception
    assert at.session_state['response_reads'] == [
        ('aeris-responses', 'Respuestas de formulario 1')]
    assert at.session_state['structure_read'] == 'aeris-form'
    assert at.selectbox(key='aligners_forms_selector_aeris').value == 2
    assert any('demo-stl' in item.value for item in at.markdown)
    assert len(at.get('download_button')) == 1
    assert not at.warning
