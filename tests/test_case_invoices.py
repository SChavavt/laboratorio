"""Facturas y búsqueda con S3/Sheets simulados; no realizan llamadas de red."""
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
import sys
from urllib.parse import parse_qs, quote, urlparse
from unittest.mock import Mock

from botocore.exceptions import ClientError, EndpointConnectionError
import pandas as pd
import pytest
from streamlit.errors import StreamlitSecretNotFoundError
from streamlit.testing.v1 import AppTest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import case_invoices as invoices
import case_search as search
import lab_pg as lab
import alineadores_pg as align

PDF = b'%PDF-1.4\n1 0 obj\n<<>>\nendobj\n%%EOF'


class Upload(BytesIO):
    def __init__(self, name, body=PDF, file_id=None):
        super().__init__(body)
        self.name = name
        self.file_id = file_id or name


class FakeS3:
    def __init__(self):
        self.objects = {}
        self.writes = []
        self.signatures = []
        self.fail_once = set()

    def put_object(self, **kwargs):
        name = kwargs['Key'].rsplit('/', 1)[-1]
        if name in self.fail_once:
            self.fail_once.remove(name)
            raise OSError('Falla temporal')
        self.writes.append(kwargs)
        self.objects[kwargs['Key']] = kwargs

    def get_paginator(self, operation):
        assert operation == 'list_objects_v2'
        return self

    def paginate(self, **kwargs):
        rows = [{'Key': key, 'Size': len(value['Body']), 'LastModified': datetime(2026, 10, 8, 16, tzinfo=timezone.utc)}
                for key, value in self.objects.items() if key.startswith(kwargs['Prefix'])]
        for index in range(0, len(rows), 2):
            yield {'Contents': rows[index:index + 2]}

    def generate_presigned_url(self, operation, **kwargs):
        assert operation == 'get_object'
        self.signatures.append(kwargs)
        return 'https://files.example.test/' + quote(kwargs['Params']['Key'], safe='/') + '?signed=1'


@pytest.fixture
def store(monkeypatch):
    store = invoices.InvoiceStore(FakeS3(), 'test-invoices')
    invoices.list_invoices.clear()
    monkeypatch.setattr(invoices, 'get_store', lambda: store)
    yield store
    invoices.list_invoices.clear()


def test_many_pdfs_append_preserve_names_paginate_and_do_not_collide_across_cases(store):
    uploads = [Upload('Factura México.pdf', file_id='a'), Upload('Factura México.pdf', file_id='b'),
               Upload('segunda.PDF', file_id='c')]
    assert store.upload('aparatos', '001', uploads, 'Jime', lambda: None) == ([file.name for file in uploads], [])
    store.upload('aparatos', '001', [Upload('posterior.pdf')], 'Admin', lambda: None)
    store.upload('aparatos', '0012', [uploads[0]], 'Admin', lambda: None)
    store.upload('alineadores', '001', [uploads[0]], 'Admin', lambda: None)
    store.upload('polanco', '001', [uploads[0]], 'Admin', lambda: None)
    assert len(store.list('aparatos', '001')) == 4
    assert sorted(file.name for file in store.list('aparatos', '001')).count('Factura México.pdf') == 2
    assert len(store.list('aparatos', '0012')) == len(store.list('alineadores', '001')) == len(store.list('polanco', '001')) == 1
    assert len(store.client.objects) == 7
    assert all(write['ContentType'] == 'application/pdf' and 'ACL' not in write for write in store.client.writes)
    assert invoices.case_prefix('aparatos', '001') != invoices.case_prefix('aparatos', '1')


@pytest.mark.parametrize('bad', [Upload('archivo.png'), Upload('archivo.pdf', b'No es un PDF'), Upload('vacio.pdf', b'')])
def test_all_files_are_validated_before_first_write(store, bad):
    with pytest.raises(ValueError, match='PDF'):
        store.upload('aparatos', '001', [Upload('valida.pdf'), bad], 'Jime', lambda: None)
    assert store.client.writes == []


@pytest.mark.parametrize('rows,user', [([], 'Jime'), (['001', '001'], 'Jime'), (['001'], 'Invitado')])
def test_removed_duplicate_or_unauthorized_case_cannot_receive_files(store, rows, user):
    with pytest.raises(ValueError):
        store.upload('aparatos', '001', [Upload('factura.pdf')], user,
                     lambda: invoices.require_unique_case(pd.DataFrame({'Folio': rows}), '001', 'Folio'))
    assert store.client.writes == []


def test_retry_after_partial_failure_keeps_all_files_once(store):
    files = [Upload('uno.pdf'), Upload('dos.pdf'), Upload('tres.pdf')]
    store.client.fail_once.add('dos.pdf')
    saved, errors = store.upload('aparatos', '001', files, 'Lesly', lambda: None)
    assert saved == ['uno.pdf', 'tres.pdf'] and len(errors) == 1
    saved, errors = store.upload('aparatos', '001', files, 'Lesly', lambda: None)
    assert not errors and len(saved) == 3 and len(store.list('aparatos', '001')) == 3


def test_pdf_links_are_temporary_inline_or_download_and_scoped_to_the_case(store):
    store.upload('aparatos', '001', [Upload('Factura México.pdf')], 'Vero', lambda: None)
    [file] = store.list('aparatos', '001')
    store.url('aparatos', '001', file)
    store.url('aparatos', '001', file, download=True)
    inline, download = store.client.signatures
    assert inline['ExpiresIn'] == download['ExpiresIn'] == 1800
    assert inline['Params']['ResponseContentType'] == 'application/pdf'
    assert inline['Params']['ResponseContentDisposition'].startswith('inline;')
    assert download['Params']['ResponseContentDisposition'].startswith('attachment;')
    with pytest.raises(ValueError, match='no pertenece'):
        store.url('alineadores', '001', file)


@pytest.mark.parametrize('layout', ['root', 'section', 'blank_root'])
def test_storage_reads_existing_secrets_and_signs_v4_with_optional_session_token(monkeypatch, layout):
    config = dict(zip(invoices.AWS_KEYS, ['test-key', 'test-secret', 'us-east-1', 'test-bucket']))
    config['aws_session_token'] = 'test-session'
    secrets = config if layout == 'root' else {'aws': config}
    if layout == 'blank_root':
        secrets.update({key: '  ' for key in config})
    monkeypatch.setattr(invoices.st, 'secrets', secrets)
    client = Mock()
    monkeypatch.setattr(invoices.boto3, 'client', client)
    invoices.get_store.clear()
    try:
        store = invoices.get_store()
        assert store.bucket == 'test-bucket'
        args = client.call_args.kwargs
        assert args['aws_access_key_id'] == 'test-key'
        assert args['aws_secret_access_key'] == 'test-secret'
        assert args['aws_session_token'] == 'test-session'
        assert args['region_name'] == 'us-east-1'
        assert args['config'].signature_version == 's3v4'
    finally:
        invoices.get_store.clear()


def test_storage_config_reports_only_missing_key_names_and_never_creates_a_client(monkeypatch):
    monkeypatch.setattr(invoices.st, 'secrets', {'aws_access_key_id': 'PRIVATE_VALUE', 'aws': {'aws_region': 'us-east-1'}})
    client = Mock()
    monkeypatch.setattr(invoices.boto3, 'client', client)
    invoices.get_store.clear()
    try:
        with pytest.raises(invoices.StorageConfigurationError) as error:
            invoices.get_store()
        assert error.value.missing == ('aws_secret_access_key', 's3_bucket_name')
        assert 'PRIVATE_VALUE' not in ' '.join(invoices.storage_error(error.value, 'consultar'))
        client.assert_not_called()
    finally:
        invoices.get_store.clear()


def test_storage_handles_absent_secrets_file(monkeypatch):
    secrets = Mock()
    secrets.get.side_effect = StreamlitSecretNotFoundError('PRIVATE_PATH')
    monkeypatch.setattr(invoices.st, 'secrets', secrets)
    invoices.get_store.clear()
    try:
        with pytest.raises(invoices.StorageConfigurationError) as error:
            invoices.get_store()
        assert error.value.missing == invoices.AWS_KEYS
        assert 'PRIVATE_PATH' not in str(error.value)
    finally:
        invoices.get_store.clear()


@pytest.mark.parametrize('session_token', ['', 'test-session'])
def test_real_s3_client_generates_v4_pdf_link_without_network(monkeypatch, session_token):
    config = dict(zip(invoices.AWS_KEYS, ['test-key', 'test-secret', 'us-east-1', 'test-bucket']))
    config['aws_session_token'] = session_token
    monkeypatch.setattr(invoices.st, 'secrets', {'aws': config})
    invoices.get_store.clear()
    try:
        store = invoices.get_store()
        invoice = invoices.Invoice('facturas/aparatos/001/file/factura.pdf', 'factura.pdf', 10)
        query = parse_qs(urlparse(store.url('aparatos', '001', invoice)).query)
        assert query['X-Amz-Algorithm'] == ['AWS4-HMAC-SHA256']
        assert query['X-Amz-Expires'] == ['1800']
        assert query.get('X-Amz-Security-Token', []) == ([session_token] if session_token else [])
        assert query['response-content-disposition'][0].startswith('inline;')
    finally:
        invoices.get_store.clear()


@pytest.mark.parametrize('code,hint', [
    ('AccessDenied', 's3:ListBucket'), ('InvalidAccessKeyId', 'claves'),
    ('ExpiredToken', 'aws_session_token'), ('NoSuchBucket', 's3_bucket_name'),
    ('AuthorizationHeaderMalformed', 'aws_region'), ('UnknownError', 'registros'),
])
def test_storage_errors_identify_the_action_without_exposing_aws_response(code, hint):
    error = ClientError({'Error': {'Code': code, 'Message': 'PRIVATE_RESPONSE'}}, 'ListObjectsV2')
    text = ' '.join(invoices.storage_error(error, 'consultar'))
    assert hint in text and 'PRIVATE_RESPONSE' not in text
    if code == 'AccessDenied':
        assert 's3:PutObject' in invoices.storage_error(error, 'guardar')[1]


SECTION_SCRIPT = f'''
import sys
sys.path.insert(0, {str(ROOT)!r})
import streamlit as st
import case_invoices as invoices
invoices.render_section('aparatos', '001', st.session_state.get('user', 'Jime'),
                        verify_case=lambda: None, rerun=st.rerun)
'''


@pytest.mark.parametrize('user', ['Jime', 'Admin'])
def test_denied_listing_does_not_claim_order_is_empty_and_can_confirm_successful_upload(store, monkeypatch, user):
    listing = Mock(side_effect=ClientError({'Error': {'Code': 'AccessDenied', 'Message': 'PRIVATE_RESPONSE'}}, 'ListObjectsV2'))
    monkeypatch.setattr(invoices, 'list_invoices', listing)
    at = AppTest.from_string(SECTION_SCRIPT, default_timeout=15)
    at.session_state['user'] = user
    at.run()
    assert not at.exception and len(at.file_uploader) == 1
    assert any('permiso para consultar' in item.value for item in at.warning)
    assert not any('todavía no tiene' in item.value for item in at.caption)
    assert any('s3:ListBucket' in item.value for item in at.caption) == (user == 'Admin')
    assert not any('PRIVATE_RESPONSE' in item.value for item in at.warning)
    at.file_uploader[0].set_value(('factura.pdf', PDF, 'application/pdf')).run()
    next(button for button in at.button if button.label == 'Guardar facturas').click().run()
    assert not at.exception and len(store.list('aparatos', '001')) == 1
    assert any('1 factura(s) guardada(s)' in item.value for item in at.success)
    assert any('consulta falló' in item.value for item in at.caption)
    assert not at.file_uploader[0].value
    listing.side_effect = None
    listing.return_value = store.list('aparatos', '001')
    next(button for button in at.button if button.label == 'Actualizar facturas').click().run()
    listing.clear.assert_called_with('aparatos', '001')
    assert not at.exception and not at.warning
    assert len(at.get('link_button')) == 2


def test_missing_config_blocks_upload_with_safe_admin_instructions(monkeypatch):
    monkeypatch.setattr(invoices, 'get_store', Mock(side_effect=invoices.StorageConfigurationError(('s3_bucket_name',))))
    listing = Mock()
    monkeypatch.setattr(invoices, 'list_invoices', listing)
    at = AppTest.from_string(SECTION_SCRIPT, default_timeout=15)
    at.session_state['user'] = 'Admin'
    at.run()
    assert not at.exception and not at.file_uploader
    assert any('s3_bucket_name' in item.value for item in at.caption)
    assert not any('todavía no tiene' in item.value for item in at.caption)
    listing.assert_not_called()


def test_connection_failure_does_not_render_an_empty_order(store, monkeypatch):
    listing = Mock(side_effect=EndpointConnectionError(endpoint_url='https://PRIVATE_ENDPOINT'))
    monkeypatch.setattr(invoices, 'list_invoices', listing)
    at = AppTest.from_string(SECTION_SCRIPT, default_timeout=15).run()
    assert not at.exception
    assert any('conectar al almacenamiento' in item.value for item in at.warning)
    assert not any('PRIVATE_ENDPOINT' in item.value for item in at.warning)
    assert not any('todavía no tiene' in item.value for item in at.caption)


def test_denied_upload_reports_write_permission_and_keeps_selection_for_retry(store, monkeypatch):
    store.client.put_object = Mock(side_effect=ClientError({'Error': {'Code': 'AccessDenied', 'Message': 'PRIVATE_RESPONSE'}}, 'PutObject'))
    at = AppTest.from_string(SECTION_SCRIPT, default_timeout=15)
    at.session_state['user'] = 'Admin'
    at.run()
    at.file_uploader[0].set_value(('factura.pdf', PDF, 'application/pdf')).run()
    next(button for button in at.button if button.label == 'Guardar facturas').click().run()
    assert not at.exception and not at.success
    assert any('s3:PutObject' in item.value for item in at.error)
    assert not any('PRIVATE_RESPONSE' in item.value for item in at.error)
    assert at.file_uploader[0].value
    assert store.client.objects == {}


@pytest.mark.parametrize('query', ['jose', 'José Martín', 'LOPEZ', '001', 'jose lopez'])
def test_search_matches_patient_doctor_id_without_accents_and_keeps_archived_cases(query):
    cases = search.source_cases(pd.DataFrame([
        {'Folio': '001', 'NOMBRE DOCTOR': 'Dra. López', 'NOMBRE PACIENTE': 'José Martín', 'STATUS': 'PRODUCTO ENVIADO'},
        {'Folio': '002', 'NOMBRE DOCTOR': 'Otra doctora', 'NOMBRE PACIENTE': 'Otra paciente', 'STATUS': 'ORDEN RECIBIDA'},
        {'Folio': '', 'NOMBRE DOCTOR': 'López'}, {'Folio': '003', 'NOMBRE DOCTOR': ''},
    ]), 'aparatos', 'Folio')
    assert list(search.find_cases(cases, query)[search.ID_COLUMN]) == ['001']
    assert search.find_cases(cases, 'ausente').empty
    assert search.find_cases(cases, '').empty
    assert search.find_cases(cases, '003').empty


@pytest.mark.parametrize('case_type', ['aparatos', 'alineadores', 'polanco'])
@pytest.mark.parametrize('partial', [{}, {'DETALLE COMENTARIOS': 'Sólo información administrativa'}])
def test_search_retains_partial_duplicate_folios_for_validation(case_type, partial):
    cases = search.source_cases(pd.DataFrame([
        {'Folio': '001', 'NOMBRE PACIENTE': 'Paciente demo'},
        {'Folio': ' 001 ', **partial},
        {'Folio': '', 'NOMBRE PACIENTE': 'Fila sin folio'},
    ]), case_type, 'Folio')
    assert list(cases[search.ID_COLUMN]) == ['001', '001']
    assert list(cases[search.SOURCE_COLUMN]) == [case_type, case_type]
    assert len(search.find_cases(cases, '001')) == 1
    with pytest.raises(ValueError, match='duplicado'):
        invoices.require_unique_case(cases, '001', search.ID_COLUMN)


EDITOR_SCRIPT = f'''
import sys
sys.path.insert(0, {str(ROOT)!r})
from unittest.mock import patch, Mock
import pandas as pd
import streamlit as st
import lab_pg as lab
import alineadores_pg as align
st.session_state.setdefault('case_type', 'aparatos')
case_type = st.session_state['case_type']
st.session_state['aligners_order_sheet'] = align.SHEET_POLANCO if case_type == 'polanco' else align.SHEET_ORDERS
id_column = lab.ID_COLUMN if case_type == 'aparatos' else align.ID_COLUMN
row = {{id_column: '001', 'NOMBRE DOCTOR': 'Doctora demo', 'NOMBRE PACIENTE': 'Paciente demo',
       'APARATO': 'MSE', 'PRODUCTO': 'Graphy', 'STATUS': 'REVISIÓN DE ARCHIVOS', 'SERVICIO': 'PLANEACIÓN'}}
if st.session_state.get('service_key'):
    # AppTest no expone todavía el click del expander; conserva su estado abierto.
    st.session_state[st.session_state['service_key']] = True
definitions = align.parse_process_matrix([['Graphy', ''], ['Fases', 'Tiempo'],
    ['REVISIÓN DE ARCHIVOS', ''], ['EN PLANEACIÓN', ''], ['ENVIADO', '']])
catalog = {{'SERVICIO': ['PLANEACIÓN'], 'ARCHIVOS RECIBIDOS': ['STLs']}}
with patch.object(lab, 'get_workbench_form_catalog', lambda: catalog), \
     patch.object(lab, 'workbench_sent_pending_count', lambda: 0), \
     patch.object(lab, 'read_sheet_df', Mock(return_value=pd.DataFrame([row]))), \
     patch.object(align, 'get_order_form_catalog', lambda: catalog), \
     patch.object(align, 'read_order_values', Mock()), \
     patch.object(align, 'read_orders_df', lambda sheet=None: pd.DataFrame([row])):
    if case_type == 'aparatos':
        lab.render_workbench_order_editor(pd.Series(row), 'Jime')
    else:
        align.render_aligner_order_editor(pd.Series(row), 'Jime', definitions)
'''


@pytest.mark.parametrize('case_type', ['aparatos', 'alineadores', 'polanco'])
def test_pdf_upload_from_service_section_preserves_order_drafts_and_allows_more_invoices(store, case_type):
    at = AppTest.from_string(EDITOR_SCRIPT, default_timeout=15)
    at.session_state['case_type'] = case_type
    at.run()
    assert not at.exception and not at.file_uploader
    patient = next(field for field in at.text_input if 'NOMBRE PACIENTE' in field.label)
    token = patient.key.split('_')[2]
    at.session_state['service_key'] = f'detail_group_{token}_{invoices.SERVICE_SECTION}'
    at.run()
    assert not at.exception and len(at.file_uploader) == 1
    assert at.file_uploader[0].accept_multiple_files and at.file_uploader[0].allowed_type == ['.pdf']
    patient = next(field for field in at.text_input if 'NOMBRE PACIENTE' in field.label)
    patient.set_value('Paciente corregido').run()
    at.file_uploader[0].set_value([('uno.pdf', PDF, 'application/pdf'), ('dos.pdf', PDF, 'application/pdf')]).run()
    next(button for button in at.button if button.label == 'Guardar facturas').click().run()
    assert not at.exception
    assert len(store.list(case_type, '001')) == 2
    assert next(field for field in at.text_input if 'NOMBRE PACIENTE' in field.label).value == 'Paciente corregido'
    assert not next(button for button in at.button if button.label == '💾 Guardar este pedido').disabled
    assert not at.file_uploader[0].value
    at.file_uploader[0].set_value(('otra.pdf', PDF, 'application/pdf')).run()
    next(button for button in at.button if button.label == 'Guardar facturas').click().run()
    assert not at.exception and len(store.list(case_type, '001')) == 3
    assert len(at.get('link_button')) == 6


SEARCH_SCRIPT = f'''
import sys
sys.path.insert(0, {str(ROOT)!r})
import streamlit as st
import pandas as pd
import case_search as search
st.session_state.setdefault('rows', [
    {{'Folio': '001', 'NOMBRE DOCTOR': 'Dra. López', 'NOMBRE PACIENTE': 'José Martín', 'STATUS': 'PRODUCTO ENVIADO'}},
    {{'Folio': '002', 'NOMBRE DOCTOR': 'Dra. López', 'NOMBRE PACIENTE': 'Otro paciente', 'STATUS': 'CANCELO'}},
])
st.session_state.setdefault('loads', 0)
def load():
    st.session_state['loads'] += 1
    return search.source_cases(pd.DataFrame(st.session_state['rows']), 'aparatos', 'Folio')
def verify(case_type, identifier):
    import case_invoices
    case_invoices.require_unique_case(pd.DataFrame(st.session_state['rows']), identifier, 'Folio')
search.render('Jime', namespace='demo', load_cases=load, verify_case=verify, rerun=st.rerun)
'''


def test_search_shows_archived_summary_and_only_its_invoices_and_accepts_more(store):
    store.upload('aparatos', '001', [Upload('factura-correcta.pdf')], 'Jime', lambda: None)
    store.upload('aparatos', '002', [Upload('otra-orden.pdf')], 'Jime', lambda: None)
    at = AppTest.from_string(SEARCH_SCRIPT, default_timeout=15).run()
    assert not at.exception and at.session_state['loads'] == 0
    at.text_input[0].set_value('jose').run()
    assert not at.exception
    assert any(item.value == 'Orden · 001' for item in at.subheader)
    assert any('PRODUCTO ENVIADO' in item.value for item in at.text)
    assert any(item.value == 'factura-correcta.pdf' for item in at.markdown)
    assert not any(item.value == 'otra-orden.pdf' for item in at.markdown)
    at.file_uploader[0].set_value(('posterior.pdf', PDF, 'application/pdf')).run()
    next(button for button in at.button if button.label == 'Guardar facturas').click().run()
    assert not at.exception and len(store.list('aparatos', '001')) == 2
    assert at.session_state['rows'][0]['STATUS'] == 'PRODUCTO ENVIADO'


def test_search_multiple_results_require_selection_and_keep_identity_after_reordering(store):
    at = AppTest.from_string(SEARCH_SCRIPT, default_timeout=15).run()
    at.text_input[0].set_value('lopez').run()
    assert at.selectbox[0].value is None and not at.file_uploader
    selected = 'aparatos\0' + '002\0' + '0'
    at.selectbox[0].select(selected).run()
    assert any(item.value == 'Orden · 002' for item in at.subheader)
    at.session_state['rows'] = list(reversed(at.session_state['rows']))
    at.run()
    assert not at.exception and any(item.value == 'Orden · 002' for item in at.subheader)


def test_duplicate_search_result_cannot_access_or_upload_invoices(store):
    at = AppTest.from_string(SEARCH_SCRIPT, default_timeout=15).run()
    at.session_state['rows'] = [at.session_state['rows'][0], dict(at.session_state['rows'][0])]
    at.text_input[0].set_value('jose').run()
    at.selectbox[0].select('aparatos\0' + '001\0' + '0').run()
    assert not at.exception
    assert any('duplicado' in item.value for item in at.error)
    assert not at.file_uploader and not at.get('link_button')


@pytest.mark.parametrize('partial', [{}, {'DETALLE COMENTARIOS': 'Sólo información administrativa'}])
@pytest.mark.parametrize('query', ['jose', '001'])
def test_partial_duplicate_blocks_invoice_access_even_with_one_search_result(store, monkeypatch, partial, query):
    store.upload('aparatos', '001', [Upload('factura-privada.pdf')], 'Jime', lambda: None)
    listing = Mock()
    monkeypatch.setattr(invoices, 'list_invoices', listing)
    at = AppTest.from_string(SEARCH_SCRIPT, default_timeout=15).run()
    at.session_state['rows'] = [at.session_state['rows'][0], {'Folio': ' 001 ', **partial}]
    at.text_input[0].set_value(query).run()
    assert not at.exception
    assert any('1 caso(s)' in item.value for item in at.caption)
    assert any('duplicado' in item.value for item in at.error)
    assert not at.file_uploader and not at.get('link_button')
    listing.assert_not_called()


def test_aligner_search_preserves_invoice_origin_when_folios_match(store, monkeypatch):
    rows = {sheet: pd.DataFrame([{'No. Orden': '001', 'PRODUCTO': 'Graphy',
            'NOMBRE PACIENTE': 'Paciente compartido', 'NOMBRE DOCTOR': sheet, 'STATUS': 'ENVIADO'}])
            for sheet in (align.SHEET_ORDERS, align.SHEET_POLANCO)}
    reads = []
    def read(sheet=None):
        reads.append(sheet)
        return rows[sheet or align.SHEET_ORDERS]
    values = Mock()
    monkeypatch.setattr(align, 'read_orders_df', read)
    monkeypatch.setattr(align, 'read_order_values', values)
    store.upload('alineadores', '001', [Upload('alineadores.pdf')], 'Jime', lambda: None)
    store.upload('polanco', '001', [Upload('polanco.pdf')], 'Jime', lambda: None)
    script = f'''
import sys
sys.path.insert(0, {str(ROOT)!r})
import alineadores_pg as app
import streamlit as st
st.session_state['aligners_order_sheet'] = app.SHEET_ORDERS
app.render_case_search('Jime')
'''
    at = AppTest.from_string(script, default_timeout=15).run()
    assert not at.exception and not reads
    at.text_input[0].set_value('compartido').run()
    assert {align.SHEET_ORDERS, align.SHEET_POLANCO}.issubset(reads)
    assert at.selectbox[0].value is None
    at.selectbox[0].select('polanco\0' + '001\0' + '0').run()
    assert any(item.value == 'polanco.pdf' for item in at.markdown)
    assert not any(item.value == 'alineadores.pdf' for item in at.markdown)
    at.file_uploader[0].set_value(('posterior.pdf', PDF, 'application/pdf')).run()
    next(button for button in at.button if button.label == 'Guardar facturas').click().run()
    assert not at.exception
    values.clear.assert_called_with(align.SHEET_POLANCO)
    assert len(store.list('polanco', '001')) == 2 and len(store.list('alineadores', '001')) == 1
    assert at.session_state['aligners_order_sheet'] == align.SHEET_ORDERS


def test_apparatus_search_tab_does_not_discard_pending_order_changes(monkeypatch):
    state = {'workbench_active_tab': '📋 Seguimiento', 'lab_primary_tabs_Jime': search.TAB_LABEL,
             'order_detail_drafts_apparatus': {'001': {'changes': {'NOMBRE DOCTOR': 'Corrección pendiente'}}}}
    monkeypatch.setattr(lab.st, 'session_state', state)
    lab.guard_workbench_tab_change('Jime')
    assert state['lab_primary_tabs_Jime'] == '📋 Seguimiento'
    assert state['workbench_tab_warning']
    assert state['order_detail_drafts_apparatus']['001']['changes'] == {'NOMBRE DOCTOR': 'Corrección pendiente'}
