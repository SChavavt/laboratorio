"""Búsqueda resumida de pedidos, incluidas órdenes archivadas, en ambas vistas."""
import re
import unicodedata
from urllib.parse import unquote, urlsplit

import pandas as pd
import streamlit as st

import case_invoices

TAB_LABEL = "🔎 Buscar casos"
SOURCE_COLUMN = "_CASE_SOURCE"
ID_COLUMN = "_CASE_ID"
SEARCH_COLUMNS = (ID_COLUMN, "NOMBRE DOCTOR", "NOMBRE PACIENTE", "APARATO", "PRODUCTO", "SERVICIO", "STATUS")


def text(value) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def normalized(value) -> str:
    return "".join(char for char in unicodedata.normalize("NFKD", text(value)).casefold()
                   if not unicodedata.combining(char))


def source_cases(frame: pd.DataFrame, case_type: str, id_column: str) -> pd.DataFrame:
    if id_column not in frame:
        return pd.DataFrame()
    result = frame.copy()
    result[ID_COLUMN] = result[id_column].map(text)
    result[SOURCE_COLUMN] = case_type
    meaningful = [column for column in SEARCH_COLUMNS if column != ID_COLUMN and column in result]
    mask = result[ID_COLUMN].ne("")
    if meaningful:
        mask &= result[meaningful].apply(lambda row: any(text(value) for value in row), axis=1)
    return result[mask].reset_index(drop=True)


def find_cases(frame: pd.DataFrame, query: str) -> pd.DataFrame:
    terms = normalized(query).split()
    if not terms or frame.empty:
        return frame.iloc[:0].copy()
    fields = [column for column in SEARCH_COLUMNS if column in frame]
    searchable = frame[fields].apply(lambda row: normalized(" ".join(text(value) for value in row)), axis=1)
    return frame[searchable.map(lambda value: all(term in value for term in terms))].reset_index(drop=True)


def row_file_links(row) -> list[tuple[str, str]]:
    links = {}
    for column, value in row.items():
        if not any(word in normalized(column) for word in ("archivo", "enlace", "link", "url", "comprobante")):
            continue
        for url in re.findall(r"https?://[^\s<>\"']+", text(value)):
            url = url.rstrip(",;)")
            label = unquote(urlsplit(url).path.rsplit("/", 1)[-1]) or str(column)
            links.setdefault(url, label)
    return [(label, url) for url, label in links.items()]


def case_label(row) -> str:
    parts = [row[ID_COLUMN], text(row.get("NOMBRE PACIENTE")), text(row.get("NOMBRE DOCTOR")),
             text(row.get("APARATO", row.get("PRODUCTO"))), case_invoices.CASE_TYPES[row[SOURCE_COLUMN]]]
    return " · ".join(part for part in parts if part)


def render(current_user: str, *, namespace: str, load_cases, verify_case, rerun) -> None:
    if current_user not in case_invoices.ALLOWED_USERS:
        st.error("Tu usuario no tiene acceso al buscador de casos.")
        return
    st.subheader(TAB_LABEL)
    st.caption("Consulta órdenes activas, enviadas y archivadas, junto con sus facturas.")
    query = st.text_input("Buscar por paciente, doctor, folio o aparato/producto", key=f"case_search_{namespace}")
    if not query.strip():
        st.info("Escribe un paciente, doctor o folio para buscar su caso.")
        return
    try:
        cases = load_cases()
    except Exception:
        st.error("No se pudieron consultar los casos. Intenta de nuevo.")
        return
    matches = find_cases(cases, query)
    if matches.empty:
        st.info("No se encontraron casos con esa búsqueda.")
        return
    st.caption(f"{len(matches)} caso(s) encontrado(s)")
    occurrences = matches.groupby([SOURCE_COLUMN, ID_COLUMN]).cumcount()
    keys = [f"{row[SOURCE_COLUMN]}\0{row[ID_COLUMN]}\0{occurrences.iloc[index]}"
            for index, (_, row) in enumerate(matches.iterrows())]
    lookup = dict(zip(keys, (row for _, row in matches.iterrows())))
    selected = st.selectbox("Caso para consultar", keys,
                           format_func=lambda key: case_label(lookup[key]),
                           index=0 if len(matches) == 1 else None, placeholder="Selecciona una orden",
                           key=f"case_result_{namespace}_{normalized(query)}")
    if selected is None:
        return
    row = lookup[selected]
    identifier, case_type = row[ID_COLUMN], row[SOURCE_COLUMN]
    with st.container(border=True):
        st.subheader(f"Orden · {identifier}")
        fields = [("Vista", case_invoices.CASE_TYPES[case_type]),
                  ("Paciente", row.get("NOMBRE PACIENTE")), ("Doctor", row.get("NOMBRE DOCTOR")),
                  ("Aparato / producto", row.get("APARATO", row.get("PRODUCTO"))),
                  ("Etapa", row.get("STATUS")), ("Servicio", row.get("SERVICIO")),
                  ("Fecha de recepción", row.get("FECHA DE RECEPCIÓN", row.get("FECHA RECEPCIÓN"))),
                  ("Fecha de envío", row.get("FECHA ENVÍO"))]
        columns = st.columns(2)
        for index, (label, value) in enumerate(fields):
            if text(value):
                columns[index % 2].text(f"{label}: {text(value)}")
        duplicates = cases[SOURCE_COLUMN].eq(case_type) & cases[ID_COLUMN].eq(identifier)
        if duplicates.sum() != 1:
            st.error("Este folio está duplicado. Corrige la hoja para consultar y adjuntar sus facturas.")
            return
        with st.expander(case_invoices.SERVICE_SECTION, expanded=True):
            for label, url in row_file_links(row):
                st.link_button(label, url)
            case_invoices.render_section(case_type, identifier, current_user,
                verify_case=lambda: verify_case(case_type, identifier), rerun=rerun)
