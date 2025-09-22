import streamlit as st
import pandas as pd
import time
from datetime import datetime, date
import gspread
from google.oauth2.service_account import Credentials
import json

# ==============================
# 🔧 CONFIGURACIÓN
# ==============================
SCOPE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

COLUMNS = [
    "ID",
    "Proceso",
    "Responsable",
    "Estado",
    "Prioridad",
    "Fecha_Inicio",
    "Fecha_Compromiso",
    "Fecha_Cierre",
    "Comentarios",
    "Ultima_Modificacion",
]

ESTADOS = ["Planificado", "En Progreso", "Bloqueado", "Completado"]
PRIORIDADES = ["Alta", "Media", "Baja"]

# ==============================
# 🔐 CLIENTE GOOGLE SHEETS
# ==============================
def _get_gs_client():
    creds_dict = json.loads(st.secrets["gsheets"]["google_credentials"])
    credentials = Credentials.from_service_account_info(creds_dict, scopes=SCOPE)
    return gspread.authorize(credentials)

@st.cache_resource
def get_worksheet():
    client = _get_gs_client()
    sheet_id = st.secrets["gsheets"]["sheet_id"]
    ws_name = st.secrets["gsheets"]["worksheet_name"]

    ss = client.open_by_key(sheet_id)
    try:
        ws = ss.worksheet(ws_name)
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(title=ws_name, rows="1000", cols=str(len(COLUMNS)))
        ws.update([COLUMNS])
    return ws

@st.cache_data(ttl=30)
def fetch_df():
    ws = get_worksheet()
    values = ws.get_all_values()
    if not values:
        return pd.DataFrame(columns=COLUMNS)
    df = pd.DataFrame(values[1:], columns=values[0])
    for c in COLUMNS:
        if c not in df.columns:
            df[c] = ""
    return df[COLUMNS]

def next_id(df):
    if df.empty or df["ID"].isna().all():
        return 1
    return int(pd.to_numeric(df["ID"], errors="coerce").max()) + 1

def append_row(row):
    ws = get_worksheet()
    values = [str(row.get(c, "")) for c in COLUMNS]
    ws.append_row(values, value_input_option="USER_ENTERED")

def update_row_by_id(id_value, row):
    ws = get_worksheet()
    df = fetch_df()
    matches = df.index[df["ID"] == str(id_value)].tolist()
    if not matches:
        return False
    idx = matches[0]
    sheet_row = idx + 2
    values = [str(row.get(c, "")) for c in COLUMNS]
    ws.update(f"A{sheet_row}:{chr(64+len(COLUMNS))}{sheet_row}", [values])
    return True

# ==============================
# 🚀 APP STREAMLIT
# ==============================
st.set_page_config(page_title="Procesos ARTTDLAB", layout="wide")
st.title("📋 Plataforma de Procesos – ARTTDLAB")

tab1, tab2, tab3, tab4, tab5 = st.tabs(
    ["📋 Lista", "➕ Nuevo", "✏️ Editar", "📊 Estadísticas", "⚙️ Admin"]
)

# 📋 LISTA
with tab1:
    st.subheader("Listado de Procesos")
    df = fetch_df()

    with st.expander("🔎 Filtros", expanded=True):
        col1, col2, col3 = st.columns(3)
        estado = col1.selectbox("Estado", ["(Todos)"] + ESTADOS)
        prioridad = col2.selectbox("Prioridad", ["(Todos)"] + PRIORIDADES)
        responsable = col3.text_input("Responsable contiene...")
        buscar = st.text_input("Buscar en Proceso/Comentarios...")

    if estado != "(Todos)":
        df = df[df["Estado"] == estado]
    if prioridad != "(Todos)":
        df = df[df["Prioridad"] == prioridad]
    if responsable:
        df = df[df["Responsable"].str.contains(responsable, case=False, na=False)]
    if buscar:
        df = df[
            df["Proceso"].str.contains(buscar, case=False, na=False)
            | df["Comentarios"].str.contains(buscar, case=False, na=False)
        ]

    st.dataframe(df, use_container_width=True, height=500)

# ➕ NUEVO
with tab2:
    st.subheader("Registrar nuevo proceso")
    base_df = fetch_df()
    new_id = next_id(base_df)

    with st.form("form_nuevo"):
        st.markdown(f"**Nuevo ID:** `{new_id}`")
        in_proceso = st.text_input("Proceso *")
        in_responsable = st.text_input("Responsable *")
        in_estado = st.selectbox("Estado", ESTADOS)
        in_prioridad = st.selectbox("Prioridad", PRIORIDADES)
        in_fini = st.date_input("Fecha Inicio", value=date.today())
        in_fcomp = st.date_input("Fecha Compromiso")
        in_coment = st.text_area("Comentarios")
        enviado = st.form_submit_button("💾 Guardar")

    if enviado and in_proceso and in_responsable:
        row = {
            "ID": new_id,
            "Proceso": in_proceso,
            "Responsable": in_responsable,
            "Estado": in_estado,
            "Prioridad": in_prioridad,
            "Fecha_Inicio": in_fini.strftime("%Y-%m-%d"),
            "Fecha_Compromiso": in_fcomp.strftime("%Y-%m-%d") if in_fcomp else "",
            "Fecha_Cierre": "",
            "Comentarios": in_coment,
            "Ultima_Modificacion": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        append_row(row)
        st.success("✅ Proceso creado correctamente.")
        st.cache_data.clear()

# ✏️ EDITAR
with tab3:
    st.subheader("Editar proceso existente")
    df_edit = fetch_df()
    if not df_edit.empty:
        sel_id = st.selectbox("Selecciona ID", options=df_edit["ID"])
        row_now = df_edit[df_edit["ID"] == sel_id].iloc[0]

        with st.form("form_editar"):
            ed_proceso = st.text_input("Proceso *", value=row_now["Proceso"])
            ed_responsable = st.text_input("Responsable *", value=row_now["Responsable"])
            ed_estado = st.selectbox(
                "Estado",
                ESTADOS,
                index=ESTADOS.index(row_now["Estado"])
                if row_now["Estado"] in ESTADOS
                else 0,
            )
            ed_prioridad = st.selectbox(
                "Prioridad",
                PRIORIDADES,
                index=PRIORIDADES.index(row_now["Prioridad"])
                if row_now["Prioridad"] in PRIORIDADES
                else 1,
            )
            ed_coment = st.text_area("Comentarios", value=row_now["Comentarios"])
            enviado = st.form_submit_button("💾 Actualizar")

        if enviado:
            new_row = {
                "ID": sel_id,
                "Proceso": ed_proceso,
                "Responsable": ed_responsable,
                "Estado": ed_estado,
                "Prioridad": ed_prioridad,
                "Fecha_Inicio": row_now["Fecha_Inicio"],
                "Fecha_Compromiso": row_now["Fecha_Compromiso"],
                "Fecha_Cierre": row_now["Fecha_Cierre"],
                "Comentarios": ed_coment,
                "Ultima_Modificacion": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
            update_row_by_id(sel_id, new_row)
            st.success("✅ Proceso actualizado correctamente.")
            st.cache_data.clear()

# 📊 ESTADÍSTICAS
with tab4:
    st.subheader("Estadísticas de Procesos")
    df_stats = fetch_df()
    if not df_stats.empty:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Por Estado**")
            st.bar_chart(df_stats["Estado"].value_counts())
        with col2:
            st.markdown("**Por Prioridad**")
            st.bar_chart(df_stats["Prioridad"].value_counts())

# ⚙️ ADMIN
with tab5:
    st.subheader("Administración")
    df_exp = fetch_df()
    if not df_exp.empty:
        csv = df_exp.to_csv(index=False).encode("utf-8")
        st.download_button(
            "⬇️ Descargar CSV",
            csv,
            "procesos_arttdlab.csv",
            mime="text/csv",
        )

    st.write(f"**Sheet ID:** {st.secrets['gsheets']['sheet_id']}")
    st.write(f"**Worksheet:** {st.secrets['gsheets']['worksheet_name']}")
