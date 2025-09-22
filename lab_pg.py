import streamlit as st
import pandas as pd
from datetime import datetime
import gspread
import json
from gspread.utils import rowcol_to_a1
from google.oauth2.service_account import Credentials

# ==============================
# 🔧 CONFIGURACIÓN
# ==============================
SCOPE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

COLUMNS = [
    "No_orden",
    "Nombre_paciente",
    "Nombre_doctor",
    "Status",
    "Status_NEMO",
    "Tipo_alineador",
    "Fecha_recepcion",
    "Dias_entrega",
    "Comentarios",
    "Notas",
    "Responsable_SUD",
    "Fecha_inicio_SUD",
    "Hora_inicio_SUD",
    "Plantilla_superior",
    "Plantilla_inferior",
    "IPR",
    "No_alineadores_superior",
    "No_alineadores_inferior",
    "Total_alineadores",
    "Fecha_solicitud_envio",
    "Ultima_Modificacion",
]

RESPONSABLE_SUD_OPTIONS = [
    "Selecciona",
    "Arq Brenda",
    "Karen",
    "Melissa",
    "Georgina",
    "Carolina",
    "Daniela",
]

STATUS_OPTIONS = [
    "1. Revisión de scan",
    "2. Por hacer Setup",
    "2.1 Scan con falla",
    "3. RETENEDOR",
    "3.1 RETENEDOR en proceso",
    "3.2 SUD en proceso",
    "3.3 SUD proceso modificación",
    "4. Para revisión de SUD 1",
    "4.1 Para revisión de SUD 2",
    "5. Revisado – hay que modificar",
    "5.1 Revisión alineación y nivelación",
    "5.2 Revisión attachments y secuencia",
    "5.3 Revisado VOBO y cotizar",
    "6. Para cotización",
    "7. Cotización lista",
    "7.1 Listo para envío al Dr.",
    "8. Enviado al Dr.",
    "9. Solicitud cambios Dr.",
    "10. Exportar modelos e imprimir",
    "11. Imprimir modelo",
    "12.1 Enviar biomodelos",
    "12.3 Biomodelos enviados",
    "13. Solo alineador",
    "13.1 Para impresión",
    "13.2 Reimpresión sin costo",
    "14. Enviado a impresión",
    "14.1 En impresión",
    "15. Impresión perfecta",
    "16. Error de impresión",
    "17. En calidad",
    "Revisados",
    "Falta Pago",
    "Esperando respuesta del Dr.",
    "REFINAMIENTO",
    "16. Enviado / empaquetado",
]

STATUS_NEMO_OPTIONS = [
    "Nuevo",
    "Carpeta específica",
    "Duplicado",
    "Seguimiento",
    "Solo impresión",
]

# ==============================
# 🔐 CLIENTE GOOGLE SHEETS
# ==============================
def _get_gs_client():
    creds_str = st.secrets["gsheets"]["google_credentials"]  # 👈 viene como string
    creds_info = json.loads(creds_str)  # 👈 lo convertimos en dict
    credentials = Credentials.from_service_account_info(creds_info, scopes=SCOPE)
    return gspread.authorize(credentials)
    
@st.cache_resource
def get_worksheet():
    client = _get_gs_client()
    sheet_id = st.secrets["gsheets"]["sheet_id"]
    ws_name = "Procesos_Graphy"

    ss = client.open_by_key(sheet_id)
    try:
        ws = ss.worksheet(ws_name)
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(title=ws_name, rows="1000", cols=str(len(COLUMNS)))
        ws.update("A1", [COLUMNS])
    else:
        # Mantener sincronizada la fila de encabezados con los campos definidos.
        current_headers = ws.row_values(1)
        if current_headers[: len(COLUMNS)] != COLUMNS:
            ws.update("A1", [COLUMNS])
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

def append_row(row):
    ws = get_worksheet()
    values = [str(row.get(c, "")) for c in COLUMNS]
    ws.append_row(values, value_input_option="USER_ENTERED")


def update_process(no_orden, data_dict):
    df = fetch_df()
    if df.empty:
        return False

    procesos = df[df["No_orden"].astype(str).str.strip() != ""]
    if procesos.empty:
        return False

    matching = procesos[procesos["No_orden"] == no_orden]
    if matching.empty:
        return False

    row_idx = matching.index[0]
    row_number = row_idx + 2  # encabezado +1

    start_col = COLUMNS.index("Responsable_SUD") + 1
    end_col = COLUMNS.index("Fecha_solicitud_envio") + 1

    row_values = [matching.iloc[0].get(col, "") for col in COLUMNS]
    for col, value in data_dict.items():
        if col in COLUMNS:
            row_values[COLUMNS.index(col)] = str(value)

    ws = get_worksheet()
    start_cell = rowcol_to_a1(row_number, start_col)
    end_cell = rowcol_to_a1(row_number, end_col)
    ws.update(
        f"{start_cell}:{end_cell}",
        [row_values[start_col - 1 : end_col]],
    )
    ws.update(
        rowcol_to_a1(row_number, COLUMNS.index("Ultima_Modificacion") + 1),
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )
    return True

# ==============================
# 🚀 APP STREAMLIT
# ==============================
st.set_page_config(page_title="Procesos – ARTTDLAB", layout="wide")
st.title("🧪 Plataforma de Procesos – ARTTDLAB")

tab1, tab_sud, tab2 = st.tabs(["➕ Nuevo Proceso", "SUD", "📋 Consulta"])

# ➕ NUEVO PROCESO
with tab1:
    st.subheader("🆕 Registrar nuevo proceso")

    with st.form("form_nuevo"):
        in_orden = st.text_input("🧾 No. orden *")
        in_paciente = st.text_input("🧑‍🦱 Nombre del paciente *")
        in_doctor = st.text_input("🧑‍⚕️ Nombre del doctor *")
        in_status = st.selectbox("📌 Status *", STATUS_OPTIONS)
        in_status_nemo = st.selectbox("🌐 Status en NEMO *", STATUS_NEMO_OPTIONS)
        in_tipo_alineador = st.selectbox(
            "🦷 Tipo de alineador *", ["Graphy", "Convencional"]
        )
        in_fecha_recepcion = st.date_input("📅 Fecha de recepción *", datetime.today())
        in_dias_entrega = st.number_input(
            "⏳ Días de entrega *", min_value=1, value=1, step=1
        )
        in_comentarios = st.text_area("💬 Comentarios")
        in_notas = st.text_area("📝 Notas")
        enviado = st.form_submit_button("💾 Guardar")

    if (
        enviado
        and in_orden
        and in_paciente
        and in_doctor
        and in_status
        and in_status_nemo
        and in_tipo_alineador
        and in_fecha_recepcion
        and in_dias_entrega
    ):
        row = {
            "No_orden": in_orden,
            "Nombre_paciente": in_paciente,
            "Nombre_doctor": in_doctor,
            "Status": in_status,
            "Status_NEMO": in_status_nemo,
            "Tipo_alineador": in_tipo_alineador,
            "Fecha_recepcion": in_fecha_recepcion.strftime("%Y-%m-%d"),
            "Dias_entrega": str(int(in_dias_entrega)),
            "Comentarios": in_comentarios,
            "Notas": in_notas,
            "Responsable_SUD": "",
            "Fecha_inicio_SUD": "",
            "Hora_inicio_SUD": "",
            "Plantilla_superior": "",
            "Plantilla_inferior": "",
            "IPR": "",
            "No_alineadores_superior": "",
            "No_alineadores_inferior": "",
            "Total_alineadores": "",
            "Fecha_solicitud_envio": "",
            "Ultima_Modificacion": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        append_row(row)
        st.success("🎉 Proceso registrado correctamente.")
        st.cache_data.clear()
    elif enviado:
        st.error("⚠️ Por favor completa los campos obligatorios (*).")

# 🧩 SUD
with tab_sud:
    st.subheader("Gestión de SUD")
    df_sud = fetch_df()

    if df_sud.empty or df_sud["No_orden"].astype(str).str.strip().eq("").all():
        st.info("No hay procesos disponibles para actualizar.")
    else:
        procesos = df_sud[df_sud["No_orden"].astype(str).str.strip() != ""].copy()
        procesos["No_orden"] = procesos["No_orden"].astype(str)
        procesos["Nombre_paciente"] = procesos["Nombre_paciente"].astype(str)
        display_labels = {}
        for _, row in procesos.iterrows():
            label = (
                f"{row['No_orden']} – {row['Nombre_paciente']}"
                if row["Nombre_paciente"].strip()
                else str(row["No_orden"])
            )
            display_labels[row["No_orden"]] = label
        opciones = list(dict.fromkeys(procesos["No_orden"].tolist()))

        if not opciones:
            st.info("No hay procesos con número de orden válido.")
        else:
            selected_no_orden = st.selectbox(
                "Selecciona el proceso",
                opciones,
                format_func=lambda x: display_labels.get(x, str(x)),
            )

            selected_row = procesos[procesos["No_orden"] == selected_no_orden].iloc[0]

            def _parse_date(value):
                try:
                    return datetime.strptime(value, "%Y-%m-%d").date()
                except Exception:
                    return None

            def _parse_time(value):
                for fmt in ("%H:%M", "%H:%M:%S"):
                    try:
                        return datetime.strptime(value, fmt).time()
                    except Exception:
                        continue
                return None

            fecha_inicio_val = _parse_date(selected_row.get("Fecha_inicio_SUD", ""))
            fecha_solicitud_val = _parse_date(
                selected_row.get("Fecha_solicitud_envio", "")
            )
            hora_inicio_val = _parse_time(selected_row.get("Hora_inicio_SUD", ""))

            responsable_default = selected_row.get("Responsable_SUD", "")
            responsable_options = RESPONSABLE_SUD_OPTIONS.copy()
            if (
                responsable_default
                and responsable_default not in responsable_options
            ):
                responsable_options.append(responsable_default)
            responsable_index = (
                responsable_options.index(responsable_default)
                if responsable_default in responsable_options
                else 0
            )

            with st.form("form_sud"):
                responsable = st.selectbox(
                    "Responsable hacer SUD *",
                    responsable_options,
                    index=responsable_index,
                )
                fecha_inicio = st.date_input(
                    "Fecha inicio SUD *",
                    value=fecha_inicio_val or datetime.today().date(),
                )
                hora_inicio = st.time_input(
                    "Hora de inicio *",
                    value=hora_inicio_val or datetime.now().time().replace(second=0, microsecond=0),
                )
                plantilla_superior = st.text_input(
                    "Plantilla superior *",
                    value=selected_row.get("Plantilla_superior", ""),
                )
                plantilla_inferior = st.text_input(
                    "Plantilla inferior *",
                    value=selected_row.get("Plantilla_inferior", ""),
                )
                ipr_default = selected_row.get("IPR", "-") if selected_row.get("IPR") else "-"
                ipr = st.radio("IPR *", options=["x", "-"], index=0 if ipr_default == "x" else 1)
                no_sup = st.number_input(
                    "No. alineadores superior *",
                    min_value=0,
                    value=int(selected_row.get("No_alineadores_superior", "0") or 0),
                    step=1,
                )
                no_inf = st.number_input(
                    "No. alineadores inferior *",
                    min_value=0,
                    value=int(selected_row.get("No_alineadores_inferior", "0") or 0),
                    step=1,
                )
                total_alineadores = no_sup + no_inf
                st.number_input(
                    "Total alineadores",
                    min_value=0,
                    value=total_alineadores,
                    step=1,
                    disabled=True,
                )
                fecha_solicitud = st.date_input(
                    "Fecha solicitud de envío *",
                    value=fecha_solicitud_val or datetime.today().date(),
                )

                guardar_sud = st.form_submit_button("Actualizar SUD")

            if guardar_sud:
                errores = []
                if responsable in ("", "Selecciona"):
                    errores.append("Selecciona un responsable.")
                if not plantilla_superior.strip():
                    errores.append("Ingresa la plantilla superior.")
                if not plantilla_inferior.strip():
                    errores.append("Ingresa la plantilla inferior.")

                if errores:
                    st.error("\n".join(errores))
                else:
                    data = {
                        "Responsable_SUD": responsable,
                        "Fecha_inicio_SUD": fecha_inicio.isoformat(),
                        "Hora_inicio_SUD": hora_inicio.strftime("%H:%M"),
                        "Plantilla_superior": plantilla_superior.strip(),
                        "Plantilla_inferior": plantilla_inferior.strip(),
                        "IPR": ipr,
                        "No_alineadores_superior": str(no_sup),
                        "No_alineadores_inferior": str(no_inf),
                        "Total_alineadores": str(total_alineadores),
                        "Fecha_solicitud_envio": fecha_solicitud.isoformat(),
                    }

                    if update_process(selected_no_orden, data):
                        st.success("Información SUD actualizada correctamente.")
                        st.cache_data.clear()
                    else:
                        st.error(
                            "No se encontró el proceso o no fue posible actualizar los datos."
                        )

# 📋 CONSULTA
with tab2:
    st.subheader("Listado de procesos")
    df = fetch_df()
    if not df.empty:
        st.dataframe(df, use_container_width=True, height=600)
    else:
        st.info("No hay procesos registrados aún.")
