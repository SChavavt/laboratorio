"""Ficha compartida de pedidos. Los borradores nunca escriben en Sheets."""

import hashlib
import html
import json
import re
from datetime import datetime

import pandas as pd
import streamlit as st
from dropdown_fields import join_values, option_colors, split_values


def order_title(row) -> str:
    customer = str(row.get("NOMBRE DOCTOR", "") or "").strip()
    patient = str(row.get("NOMBRE PACIENTE", "") or "").strip()
    return "Pedido · " + (customer or patient or "Sin nombre de cliente")


def detail_columns(columns, editable, pinned) -> list[str]:
    return [column for column in columns if column in editable and column not in pinned
            and not column.startswith(("_", "Columna_"))
            and re.sub(r"_\d+$", "", column).strip().upper() != "TOTAL"]


def parse_sheet_catalogs(metadata, canonicalize=lambda value: value.strip()):
    """Lee las listas de tablas nativas y las validaciones de celdas tradicionales."""
    result = {}
    for sheet in metadata.get("sheets", []):
        catalog = {}
        for table in sheet.get("tables", []):
            if table.get("range", {}).get("startRowIndex") != 1:
                continue
            for column in table.get("columnProperties", []):
                condition = column.get("dataValidationRule", {}).get("condition", {})
                if condition.get("type") == "ONE_OF_LIST":
                    catalog[canonicalize(column.get("columnName", ""))] = list(dict.fromkeys(
                        value["userEnteredValue"] for value in condition.get("values", [])
                        if str(value.get("userEnteredValue", "")).strip()))
        # El rango leído comienza en la fila de encabezados, no en toda la hoja.
        for block in sheet.get("data", []):
            rows = block.get("rowData", [])
            if block.get("startRow", 0) != 1 or not rows:
                continue
            headers = [canonicalize(cell.get("formattedValue", ""))
                       for cell in rows[0].get("values", [])]
            for row in rows[1:]:
                for index, cell in enumerate(row.get("values", [])):
                    condition = cell.get("dataValidation", {}).get("condition", {})
                    if index < len(headers) and headers[index] not in catalog and condition.get("type") == "ONE_OF_LIST":
                        catalog[headers[index]] = list(dict.fromkeys(
                            value["userEnteredValue"] for value in condition.get("values", [])
                            if str(value.get("userEnteredValue", "")).strip()))
        result[sheet.get("properties", {}).get("title", "")] = catalog
    return result


def draft_key(namespace):
    return f"order_detail_drafts_{namespace}"


def pending_count(namespace):
    return sum(bool(draft["changes"]) for draft in st.session_state.get(draft_key(namespace), {}).values())


def clear_drafts(namespace):
    st.session_state.pop(draft_key(namespace), None)
    key = f"order_detail_version_{namespace}"
    st.session_state[key] = st.session_state.get(key, 0) + 1


def rows_with_drafts(selected, namespace, id_column):
    """Uncheck no pierde el formulario que todavía tiene cambios sin guardar."""
    rows = selected.to_dict("records")
    identifiers = {str(row[id_column]) for row in rows}
    for identifier, draft in st.session_state.get(draft_key(namespace), {}).items():
        if draft["changes"] and identifier not in identifiers:
            rows.append(draft["baseline"])
    return pd.DataFrame(rows)


def choose_order(rows, namespace, id_column):
    if rows.empty:
        return None
    if rows[id_column].duplicated().any():
        st.error("Hay identificadores duplicados. Corrige la hoja antes de editar estos pedidos.")
        return None
    identifiers = rows[id_column].tolist()
    key = f"order_detail_choice_{namespace}"
    if st.session_state.get(key) not in identifiers:
        st.session_state[key] = identifiers[0]
    if len(rows) > 1:
        labels = {row[id_column]: order_title(row) + " · " + str(row.get("NOMBRE PACIENTE", ""))
                  for _, row in rows.iterrows()}
        # Sólo los nombres repetidos requieren un dato adicional para distinguirlos.
        repeated = {label for label in labels.values() if list(labels.values()).count(label) > 1}
        for identifier, label in list(labels.items()):
            if label in repeated:
                labels[identifier] = f"{label} · Ref. {identifier}"
        chosen = st.selectbox("✏️ Pedido para editar", identifiers, key=key,
                              format_func=labels.get, disabled=bool(pending_count(namespace)))
    else:
        chosen = identifiers[0]
    return rows[rows[id_column] == chosen].iloc[0]


def note_selection(identifiers, namespace, active_id=None):
    """Abre la última casilla marcada sin desmarcar las demás ni perder borradores."""
    key = f"order_detail_selection_{namespace}"
    previous = st.session_state.get(key, [])
    added = [identifier for identifier in identifiers if identifier not in previous]
    st.session_state[key] = list(identifiers)
    if added and not pending_count(namespace):
        st.session_state[f"order_detail_choice_{namespace}"] = active_id if active_id in added else added[-1]


def common_stages(rows, options_for, status_column, normalize):
    """Intersección de transiciones, nunca la unión de permisos de las filas."""
    if rows.empty:
        return []
    options = [list(options_for(row)) for _, row in rows.iterrows()]
    shared = set.intersection(*({normalize(value) for value in values} for values in options))
    return list(dict.fromkeys(value for value in options[0]
                if normalize(value) in shared
                and any(normalize(row[status_column]) != normalize(value) for _, row in rows.iterrows())))


def restore_catalog_values(changes, catalog, multiple=()):
    """Preserva el valor nativo del chip, incluidos sus espacios significativos."""
    for _, delta in changes:
        for column, value in list(delta.items()):
            if column in multiple:
                delta[column] = join_values(split_values(value, (catalog or {}).get(column, [])))
                continue
            matches = [option for option in (catalog or {}).get(column, [])
                       if str(option).strip() == str(value).strip()]
            if matches:
                delta[column] = value if value in matches else matches[0]


def lock_grid(options, locked):
    if locked:
        for column in options["columnDefs"]:
            if column["field"] != "SELECCIONAR":
                column["editable"] = False


def _remember(namespace, identifier, column, key, equivalent, convert):
    draft = st.session_state[draft_key(namespace)][identifier]
    value = convert(st.session_state[key])
    if equivalent(column, draft["baseline"].get(column, ""), value):
        draft["changes"].pop(column, None)
    else:
        draft["changes"][column] = value


def _remember_datetime(namespace, identifier, column, key, equivalent, format_datetime):
    day, clock = st.session_state.get(key + "_date"), st.session_state.get(key + "_time")
    value = format_datetime(datetime.combine(day, clock)) if day else ""
    draft = st.session_state[draft_key(namespace)][identifier]
    if equivalent(column, draft["baseline"].get(column, ""), value):
        draft["changes"].pop(column, None)
    else:
        draft["changes"][column] = value


def _now(namespace, identifier, column, key, equivalent, format_datetime, now):
    value = now().replace(second=0, microsecond=0)
    st.session_state[key + "_date"] = value.date()
    st.session_state[key + "_time"] = value.time()
    _remember_datetime(namespace, identifier, column, key, equivalent, format_datetime)


def render_editor(row, *, namespace, id_column, columns, catalog, labels,
                  option_label, palettes, equivalent, parse_date, format_date,
                  datetime_columns=(), parse_datetime=None, format_datetime=None,
                  now=datetime.now, blocked=False, multiple=()):
    """Devuelve baseline/delta sólo al pulsar Guardar; cada campo conserva su nombre real."""
    identifier = str(row[id_column])
    drafts = st.session_state.setdefault(draft_key(namespace), {})
    draft = drafts.setdefault(identifier, {"baseline": row.to_dict(), "changes": {}})
    if not draft["changes"]:
        draft["baseline"] = row.to_dict()
    version = st.session_state.get(f"order_detail_version_{namespace}", 0)
    baseline_values = json.dumps({column: draft["baseline"].get(column, "") for column in columns},
                                 sort_keys=True, default=str, ensure_ascii=False)
    token = hashlib.sha256(f"{namespace}:{identifier}:{version}:{baseline_values}".encode()).hexdigest()[:16]
    groups = {}
    for column in columns:
        group = ("💳 Pagos" if any(word in column for word in ("PAGO", "ADEUDO"))
                 else "📅 Fechas y entrega" if "FECHA" in column
                 else "📝 Notas" if "COMENTARIO" in column
                 else "📋 Servicio y archivos" if column in catalog
                 else "📦 Otros datos")
        groups.setdefault(group, []).append(column)
    st.markdown("#### ✏️ Editar este pedido")
    st.caption("Completa los campos y pulsa Guardar este pedido. Los cambios se aplican únicamente a esta ficha.")
    for group, fields in groups.items():
        st.markdown(f"**{group}**")
        for start in range(0, len(fields), 3):
            for container, column in zip(st.columns(3), fields[start:start + 3]):
                with container:
                    value = draft["changes"].get(column, draft["baseline"].get(column, ""))
                    text = "" if value is None else str(value)
                    key = f"order_field_{token}_{column}"
                    label = labels.get(column, column.replace("_", " · ").title())
                    args = (namespace, identifier, column, key, equivalent, lambda value: value or "")
                    common = dict(key=key, disabled=blocked, on_change=_remember, args=args)
                    if column in catalog and catalog[column]:
                        if column in multiple:
                            values = split_values(text, catalog[column])
                            choices = list(dict.fromkeys([*catalog[column], *values]))
                            common["args"] = (namespace, identifier, column, key, equivalent, join_values)
                            st.multiselect(label, choices, default=values,
                                format_func=lambda value, col=column: option_label(col, value),
                                placeholder="Elige una o varias opciones", **common)
                        else:
                            choices = list(dict.fromkeys(["", *catalog[column], *([text] if text else [])]))
                            st.selectbox(label, choices, index=choices.index(text),
                                         format_func=lambda value, col=column: option_label(col, value) if value else "— Sin dato —",
                                         **common)
                            values = [text] if text else []
                        for selected_value in values:
                            colors = option_colors(column, selected_value, palettes)
                            st.markdown(f'<span style="display:inline-block;border-radius:8px;padding:3px 9px;'
                                        f'background:{colors[0]};color:{colors[1]};font-size:.8rem">'
                                        f'{html.escape(option_label(column, selected_value))}</span>', unsafe_allow_html=True)
                    elif column in datetime_columns and (not text or parse_datetime(text) is not None):
                        parsed = parse_datetime(text) if text else None
                        dt_args = (namespace, identifier, column, key, equivalent, format_datetime)
                        st.date_input(label, value=parsed.date() if parsed else None, key=key + "_date",
                                      format="DD/MM/YYYY", disabled=blocked, on_change=_remember_datetime, args=dt_args)
                        st.time_input("🕒 Hora", value=(parsed or now()).time().replace(second=0, microsecond=0),
                                      key=key + "_time", disabled=blocked, on_change=_remember_datetime, args=dt_args)
                        st.button("Ahora", key=key + "_now", disabled=blocked,
                                  on_click=_now, args=(*dt_args, now))
                    elif "FECHA" in column and column not in datetime_columns and (not text or parse_date(text) is not None):
                        common["args"] = (namespace, identifier, column, key, equivalent,
                                          lambda value: format_date(value) if value else "")
                        st.date_input(label, value=parse_date(text) if text else None, format="DD/MM/YYYY", **common)
                    elif "COMENTARIO" in column:
                        st.text_area(label, value=text, height=100, **common)
                    else:
                        st.text_input(label, value=text, **common)
                        if "FECHA" in column and text:
                            st.caption("Se conserva el texto de esta fecha; puedes corregirlo aquí.")
    save, discard, count = st.columns([1.4, 1.4, 2])
    changed = bool(draft["changes"])
    submitted = save.button("💾 Guardar este pedido", key=f"detail_save_{token}", type="primary",
                            disabled=blocked or not changed, use_container_width=True)
    discard.button("↩️ Descartar esta edición", key=f"detail_discard_{token}", disabled=not changed,
                   use_container_width=True, on_click=clear_drafts, args=(namespace,))
    count.caption(f"{len(draft['changes'])} campos con cambios" if changed else "Sin cambios pendientes")
    return (draft["baseline"], dict(draft["changes"])) if submitted else None
