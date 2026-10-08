"""Ficha compartida de pedidos. Los borradores nunca escriben en Sheets."""

import hashlib
import html
import json
import re
from datetime import datetime

import pandas as pd
import streamlit as st
from dropdown_fields import join_values, option_colors, split_values


HEADER_CSS = """<style>
[class*="st-key-order_head_"] h3 {padding: 0; line-height: 1.25;}
[class*="st-key-order_head_"] p, [class*="st-key-order_head_"] [data-testid="stMarkdownContainer"] {margin: 0;}
.order-chip {display: inline-block; border-radius: 999px; padding: 4px 12px; margin: 2px 6px 2px 0;
    font-size: .8rem; font-weight: 750; white-space: nowrap;}
.order-chip-detail {font-size: .85rem; opacity: .72;}
</style>"""
# Las secciones plegadas se leen como un solo bloque.
SECTIONS_CSS = '[class*="st-key-order_sections_"] {gap: .5rem;}'
MORE_STAGES = "__all_stages__"
MORE_STAGES_LABEL = "Ver todas las etapas…"


def order_title(row) -> str:
    customer = str(row.get("NOMBRE DOCTOR", "") or "").strip()
    patient = str(row.get("NOMBRE PACIENTE", "") or "").strip()
    return "Pedido · " + (customer or patient or "Sin nombre de cliente")


def render_header(namespace, title, chips, detail=""):
    """Título, etapa y semáforo en una sola línea; el resto de datos ya están en los campos."""
    st.html(HEADER_CSS)
    with st.container(horizontal=True, vertical_alignment="center", gap="small", key=f"order_head_{namespace}"):
        st.subheader(title, anchor=False, width="content")
        badges = "".join(f'<span class="order-chip" style="background:{background};color:{foreground}">'
                         f'{html.escape(label)}</span>' for label, (background, foreground) in chips if label)
        if detail:
            badges += f'<span class="order-chip-detail">{html.escape(detail)}</span>'
        st.markdown(badges, unsafe_allow_html=True, width="content")


def grid_width(count):
    """Tres o cuatro columnas, las que dejen menos huecos en la última fila."""
    return min((4, 3), key=lambda width: -count % width)


def widget_class(key):
    """Clase st-key-… que el navegador de Streamlit asigna al contenedor del widget."""
    return "st-key-" + "".join(char if re.fullmatch(r"[a-zA-Z0-9_-]", char)
                               else "-" * (len(char.encode("utf-16-le")) // 2) for char in key.strip())


def value_css(key, column, values, palettes, multiple):
    """Pinta cada opción elegida dentro del propio campo con el color de su chip en Sheets."""
    rules = []
    for index, value in enumerate(values):
        background, foreground = option_colors(column, value, palettes)
        target = (f'.{widget_class(key)} [data-tag-index="{index}"]' if multiple
                  else f'.{widget_class(key)} [role="group"]')
        rules.append(f"{target} {{background: {background} !important; color: {foreground} !important;}}"
                     f"{target} * {{color: {foreground} !important; -webkit-text-fill-color: {foreground};}}")
    return "".join(rules)


def detail_columns(columns, editable, pinned) -> list[str]:
    eligible = [column for column in columns if column in editable
                and not column.startswith(("_", "Columna_"))
                and re.sub(r"_\d+$", "", column).strip().upper() != "TOTAL"]
    return [column for column in pinned if column in eligible] + [column for column in eligible if column not in pinned]


def stage_source(row, namespace, id_column, status_column):
    """El flujo usa el aparato/producto editado y parte de la etapa guardada."""
    draft = st.session_state.get(draft_key(namespace), {}).get(str(row[id_column]), {})
    baseline = draft.get("baseline", row.to_dict()) if draft.get("changes") else row.to_dict()
    candidate = {**baseline, **draft.get("changes", {})}
    candidate[status_column] = baseline.get(status_column, "")
    return pd.Series(candidate)


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


def draft_value(draft, column):
    return draft["changes"].get(column, draft["baseline"].get(column, ""))


def set_draft_value(namespace, identifier, column, value, equivalent):
    """Anota un valor en el borrador; volver al original deja el campo sin cambios."""
    draft = st.session_state[draft_key(namespace)][identifier]
    if equivalent(column, draft["baseline"].get(column, ""), value):
        draft["changes"].pop(column, None)
    else:
        draft["changes"][column] = value


def _remember(namespace, identifier, column, key, equivalent, convert):
    set_draft_value(namespace, identifier, column, convert(st.session_state[key]), equivalent)


def _remember_stage(namespace, identifier, column, key, equivalent):
    draft = st.session_state[draft_key(namespace)][identifier]
    value = st.session_state[key]
    expanded = draft.setdefault("expanded_columns", {})
    if value == MORE_STAGES:
        expanded[column] = not expanded.get(column, False)
        # La opción de navegación nunca pasa al borrador ni sustituye la etapa.
        st.session_state[key] = draft_value(draft, column)
        return
    set_draft_value(namespace, identifier, column, value, equivalent)
    draft.get("manual_targets", {}).pop(column, None)
    expanded.pop(column, None)


def _remember_expanded_stage(namespace, identifier, column, key, equivalent, normal_choices):
    draft = st.session_state[draft_key(namespace)][identifier]
    value = st.session_state[key + "_all"]
    if value is None:
        return
    set_draft_value(namespace, identifier, column, value, equivalent)
    manual = draft.setdefault("manual_targets", {})
    if value not in normal_choices:
        manual[column] = value
    else:
        manual.pop(column, None)
    st.session_state[key] = value


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


def _remember_date_with_auto_time(namespace, identifier, column, key, equivalent, format_datetime, now):
    st.session_state[key + "_time"] = now().time().replace(second=0, microsecond=0)
    _remember_datetime(namespace, identifier, column, key, equivalent, format_datetime)


def render_editor(row, *, namespace, id_column, columns, catalog, labels,
                  option_label, palettes, equivalent, parse_date, format_date,
                  datetime_columns=(), parse_datetime=None, format_datetime=None,
                  now=datetime.now, blocked=False, multiple=(), primary=(),
                  multi_codecs=None, required=(), constrained=(), extra=None, hints=None,
                  automatic_time_columns=(), expanded_catalog=None, section_extras=None):
    """Devuelve baseline/delta sólo al pulsar Guardar; cada campo conserva su nombre real.

    ``extra`` dibuja secciones propias de la vista sobre el mismo borrador, antes
    del botón Guardar, para que un solo guardado incluya todos los campos.
    ``hints`` pone la ayuda de un campo en su ícono (?) en vez de un texto fijo.
    ``expanded_catalog`` ofrece un segundo selector dentro del campo; abrirlo
    sólo navega y nunca añade una opción artificial a los cambios del pedido.
    ``section_extras`` añade acciones propias a una sección y sólo las carga
    cuando se abre, sin mezclarlas con los cambios de los campos.
    """
    identifier = str(row[id_column])
    drafts = st.session_state.setdefault(draft_key(namespace), {})
    draft = drafts.setdefault(identifier, {"baseline": row.to_dict(), "changes": {}})
    if not draft["changes"]:
        draft["baseline"] = row.to_dict()
    version = st.session_state.get(f"order_detail_version_{namespace}", 0)
    baseline_values = json.dumps({column: draft["baseline"].get(column, "") for column in columns},
                                 sort_keys=True, default=str, ensure_ascii=False)
    token = hashlib.sha256(f"{namespace}:{identifier}:{version}:{baseline_values}".encode()).hexdigest()[:16]
    primary_fields = [column for column in primary if column in columns]
    groups = {}
    for column in columns:
        if column in primary:
            continue
        group = ("💳 Pagos" if any(word in column for word in ("PAGO", "ADEUDO"))
                 else "📅 Fechas y entrega" if "FECHA" in column
                 else "📝 Notas" if "COMENTARIO" in column
                 else "📋 Servicio y archivos" if column in catalog
                 else "📦 Otros datos")
        groups.setdefault(group, []).append(column)
    for group in section_extras or {}:
        groups.setdefault(group, [])
    # Los datos principales quedan a la vista; las demás secciones se pliegan juntas.
    sections = [(st.container(), primary_fields, None)] if primary_fields else []
    folded = st.container(key=f"order_sections_{token}") if groups or extra is not None else None
    sections += [(folded.expander(group, expanded=False, key=f"detail_group_{token}_{group}",
                                 on_change="rerun" if group in (section_extras or {}) else "ignore"),
                  fields, (section_extras or {}).get(group))
                 for group, fields in groups.items()]
    styles = []
    for section, fields, section_extra in sections:
        with section:
            regular = [column for column in fields if "COMENTARIO" not in column]
            width = grid_width(len(regular))
            field_containers = [pair for start in range(0, len(regular), width)
                                for pair in zip(st.columns(width), regular[start:start + width])]
            field_containers.extend((st.container(), column) for column in fields if "COMENTARIO" in column)
            for container, column in field_containers:
                with container:
                    value = draft["changes"].get(column, draft["baseline"].get(column, ""))
                    text = "" if value is None else str(value)
                    key = f"order_field_{token}_{column}"
                    label = labels.get(column, column.replace("_", " · ").title())
                    args = (namespace, identifier, column, key, equivalent, lambda value: value or "")
                    common = dict(key=key, disabled=blocked, on_change=_remember, args=args,
                                  help=(hints or {}).get(column))
                    if column in catalog and catalog[column]:
                        if column in multiple:
                            decode, encode = (multi_codecs or {}).get(column, (split_values, join_values))
                            values = decode(text, catalog[column])
                            choices = list(dict.fromkeys([*catalog[column], *values]))
                            common["args"] = (namespace, identifier, column, key, equivalent, encode)
                            shown = st.multiselect(label, choices, default=values,
                                format_func=lambda value, col=column: option_label(col, value),
                                placeholder="Elige una o varias opciones", **common)
                        else:
                            choices = list(dict.fromkeys([*([] if column in required else [""]), *catalog[column]]))
                            normal_choices = choices.copy()
                            all_choices = (expanded_catalog or {}).get(column, [])
                            manual_target = draft.get("manual_targets", {}).get(column)
                            valid_manual = text == manual_target and text in all_choices
                            if text not in choices:
                                if column in constrained and not valid_manual:
                                    # Cambiar el aparato/producto puede invalidar una
                                    # etapa elegida en el borrador. No se ofrece un salto.
                                    draft["changes"].pop(column, None)
                                    draft.get("manual_targets", {}).pop(column, None)
                                    draft.get("expanded_columns", {}).pop(column, None)
                                    text = str(draft["baseline"].get(column, ""))
                                    st.session_state[key] = text
                                    st.caption("Se restableció la etapa; elige una opción del flujo actual.")
                                else:
                                    choices.append(text)
                            if any(value not in normal_choices for value in all_choices):
                                choices.append(MORE_STAGES)
                                common["on_change"] = _remember_stage
                                common["args"] = (namespace, identifier, column, key, equivalent)
                            if column in constrained and len(choices) < 2:
                                common["disabled"] = True
                            shown = [st.selectbox(label, choices, index=choices.index(text),
                                         format_func=lambda value, col=column: (MORE_STAGES_LABEL if value == MORE_STAGES
                                             else option_label(col, value) if value else "— Sin dato —"),
                                         **common)]
                            if all_choices and draft.get("expanded_columns", {}).get(column):
                                st.caption("Todas las etapas del aparato. Elige una y guarda el pedido.")
                                selected = st.selectbox("Etapa del flujo", all_choices,
                                    index=all_choices.index(text) if text in all_choices and manual_target == text else None,
                                    placeholder="Selecciona una etapa", key=key + "_all", disabled=blocked,
                                    format_func=lambda value, col=column: option_label(col, value),
                                    on_change=_remember_expanded_stage,
                                    args=(namespace, identifier, column, key, equivalent, normal_choices))
                                styles.append(value_css(key + "_all", column, [selected] if selected else [], palettes, False))
                        # El color del chip va dentro del campo, sin repetir el valor debajo.
                        styles.append(value_css(key, column, [item for item in shown if item],
                                                palettes, column in multiple))
                    elif column in automatic_time_columns:
                        parsed = parse_datetime(text) if text else None
                        dt_args = (namespace, identifier, column, key, equivalent, format_datetime)
                        st.date_input(label, value=parsed.date() if parsed else None, key=key + "_date",
                                      format="DD/MM/YYYY", disabled=blocked,
                                      on_change=_remember_date_with_auto_time, args=(*dt_args, now),
                                      help="Elige el día. La hora se registra automáticamente al guardar (Ciudad de México).")
                        st.button("Ahora", key=key + "_now", disabled=blocked, use_container_width=True,
                                  on_click=_now, args=(*dt_args, now))
                    elif column in datetime_columns and (not text or parse_datetime(text) is not None):
                        parsed = parse_datetime(text) if text else None
                        dt_args = (namespace, identifier, column, key, equivalent, format_datetime)
                        st.date_input(label, value=parsed.date() if parsed else None, key=key + "_date",
                                      format="DD/MM/YYYY", disabled=blocked, on_change=_remember_datetime,
                                      args=dt_args, help=common["help"])
                        # Hora y Ahora comparten fila bajo la fecha, sin otra etiqueta.
                        clock, instant = st.columns([3, 2], gap="small")
                        clock.time_input("🕒 Hora", value=(parsed or now()).time().replace(second=0, microsecond=0),
                                         key=key + "_time", disabled=blocked, on_change=_remember_datetime,
                                         args=dt_args, label_visibility="collapsed")
                        instant.button("Ahora", key=key + "_now", disabled=blocked, use_container_width=True,
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
            if section_extra is not None and section.open:
                section_extra()
    if extra is not None:
        with folded:
            extra()
    st.html("<style>" + SECTIONS_CSS + "".join(styles) + "</style>")
    save, discard, count = st.columns([1.4, 1.4, 2], vertical_alignment="center")
    changed = bool(draft["changes"])
    submitted = save.button("💾 Guardar este pedido", key=f"detail_save_{token}", type="primary",
                            disabled=blocked or not changed, use_container_width=True)
    discard.button("↩️ Descartar esta edición", key=f"detail_discard_{token}", disabled=not changed,
                   use_container_width=True, on_click=clear_drafts, args=(namespace,))
    count.caption(f"{len(draft['changes'])} campos con cambios" if changed else "Sin cambios pendientes")
    if submitted:
        changes = dict(draft["changes"])
        for column in automatic_time_columns:
            if column in changes and changes[column]:
                parsed = parse_datetime(changes[column])
                changes[column] = format_datetime(datetime.combine(parsed.date(), now().time()))
        return draft["baseline"], changes
    return None
