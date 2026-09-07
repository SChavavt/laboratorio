"""Editor de celdas con opciones por pedido; las escrituras se validan en lab_pg."""
from functools import lru_cache
from pathlib import Path

import pandas as pd
from st_aggrid import AgGrid, JsCode


@lru_cache(maxsize=1)
def date_editor():
    return JsCode((Path(__file__).parent / "ui" / "workbench_date_editor.js").read_text())


@lru_cache(maxsize=1)
def status_editor():
    return JsCode((Path(__file__).parent / "ui" / "workbench_status_editor.js").read_text())


COLLECT_ROWS = JsCode("""function(params) {
    const rows = [];
    params.eventData.api.forEachNode(node => rows.push(node.data));
    const columnOrder = params.eventData.api.getColumnState()
        .map(column => column.colId).filter(Boolean);
    return {rows: rows, columnOrder: columnOrder};
}""")

GRID_CSS = {
    ".ag-root-wrapper": {"border": "2px solid #B199D4", "border-radius": "12px"},
    ".ag-header": {"background": "#594080", "color": "#FFFFFF"},
    ".ag-header-cell": {"border-right": "1px solid #FFFFFF26"},
    ".lab-header-fixed": {"background": "#493064 !important"},
    ".lab-header-editable": {"background": "#7443AA !important"},
    ".lab-header-readonly": {"background": "#52667D !important"},
    ".lab-header-automatic": {"background": "#147C84 !important"},
    ".lab-header-auto-editable": {"background": "linear-gradient(100deg,#147C84 0 48%,#7443AA 52% 100%) !important"},
    ".ag-cell": {"border-right": "1px solid #DCD3E9"},
    ".ag-row-odd": {"background": "#F7F3FC"},
    ".ag-row-hover": {"background": "#EDE2FF !important"},
    ".lab-cell-editable": {"cursor": "pointer", "box-shadow": "inset 0 0 0 1px #8C62D255"},
    ".lab-cell-editable:after": {"content": "' ✎'", "opacity": "0.72", "font-size": "10px"},
    ".lab-cell-readonly": {"color": "#4A5568"},
    ".lab-cell-automatic": {"box-shadow": "inset 3px 0 0 #238D94"},
    ".lab-status-editor": {
        "background": "#F7F1FF", "padding": "12px", "border": "1px solid #9E7CC7",
        "border-radius": "12px", "box-shadow": "0 8px 24px #39236530", "width": "340px",
        "max-height": "360px", "overflow-y": "auto", "display": "grid", "gap": "8px",
        "font": "14px sans-serif", "box-sizing": "border-box",
    },
    ".lab-status-editor strong": {"color": "#392365", "padding": "3px 4px"},
    ".lab-status-editor button": {
        "border": "1px solid", "border-radius": "8px", "padding": "10px 12px",
        "text-align": "left", "font": "inherit", "font-weight": "650", "cursor": "pointer",
    },
    ".lab-status-editor button:focus": {"outline": "3px solid #4C288355", "outline-offset": "1px"},
    ".lab-date-editor": {
        "background": "#F7F1FF", "color": "#332444", "padding": "16px",
        "border": "1px solid #9E7CC7", "border-radius": "12px", "width": "320px",
        "box-shadow": "0 8px 24px #39236530", "font": "14px sans-serif",
        "display": "grid", "gap": "12px", "box-sizing": "border-box",
    },
    ".lab-date-editor label": {"display": "grid", "gap": "5px"},
    ".lab-date-editor input": {
        "padding": "9px", "border": "1px solid #B49BCD", "border-radius": "6px",
        "background": "white", "color": "#332444", "font": "inherit", "width": "100%",
        "box-sizing": "border-box",
    },
    ".lab-date-editor small": {"color": "#5C4B70", "line-height": "1.4", "overflow-wrap": "anywhere"},
    ".lab-date-actions": {"display": "flex", "gap": "6px", "flex-wrap": "wrap"},
    ".lab-date-actions button": {
        "background": "#E9DDF7", "color": "#47216E", "border": "1px solid #B49BCD",
        "border-radius": "6px", "padding": "8px", "cursor": "pointer", "font": "inherit",
    },
    ".lab-date-actions .lab-date-primary": {"background": "#68419D", "color": "white"},
}


def build_grid_options(grid, *, editable, automatic, stage_options, select_options, date_values,
                       datetime_columns, palettes, time_zone, preferred_order=None, hidden_columns=None):
    """Mantiene opciones ligadas a la fotografía guardada, incluso tras editar STATUS."""
    labels = {"SELECCIONAR": "Abrir", "Columna 1": "Folio", "SEMÁFORO": "Semáforo",
              "APARATO": "Aparato", "STATUS": "🎨 Etapa / status", "NOMBRE DOCTOR": "Doctor",
              "NOMBRE PACIENTE": "Paciente", "DETALLE SEMÁFORO": "Motivo del semáforo"}
    widths = {"SELECCIONAR": 65, "Columna 1": 90, "SEMÁFORO": 160, "APARATO": 140,
              "STATUS": 300, "NOMBRE DOCTOR": 200, "NOMBRE PACIENTE": 200, "DETALLE SEMÁFORO": 380}
    default_order = ["SELECCIONAR", "Columna 1", "SEMÁFORO", "APARATO", "STATUS", "NOMBRE DOCTOR",
             "NOMBRE PACIENTE", "DETALLE COMENTARIOS", "RESPONSABLE", "HORAS EN ETAPA",
             "PLAZO HORAS", "LÍMITE ETAPA", "DETALLE SEMÁFORO"]
    fixed = [column for column in ["SELECCIONAR", "Columna 1", "SEMÁFORO"] if column in grid]
    available = [column for column in grid if column not in fixed]
    preferred = [column for column in (preferred_order or []) if column in available]
    fallback = [column for column in default_order if column in available and column not in preferred]
    order = fixed + preferred + fallback + [column for column in available if column not in {*preferred, *fallback}]
    hidden_columns = set(hidden_columns or [])
    columns = []
    for column in order:
        can_edit = column in editable
        config = {"field": column, "headerName": labels.get(column, column.title()),
                  "width": widths.get(column, 200), "editable": can_edit, "hide": column in hidden_columns}
        if column in {"SELECCIONAR", "Columna 1", "SEMÁFORO"}:
            config.update(pinned="left", lockPinned=True, lockPosition=True, suppressMovable=True,
                          headerClass="lab-header-fixed")
        elif column in automatic and can_edit:
            config["headerClass"] = "lab-header-auto-editable"
        elif column in automatic:
            config["headerClass"] = "lab-header-automatic"
        elif can_edit:
            config["headerClass"] = "lab-header-editable"
        else:
            config["headerClass"] = "lab-header-readonly"
        if column == "SELECCIONAR":
            config.update(editable=True, cellDataType="boolean", sortable=False,
                          cellRenderer="agCheckboxCellRenderer", cellEditor="agCheckboxCellEditor")
        elif column == "STATUS":
            config.update(
                cellEditor=status_editor(), cellEditorPopup=True,
                editable=JsCode("""function(p) {
                    return (p.context.stageOptions[p.data['Columna 1']] || []).length > 1;
                }""") if can_edit else False,
            )
        elif can_edit and column in select_options:
            config.update(cellEditor="agSelectCellEditor", cellEditorParams={"values": select_options[column]})
        elif can_edit and column in date_values:
            config.update(cellEditor=date_editor(), cellEditorPopup=True,
                          cellEditorParams={"withTime": column in datetime_columns,
                                            "initialValues": date_values[column], "timeZone": time_zone})
        if can_edit and column == "STATUS":
            config["cellClass"] = JsCode("""function(p) {
                return (p.context.stageOptions[p.data['Columna 1']] || []).length > 1 ?
                    'lab-cell-editable' : 'lab-cell-readonly';
            }""")
        elif can_edit:
            config["cellClass"] = "lab-cell-editable lab-cell-automatic" if column in automatic else "lab-cell-editable"
        elif column in automatic:
            config["cellClass"] = "lab-cell-automatic"
        elif column not in fixed:
            config["cellClass"] = "lab-cell-readonly"
        if column in palettes:
            config["cellStyle"] = JsCode("""function(p) {
                const colors = (p.context.palettes[p.colDef.field] || {})[p.value];
                return colors ? {backgroundColor: colors[0], color: colors[1], fontWeight: '600'} :
                    {backgroundColor: '', color: '', fontWeight: 'normal'};
            }""")
        if column in {"HORAS EN ETAPA", "PLAZO HORAS"}:
            config.update(width=145, valueFormatter=JsCode("""function(p) {
                return p.value == null ? '—' : Number(p.value).toFixed(2) + ' h';
            }"""))
        columns.append(config)
    return {
        "columnDefs": columns, "context": {"stageOptions": stage_options, "palettes": palettes},
        "defaultColDef": {"resizable": True, "sortable": True, "filter": False, "cellDataType": False},
        "rowHeight": 36, "headerHeight": 42, "animateRows": False,
        "singleClickEdit": True, "stopEditingWhenCellsLoseFocus": True,
        "suppressScrollOnNewData": True, "suppressFieldDotNotation": True,
        "localeText": {"noRowsToShow": "No hay pedidos", "loadingOoo": "Cargando…",
                       "ariaRowSelect": "Abrir pedido", "ariaRowDeselect": "Cerrar pedido"},
    }


def render_grid(grid: pd.DataFrame, options: dict, key: str) -> tuple[pd.DataFrame, list[str]]:
    response = AgGrid(
        grid.copy(), gridOptions=options, key=key, height=min(680, max(270, 36 * (len(grid) + 1) + 24)),
        data_return_mode="CUSTOM", custom_jscode_for_grid_return=COLLECT_ROWS,
        update_on=["cellValueChanged", ("columnMoved", 350)], allow_unsafe_jscode=True,
        enable_enterprise_modules=False, server_sync_strategy="client_wins",
        theme="streamlit", custom_css=GRID_CSS, show_toolbar=False,
    )
    rows = response.get("rows")
    # El componente añade su identificador técnico; nunca pasa al guardado en Sheets.
    edited = pd.DataFrame(rows).reindex(columns=grid.columns) if rows is not None else grid.copy()
    order = response.get("columnOrder") or [column["field"] for column in options["columnDefs"]]
    return edited, order
