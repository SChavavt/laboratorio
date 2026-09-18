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


@lru_cache(maxsize=1)
def apparatus_editor():
    return JsCode(
        (Path(__file__).parent / "ui" / "workbench_apparatus_editor.js").read_text()
    )


COLLECT_ROWS = JsCode("""function(params) {
    const rows = [];
    params.eventData.api.forEachNode(node => rows.push(node.data));
    const columnOrder = params.eventData.api.getColumnState()
        .map(column => column.colId).filter(Boolean);
    return {rows: rows, columnOrder: columnOrder};
}""")

GRID_CSS = {
    ".ag-root-wrapper": {"border": "2px solid #B199D4", "border-radius": "12px", "width": "100%", "max-width": "100%"},
    ".ag-root-wrapper-body": {"width": "100%"},
    ".ag-root": {"width": "100%"},
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
    ".lab-apparatus-editor": {
        "background": "#F7F1FF", "padding": "12px", "border": "1px solid #9E7CC7",
        "border-radius": "12px", "box-shadow": "0 8px 24px #39236530", "width": "300px",
        "max-height": "430px", "overflow-y": "auto", "display": "grid", "gap": "8px",
        "font": "14px sans-serif", "box-sizing": "border-box",
    },
    ".lab-apparatus-editor strong": {"color": "#392365", "padding": "3px 4px"},
    ".lab-apparatus-summary": {
        "min-height": "34px", "padding": "8px 10px", "border-radius": "8px",
        "background": "#E9DDF7", "color": "#3D2468", "font-weight": "700",
        "overflow-wrap": "anywhere",
    },
    ".lab-apparatus-option": {
        "display": "flex", "align-items": "center", "gap": "9px", "width": "100%",
        "border": "1px solid #CBBDE3", "border-radius": "8px", "padding": "9px 10px",
        "background": "#FFFFFF", "color": "#35234E", "text-align": "left",
        "font": "inherit", "cursor": "pointer",
    },
    ".lab-apparatus-option[aria-pressed='true']": {
        "background": "#E8F8F1", "border-color": "#2FA77E", "box-shadow": "inset 0 0 0 1px #2FA77E",
    },
    ".lab-apparatus-dot": {"width": "11px", "height": "11px", "border-radius": "50%", "flex": "0 0 11px"},
    ".lab-apparatus-check": {"margin-left": "auto", "font-weight": "900", "color": "#13735A"},
    ".lab-apparatus-actions": {"display": "flex", "gap": "8px", "padding-top": "4px"},
    ".lab-apparatus-actions button": {
        "flex": "1", "border": "1px solid #9E7CC7", "border-radius": "7px", "padding": "9px",
        "font": "inherit", "font-weight": "700", "cursor": "pointer",
    },
    ".lab-apparatus-apply": {"background": "#68419D", "color": "#FFFFFF"},
    ".lab-apparatus-cancel": {"background": "#FFFFFF", "color": "#4C3270"},
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
                       datetime_columns, palettes, time_zone, preferred_order=None, hidden_columns=None,
                       apparatus_options=None, apparatus_stage_options=None):
    """Mantiene opciones ligadas a la fotografía guardada, incluso tras editar STATUS."""
    labels = {"SELECCIONAR": "Abrir", "Columna 1": "Folio", "SEMÁFORO": "Semáforo",
              "APARATO": "Aparato", "STATUS": "🎨 Etapa / status", "NOMBRE DOCTOR": "Doctor",
              "NOMBRE PACIENTE": "Paciente", "DETALLE COMENTARIOS": "Comentarios",
              "DETALLE SEMÁFORO": "Motivo del semáforo",
              "FECHA/HORA ENVÍO STEFANO": "Fecha/hora envío Stefano",
              "FECHA/HORA ENTREGA STEFANO": "Fecha/hora entrega Stefano"}
    widths = {"SELECCIONAR": 58, "Columna 1": 74, "SEMÁFORO": 112, "APARATO": 126,
              "STATUS": 150, "NOMBRE DOCTOR": 118, "NOMBRE PACIENTE": 118,
              "DETALLE COMENTARIOS": 130, "DETALLE SEMÁFORO": 260}
    minimum_widths = {"SELECCIONAR": 54, "Columna 1": 68, "SEMÁFORO": 104, "APARATO": 112,
                      "STATUS": 136, "NOMBRE DOCTOR": 102, "NOMBRE PACIENTE": 102,
                      "DETALLE COMENTARIOS": 112}
    maximum_widths = {"SELECCIONAR": 62, "Columna 1": 90, "SEMÁFORO": 126, "APARATO": 156,
                      "STATUS": 180, "NOMBRE DOCTOR": 150, "NOMBRE PACIENTE": 150,
                      "DETALLE COMENTARIOS": 175, "DETALLE SEMÁFORO": 330}
    business_order = ["APARATO", "STATUS", "NOMBRE DOCTOR", "NOMBRE PACIENTE", "DETALLE COMENTARIOS"]
    default_order = [*business_order, "RESPONSABLE", "HORAS EN ETAPA",
                     "PLAZO HORAS", "LÍMITE ETAPA", "DETALLE SEMÁFORO"]
    system_fixed = [column for column in ["SELECCIONAR", "Columna 1", "SEMÁFORO"] if column in grid]
    business_fixed = [column for column in business_order if column in grid]
    pinned = system_fixed + business_fixed
    available = [column for column in grid if column not in pinned]
    preferred = [column for column in (preferred_order or []) if column in available]
    fallback = [column for column in default_order if column in available and column not in preferred]
    order = pinned + preferred + fallback + [column for column in available if column not in {*preferred, *fallback}]
    hidden_columns = set(hidden_columns or [])
    columns = []
    for column in order:
        can_edit = column in editable
        config = {
            "field": column,
            "headerName": labels.get(column, column.title()),
            "width": widths.get(column, 150),
            "minWidth": minimum_widths.get(column, 90),
            "maxWidth": maximum_widths.get(column, 320),
            "editable": can_edit,
            "hide": column in hidden_columns,
        }
        if column != "SELECCIONAR":
            config["tooltipField"] = column
        if column in pinned:
            config.update(pinned="left", lockPinned=True, lockPosition=True,
                          suppressMovable=True, suppressAutoSize=True)
        if column in system_fixed:
            config["headerClass"] = "lab-header-fixed"
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
                    const identifier = p.data['Columna 1'];
                    const apparatus = p.data['APARATO'] || '';
                    const byApparatus = (p.context.apparatusStageOptions[identifier] || {});
                    const options = byApparatus[apparatus] || p.context.stageOptions[identifier] || [];
                    return options.length > 1;
                }""") if can_edit else False,
            )
        elif column == "APARATO" and can_edit:
            config.update(
                cellEditor=apparatus_editor(),
                cellEditorPopup=True,
            )
        elif can_edit and column in select_options:
            config.update(cellEditor="agSelectCellEditor", cellEditorParams={"values": select_options[column]})
        elif can_edit and column in date_values:
            config.update(cellEditor=date_editor(), cellEditorPopup=True,
                          cellEditorParams={"withTime": column in datetime_columns,
                                            "initialValues": date_values[column], "timeZone": time_zone})
        if can_edit and column == "STATUS":
            config["cellClass"] = JsCode("""function(p) {
                const identifier = p.data['Columna 1'];
                const apparatus = p.data['APARATO'] || '';
                const byApparatus = (p.context.apparatusStageOptions[identifier] || {});
                const options = byApparatus[apparatus] || p.context.stageOptions[identifier] || [];
                return options.length > 1 ?
                    'lab-cell-editable' : 'lab-cell-readonly';
            }""")
        elif can_edit:
            config["cellClass"] = "lab-cell-editable lab-cell-automatic" if column in automatic else "lab-cell-editable"
        elif column in automatic:
            config["cellClass"] = "lab-cell-automatic"
        elif column not in system_fixed:
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
    apparatus_palette = {
        option: list((palettes.get("APARATO") or {}).get(option, ("#E9DDF7", "#392365")))
        for option in (apparatus_options or [])
    }
    return {
        "columnDefs": columns, "context": {"stageOptions": stage_options,
        "apparatusStageOptions": apparatus_stage_options or {},
        "apparatusOptions": apparatus_options or [], "apparatusColors": apparatus_palette,
        "palettes": palettes},
        "defaultColDef": {"resizable": True, "sortable": True, "filter": False, "cellDataType": False},
        "rowHeight": 36, "headerHeight": 42, "animateRows": False,
        "autoSizeStrategy": {"type": "fitCellContents", "skipHeader": False, "defaultMinWidth": 76},
        "suppressColumnVirtualisation": True, "enableBrowserTooltips": True,
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
