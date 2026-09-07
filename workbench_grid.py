"""Editor de celdas con opciones por pedido; las escrituras se validan en lab_pg."""
from functools import lru_cache
from pathlib import Path

import pandas as pd
from st_aggrid import AgGrid, JsCode


@lru_cache(maxsize=1)
def date_editor():
    return JsCode((Path(__file__).parent / "ui" / "workbench_date_editor.js").read_text())


COLLECT_ROWS = JsCode("""function(params) {
    const rows = [];
    params.eventData.api.forEachNode(node => rows.push(node.data));
    return {rows: rows};
}""")

GRID_CSS = {
    ".ag-root-wrapper": {"border": "2px solid #B199D4", "border-radius": "12px"},
    ".ag-header": {"background": "#594080", "color": "#FFFFFF"},
    ".ag-header-cell": {"border-right": "1px solid #FFFFFF26"},
    ".ag-cell": {"border-right": "1px solid #DCD3E9"},
    ".ag-row-odd": {"background": "#F7F3FC"},
    ".ag-row-hover": {"background": "#EDE2FF !important"},
    ".lab-editable": {"cursor": "pointer"},
    ".lab-editable:after": {"content": "' ▾'", "opacity": "0.65", "font-size": "10px"},
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


def build_grid_options(grid, *, editable, stage_options, select_options, date_values,
                       datetime_columns, palettes, time_zone):
    """Mantiene opciones ligadas a la fotografía guardada, incluso tras editar STATUS."""
    labels = {"SELECCIONAR": "Abrir", "Columna 1": "Folio", "SEMÁFORO": "Semáforo",
              "APARATO": "Aparato", "STATUS": "🎨 Etapa / status", "NOMBRE DOCTOR": "Doctor",
              "NOMBRE PACIENTE": "Paciente", "DETALLE SEMÁFORO": "Motivo del semáforo"}
    widths = {"SELECCIONAR": 65, "Columna 1": 90, "SEMÁFORO": 160, "APARATO": 140,
              "STATUS": 300, "NOMBRE DOCTOR": 200, "NOMBRE PACIENTE": 200, "DETALLE SEMÁFORO": 380}
    order = ["SELECCIONAR", "Columna 1", "SEMÁFORO", "APARATO", "STATUS", "NOMBRE DOCTOR",
             "NOMBRE PACIENTE", "DETALLE COMENTARIOS", "RESPONSABLE", "HORAS EN ETAPA",
             "PLAZO HORAS", "LÍMITE ETAPA", "DETALLE SEMÁFORO"]
    order = [column for column in order if column in grid] + [column for column in grid if column not in order]
    columns = []
    for column in order:
        can_edit = column in editable
        config = {"field": column, "headerName": labels.get(column, column.title()),
                  "width": widths.get(column, 200), "editable": can_edit}
        if column in {"SELECCIONAR", "Columna 1", "SEMÁFORO"}:
            config.update(pinned="left", lockPinned=True)
        if column == "SELECCIONAR":
            config.update(editable=True, cellDataType="boolean", sortable=False,
                          cellRenderer="agCheckboxCellRenderer", cellEditor="agCheckboxCellEditor")
        elif column == "STATUS":
            config.update(
                cellEditor="agSelectCellEditor",
                cellEditorParams=JsCode("""function(p) {
                    return {values: p.context.stageOptions[p.data['Columna 1']] || [p.value]};
                }"""),
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
                return (p.context.stageOptions[p.data['Columna 1']] || []).length > 1 ? 'lab-editable' : '';
            }""")
        elif can_edit and (column in select_options or column in date_values):
            config["cellClass"] = "lab-editable"
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


def render_grid(grid: pd.DataFrame, options: dict, key: str) -> pd.DataFrame:
    response = AgGrid(
        grid.copy(), gridOptions=options, key=key, height=min(680, max(270, 36 * (len(grid) + 1) + 24)),
        data_return_mode="CUSTOM", custom_jscode_for_grid_return=COLLECT_ROWS,
        update_on=["cellValueChanged"], allow_unsafe_jscode=True,
        enable_enterprise_modules=False, server_sync_strategy="client_wins",
        theme="streamlit", custom_css=GRID_CSS, show_toolbar=False,
    )
    rows = response.get("rows")
    # El componente añade su identificador técnico; nunca pasa al guardado en Sheets.
    return pd.DataFrame(rows).reindex(columns=grid.columns) if rows is not None else grid.copy()
