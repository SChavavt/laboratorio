"""Eventos pequeños y selección explícita para las dos mesas de trabajo."""
from functools import lru_cache
from pathlib import Path

import pandas as pd
from st_aggrid import JsCode


@lru_cache(maxsize=1)
def selection_renderer():
    return JsCode((Path(__file__).parent / "ui" / "order_selection.js").read_text())


COLLECT_ROWS = JsCode("""function(params) {
    const event = params.eventData;
    const api = event.api;
    const idColumn = api.getGridOption('context').idColumn;
    const state = api.__orderEvents || (api.__orderEvents = {edits: {}, sequence: 0});
    if (event.type === 'cellValueChanged' && event.colDef.field !== 'SELECCIONAR') {
        state.edits[String(event.data[idColumn])] = {...event.data};
    }
    const selectedIds = [];
    api.forEachNode(node => {
        if (node.data && node.data.SELECCIONAR === true) selectedIds.push(String(node.data[idColumn]));
    });
    const activeId = event.type === 'cellValueChanged' && event.colDef.field === 'SELECCIONAR'
        && event.newValue === true ? String(event.data[idColumn]) : null;
    return {editedRows: Object.values(state.edits), selectedIds, activeId,
        sequence: ++state.sequence,
        columnOrder: api.getColumnState().map(column => column.colId).filter(Boolean)};
}""")


def response_frame(grid, response, id_column):
    """Reconstruye por ID; marcar una casilla no transfiere toda la tabla."""
    if response.get("rows") is not None:  # Eventos de sesiones anteriores.
        return pd.DataFrame(response["rows"]).reindex(columns=grid.columns)
    edited = grid.copy()
    for row in response.get("editedRows") or []:
        matches = edited[id_column].astype(str).eq(str(row.get(id_column, "")))
        if not matches.any():
            # Conserva anomalías para que la validación de identidad las rechace.
            edited = pd.concat([edited, pd.DataFrame([row])], ignore_index=True)
        else:
            for column in grid.columns.intersection(row):
                edited.loc[matches, column] = row[column]
    if response.get("selectedIds") is not None and "SELECCIONAR" in edited:
        edited["SELECCIONAR"] = edited[id_column].astype(str).isin(response["selectedIds"])
    edited.attrs["activeId"] = response.get("activeId")
    return edited.reindex(columns=grid.columns)


def configure_selection(options, grid, id_column):
    options["context"]["idColumn"] = id_column
    if id_column in grid and not grid[id_column].duplicated().any():
        options["getRowId"] = JsCode("function(p) { return String(p.data[p.context.idColumn]); }")
    options["rowClassRules"] = {"lab-row-open": JsCode("function(p) { return p.data && p.data.SELECCIONAR === true; }")}
    for column in options["columnDefs"]:
        if column["field"] == "SELECCIONAR":
            column.update(editable=False, cellRenderer=selection_renderer(),
                          cellRendererParams={"suppressMouseEventHandling": JsCode("function() { return true; }")})
            column.pop("cellEditor", None)
    return options
