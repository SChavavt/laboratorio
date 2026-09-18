"""AG Grid especializado para el seguimiento de alineadores."""

from functools import lru_cache
from pathlib import Path

import pandas as pd
from st_aggrid import AgGrid, JsCode

from workbench_grid import COLLECT_ROWS, GRID_CSS, date_editor


@lru_cache(maxsize=1)
def aligners_status_editor() -> JsCode:
    return JsCode(
        (Path(__file__).parent / "ui" / "aligners_status_editor.js").read_text()
    )


BUSINESS_ORDER = [
    "STATUS",
    "TIPO",
    "PRODUCTO",
    "ETAPA SOLICITUD",
    "NOMBRE DOCTOR",
    "NOMBRE PACIENTE",
    "DETALLE COMENTARIOS",
]

DEFAULT_ORDER = [
    *BUSINESS_ORDER,
    "HORAS EN ETAPA",
    "PLAZO HORAS",
    "LÍMITE ETAPA",
    "SIGUIENTE ETAPA",
    "DETALLE SEMÁFORO",
]


LABELS = {
    "SELECCIONAR": "Abrir",
    "No. Orden": "No. Orden",
    "SEMÁFORO": "Semáforo",
    "STATUS": "🎨 Etapa / status",
    "TIPO": "Tipo",
    "PRODUCTO": "Producto",
    "ETAPA SOLICITUD": "Etapa solicitud",
    "NOMBRE DOCTOR": "Doctor",
    "NOMBRE PACIENTE": "Paciente",
    "DETALLE COMENTARIOS": "Comentarios",
    "HORAS EN ETAPA": "Horas en etapa",
    "PLAZO HORAS": "Plazo",
    "LÍMITE ETAPA": "Fecha límite",
    "SIGUIENTE ETAPA": "Siguiente etapa",
    "DETALLE SEMÁFORO": "Motivo del semáforo",
}


WIDTHS = {
    "SELECCIONAR": 58,
    "No. Orden": 82,
    "SEMÁFORO": 112,
    "STATUS": 154,
    "TIPO": 84,
    "PRODUCTO": 105,
    "ETAPA SOLICITUD": 120,
    "NOMBRE DOCTOR": 110,
    "NOMBRE PACIENTE": 110,
    "DETALLE COMENTARIOS": 126,
    "HORAS EN ETAPA": 120,
    "PLAZO HORAS": 100,
    "LÍMITE ETAPA": 145,
    "SIGUIENTE ETAPA": 190,
    "DETALLE SEMÁFORO": 260,
}

MIN_WIDTHS = {
    "SELECCIONAR": 54,
    "No. Orden": 72,
    "SEMÁFORO": 104,
    "STATUS": 138,
    "TIPO": 76,
    "PRODUCTO": 92,
    "ETAPA SOLICITUD": 108,
    "NOMBRE DOCTOR": 96,
    "NOMBRE PACIENTE": 96,
    "DETALLE COMENTARIOS": 108,
}

MAX_WIDTHS = {
    "SELECCIONAR": 62,
    "No. Orden": 96,
    "SEMÁFORO": 126,
    "STATUS": 184,
    "TIPO": 112,
    "PRODUCTO": 142,
    "ETAPA SOLICITUD": 160,
    "NOMBRE DOCTOR": 150,
    "NOMBRE PACIENTE": 150,
    "DETALLE COMENTARIOS": 175,
    "DETALLE SEMÁFORO": 330,
}


def build_aligners_grid_options(
    grid: pd.DataFrame,
    *,
    editable: set[str],
    automatic: set[str],
    stage_options: dict[str, list[str]],
    select_options: dict[str, list[str]],
    date_values: dict[str, dict[str, str]],
    palettes: dict[str, dict[str, tuple[str, str]]],
    time_zone: str,
    hidden_columns: set[str] | None = None,
) -> dict:
    """Crea el editor manteniendo las transiciones ligadas a cada número de orden."""

    hidden = set(hidden_columns or [])
    system_fixed = [
        column
        for column in ["SELECCIONAR", "No. Orden", "SEMÁFORO"]
        if column in grid
    ]
    business_fixed = [column for column in BUSINESS_ORDER if column in grid]
    pinned = system_fixed + business_fixed
    available = [column for column in grid if column not in pinned]
    preferred = [column for column in DEFAULT_ORDER if column in available]
    order = pinned + preferred + [
        column for column in available if column not in preferred
    ]

    columns = []
    for column in order:
        can_edit = column in editable
        config = {
            "field": column,
            "headerName": LABELS.get(column, column.title()),
            "width": WIDTHS.get(column, 150),
            "minWidth": MIN_WIDTHS.get(column, 90),
            "maxWidth": MAX_WIDTHS.get(column, 320),
            "editable": can_edit,
            "hide": column in hidden,
        }
        if column != "SELECCIONAR":
            config["tooltipField"] = column
        if column in pinned:
            config.update(
                pinned="left",
                lockPinned=True,
                lockPosition=True,
                suppressMovable=True,
                suppressAutoSize=True,
            )
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
            config.update(
                editable=True,
                cellDataType="boolean",
                sortable=False,
                cellRenderer="agCheckboxCellRenderer",
                cellEditor="agCheckboxCellEditor",
            )
        elif column == "STATUS":
            config.update(
                cellEditor=aligners_status_editor(),
                cellEditorPopup=True,
                editable=(
                    JsCode(
                        """function(p) {
                            return (p.context.stageOptions[p.data['No. Orden']] || []).length > 1;
                        }"""
                    )
                    if can_edit
                    else False
                ),
            )
        elif can_edit and column in select_options:
            config.update(
                cellEditor="agSelectCellEditor",
                cellEditorParams={"values": select_options[column]},
            )
        elif can_edit and column in date_values:
            config.update(
                cellEditor=date_editor(),
                cellEditorPopup=True,
                cellEditorParams={
                    "withTime": False,
                    "initialValues": date_values[column],
                    "timeZone": time_zone,
                },
            )

        if can_edit and column == "STATUS":
            config["cellClass"] = JsCode(
                """function(p) {
                    return (p.context.stageOptions[p.data['No. Orden']] || []).length > 1
                        ? 'lab-cell-editable' : 'lab-cell-readonly';
                }"""
            )
        elif can_edit:
            config["cellClass"] = (
                "lab-cell-editable lab-cell-automatic"
                if column in automatic
                else "lab-cell-editable"
            )
        elif column in automatic:
            config["cellClass"] = "lab-cell-automatic"
        elif column not in system_fixed:
            config["cellClass"] = "lab-cell-readonly"

        if column in palettes:
            config["cellStyle"] = JsCode(
                """function(p) {
                    const colors = (p.context.palettes[p.colDef.field] || {})[p.value];
                    return colors
                        ? {backgroundColor: colors[0], color: colors[1], fontWeight: '600'}
                        : {backgroundColor: '', color: '', fontWeight: 'normal'};
                }"""
            )
        if column in {"HORAS EN ETAPA", "PLAZO HORAS"}:
            config.update(
                valueFormatter=JsCode(
                    """function(p) {
                        return p.value == null ? '—' : Number(p.value).toFixed(2) + ' h';
                    }"""
                )
            )
        columns.append(config)

    return {
        "columnDefs": columns,
        "context": {
            "idColumn": "No. Orden",
            "stageOptions": stage_options,
            "palettes": palettes,
        },
        "defaultColDef": {
            "resizable": True,
            "sortable": True,
            "filter": False,
            "cellDataType": False,
        },
        "rowHeight": 36,
        "headerHeight": 42,
        "animateRows": False,
        "autoSizeStrategy": {
            "type": "fitCellContents",
            "skipHeader": False,
            "defaultMinWidth": 76,
        },
        "suppressColumnVirtualisation": True,
        "enableBrowserTooltips": True,
        "singleClickEdit": True,
        "stopEditingWhenCellsLoseFocus": True,
        "suppressScrollOnNewData": True,
        "suppressFieldDotNotation": True,
        "localeText": {
            "noRowsToShow": "No hay pedidos",
            "loadingOoo": "Cargando…",
            "ariaRowSelect": "Abrir pedido",
            "ariaRowDeselect": "Cerrar pedido",
        },
    }


def render_aligners_grid(
    grid: pd.DataFrame, options: dict, key: str
) -> pd.DataFrame:
    response = AgGrid(
        grid.copy(),
        gridOptions=options,
        key=key,
        height=min(680, max(270, 36 * (len(grid) + 1) + 24)),
        data_return_mode="CUSTOM",
        custom_jscode_for_grid_return=COLLECT_ROWS,
        update_on=["cellValueChanged"],
        allow_unsafe_jscode=True,
        enable_enterprise_modules=False,
        server_sync_strategy="client_wins",
        theme="streamlit",
        custom_css=GRID_CSS,
        show_toolbar=False,
    )
    rows = response.get("rows")
    if rows is None:
        return grid.copy()
    return pd.DataFrame(rows).reindex(columns=grid.columns)
