"""Tablero de pantalla para alineadores.

Sólo arma HTML y estilos a partir de un modelo de vista ya calculado por
``alineadores_pg.board_view_model``; no lee Sheets ni depende de Streamlit.
Todo texto que viene de las hojas se escapa antes de insertarse.

Colores: el semáforo usa la escala de estado (siempre con emoji + etiqueta) y la
antigüedad una rampa ordinal azul validada sobre la superficie oscura
``#13221c``; los tonos más claros corresponden a los pedidos más antiguos.
"""

from __future__ import annotations

from html import escape
from typing import Any

SIGNAL_ORDER = ("late", "due", "pause", "ok", "none")

# Rampa ordinal (0–2, 3–5, 6–10, 11–20, 21+ días hábiles) y el color de texto
# que conserva contraste sobre cada escalón.
AGE_BAND_COLORS = ("#184f95", "#256abf", "#3987e5", "#6da7ec", "#9ec5f4")
AGE_BAND_INK = ("#FFFFFF", "#FFFFFF", "#0B1511", "#0B1511", "#0B1511")

BOARD_CSS = """
.board {
  --board-bg:#0D1813; --panel:#13221c; --card:#1a2e25; --line:rgba(255,255,255,.09);
  --ink:#F2FBF6; --ink-2:#B9CCC2; --ink-3:#8FA89B; --accent:#3DDC97;
  --late:#d03b3b; --due:#fab219; --pause:#9085e9; --ok:#0ca30c; --none:#8a9aa8;
  color-scheme:dark; box-sizing:border-box; display:grid; gap:.85em;
  padding:1.1em 1.2em 1.2em; border-radius:18px; background:var(--board-bg);
  color:var(--ink); font-family:system-ui,-apple-system,"Segoe UI",sans-serif;
  font-size:14px; line-height:1.3; box-shadow:0 14px 34px #0B151133;
}
.board *, .board *::before, .board *::after {box-sizing:border-box;}
/* Modo pantalla: ocupa toda la ventana y la sección de etapas toma el alto
   sobrante; cada columna muestra las tarjetas que quepan (las más antiguas). */
.board.is-screen {
  position:fixed; inset:0; z-index:999990; width:100vw; height:100vh; overflow:hidden;
  display:flex; flex-direction:column; gap:.7em; padding:.9em 1.1em;
  font-size:clamp(12px,.8vw,26px); border-radius:0; box-shadow:none;
}
.board.is-screen > * {flex:none;}
.board.is-screen > .board-flow {flex:1 1 auto; min-height:0; grid-template-rows:auto minmax(0,1fr);}
.board.is-screen .stage {min-height:0; overflow:hidden;}
.board.is-screen .stage-cards {
  flex:1 1 auto; min-height:0; overflow:hidden;
  -webkit-mask-image:linear-gradient(to bottom,#000 calc(100% - 2.2em),transparent);
  mask-image:linear-gradient(to bottom,#000 calc(100% - 2.2em),transparent);
}
.board.is-screen .board-foot {padding-right:15em;}
.board h2, .board h3, .board p {margin:0; padding:0; color:inherit; letter-spacing:0;}
.board-head {display:flex; justify-content:space-between; align-items:flex-end; gap:1em; flex-wrap:wrap;}
.board-title h2 {font-size:1.75em; font-weight:800; line-height:1.1; color:var(--ink);}
.board-title p {color:var(--ink-2); font-size:.9em; margin-top:.25em;}
.board-live {
  display:inline-flex; align-items:center; gap:.45em; margin-bottom:.35em;
  color:var(--accent); font-size:.72em; font-weight:800; letter-spacing:.16em;
}
.board-live i {width:.6em; height:.6em; border-radius:50%; background:var(--accent);
  box-shadow:0 0 0 0 #3DDC9766; animation:board-pulse 2s ease-out infinite;}
@keyframes board-pulse {0% {box-shadow:0 0 0 0 #3DDC9766;} 100% {box-shadow:0 0 0 .7em #3DDC9700;}}
.board-clock {text-align:right;}
.board-clock strong {display:block; font-size:2.3em; font-weight:750; line-height:1; color:var(--ink);}
.board-clock span {color:var(--ink-2); font-size:.85em;}
.board-stale {
  border:1px solid var(--due); border-radius:10px; padding:.5em .8em;
  background:#fab21914; color:var(--ink); font-size:.9em;
}
.board-kpis {display:grid; grid-template-columns:1fr 2.3fr 1.05fr 1.35fr; gap:.75em;}
.kpi {
  background:var(--panel); border:1px solid var(--line); border-radius:14px;
  padding:.8em 1em; display:flex; flex-direction:column; gap:.3em; min-width:0;
}
.kpi-label {color:var(--ink-2); font-size:.8em; font-weight:700; letter-spacing:.04em; text-transform:uppercase;}
.kpi strong {font-size:2.2em; font-weight:750; line-height:1.05; color:var(--ink);}
.kpi-hero strong {font-size:3.4em;}
.kpi-sub {color:var(--ink-2); font-size:.85em; white-space:nowrap; overflow:hidden; text-overflow:ellipsis;}
.kpi-pair {display:flex; gap:1.4em;}
.kpi-pair span {display:block; color:var(--ink-2); font-size:.85em;}
.stack {display:flex; gap:2px; height:.8em; border-radius:4px; overflow:hidden; background:var(--card); margin:.2em 0;}
.stack i {display:block; min-width:4px;}
.stack.thin {height:.4em; margin:.35em 0 0;}
.sig-bg-late {background:var(--late);} .sig-bg-due {background:var(--due);}
.sig-bg-pause {background:var(--pause);} .sig-bg-ok {background:var(--ok);}
.sig-bg-none {background:var(--none);}
.legend {display:flex; flex-wrap:wrap; gap:.25em 1.1em; list-style:none; margin:0; padding:0;}
.legend li {margin:0; padding:0; color:var(--ink-2); font-size:.88em; white-space:nowrap;}
.legend li strong {font-size:1.25em; margin-left:.2em; color:var(--ink);}
.legend li.is-zero {opacity:.55;}
.board-flow {display:grid; gap:.4em .5em; align-items:stretch; min-width:0; overflow-x:auto; padding-bottom:2px;}
.flow-label {grid-row:1; color:var(--ink-3); font-size:.72em; font-weight:800; letter-spacing:.14em; text-transform:uppercase;}
.flow-sep {grid-row:1 / span 2; justify-self:center; border-left:1px dashed #8a9aa866;}
.stage {
  grid-row:2;
  background:var(--panel); border:1px solid var(--line); border-radius:12px;
  padding:.55em; display:flex; flex-direction:column; gap:.4em; min-width:0;
}
.stage.is-pause {border-style:dashed; border-color:#9085e966;}
.stage-head {border-top:3px solid var(--stage,#8a9aa8); padding-top:.45em; margin-top:-.1em;}
.stage-name {
  display:-webkit-box; -webkit-line-clamp:2; -webkit-box-orient:vertical; overflow:hidden;
  min-height:2.5em; color:var(--ink-2); font-size:.72em; font-weight:800; letter-spacing:.04em;
  text-transform:uppercase; line-height:1.25;
}
.stage-count {display:block; font-size:1.9em; font-weight:750; line-height:1.05; color:var(--ink);}
.stage-count.is-zero {color:var(--ink-3);}
.stage-cards {display:flex; flex-direction:column; gap:.4em;}
.stage-empty {color:var(--ink-3); font-size:.8em; text-align:center; padding:.8em 0;}
.stage-more {color:var(--ink-2); font-size:.8em; text-align:center; padding:.15em 0 0;}
.card {
  background:var(--card); border-radius:9px; border-left:4px solid var(--none);
  padding:.45em .55em .45em .5em; display:grid; gap:.25em; min-width:0;
}
.card.sig-late {border-left-color:var(--late);} .card.sig-due {border-left-color:var(--due);}
.card.sig-pause {border-left-color:var(--pause);} .card.sig-ok {border-left-color:var(--ok);}
.card-top, .card-foot {display:flex; align-items:center; gap:.35em; min-width:0;}
.folio {
  flex:1; min-width:0; color:var(--ink-2); font:600 .76em ui-monospace,"SFMono-Regular",Menlo,Consolas,monospace;
  white-space:nowrap; overflow:hidden; text-overflow:ellipsis;
}
.age {flex:none; border-radius:6px; padding:.05em .35em; font-size:.8em; font-weight:800; white-space:nowrap;}
.age.is-unknown {background:#2a3d35; color:var(--ink-2);}
.patient {
  flex:1; min-width:0; font-size:.9em; font-weight:650; white-space:nowrap; overflow:hidden;
  text-overflow:ellipsis; color:var(--ink);
}
.meter {height:4px; border-radius:4px; background:#2a3d35; overflow:hidden;}
.meter i {display:block; height:100%; border-radius:0 4px 4px 0;}
.card-foot {font-size:.74em; color:var(--ink-2);}
.card-foot .time {flex:1; min-width:0; white-space:nowrap; overflow:hidden; text-overflow:ellipsis;}
.chip {flex:none; border-radius:5px; padding:0 .35em; font-size:.92em; font-weight:800; letter-spacing:.03em;}
.chip-rank {background:var(--ink); color:#0B1511;}
.chip-source {border:1px solid var(--ink-3); color:var(--ink);}
.board-bottom {display:grid; grid-template-columns:minmax(0,2.1fr) minmax(0,1fr); gap:.75em;}
.panel {background:var(--panel); border:1px solid var(--line); border-radius:14px; padding:.8em 1em; min-width:0;}
.panel h3 {font-size:1.05em; font-weight:800; color:var(--ink); margin-bottom:.5em;}
.panel h3 span {color:var(--ink-3); font-weight:600; font-size:.82em; margin-left:.4em;}
.priority table {width:100%; border-collapse:collapse; table-layout:fixed; font-size:.88em;}
.priority th {
  text-align:left; color:var(--ink-3); font-size:.8em; font-weight:800; letter-spacing:.05em;
  text-transform:uppercase; padding:.3em .45em; border-bottom:1px solid var(--line);
}
.priority td {
  padding:.36em .45em; border-bottom:1px solid var(--line); color:var(--ink);
  white-space:nowrap; overflow:hidden; text-overflow:ellipsis;
}
.priority tr:last-child td {border-bottom:0;}
.priority td.muted {color:var(--ink-2);}
.priority .num {text-align:right; font-variant-numeric:tabular-nums;}
.priority td .age {display:inline-block;}
.priority .rank-cell {color:var(--ink-3); font-weight:800;}
.priority .empty {color:var(--ink-3); text-align:center; padding:1.2em 0;}
.age-rows {display:grid; gap:.5em;}
.age-row {display:grid; grid-template-columns:5.2em minmax(0,1fr); align-items:center; gap:.6em;}
.age-row > span {color:var(--ink-2); font-size:.85em; white-space:nowrap; font-variant-numeric:tabular-nums;}
.age-track {display:flex; align-items:center; gap:.45em; min-width:0;}
.age-bar {height:1.15em; max-height:24px; border-radius:0 4px 4px 0; min-width:3px;}
.age-track strong {font-size:.95em; color:var(--ink); font-variant-numeric:tabular-nums;}
.age-note {color:var(--ink-3); font-size:.78em; margin-top:.6em;}
.board-foot {color:var(--ink-3); font-size:.78em; display:flex; flex-wrap:wrap; gap:.3em 1.2em;}
@media (max-width:1100px) {
  .board-kpis {grid-template-columns:1fr 1fr;}
  .board-bottom {grid-template-columns:minmax(0,1fr);}
}
@media (max-width:640px) {
  .board {padding:.9em .8em;}
  .board-kpis {grid-template-columns:minmax(0,1fr);}
  .board-clock {text-align:left;}
  .priority {overflow-x:auto;}
  .priority table {table-layout:auto;}
  .priority col {width:auto !important;}
  .priority .hide-narrow {display:none;}
}
@media (prefers-reduced-motion:reduce) {.board-live i {animation:none;}}
"""


def _text(value: Any) -> str:
    return escape("" if value is None else str(value))


def _age_style(band: int | None) -> tuple[str, str]:
    if band is None:
        return "age is-unknown", ""
    band = max(0, min(band, len(AGE_BAND_COLORS) - 1))
    return "age", f"background:{AGE_BAND_COLORS[band]};color:{AGE_BAND_INK[band]}"


def _age_badge(card: dict[str, Any]) -> str:
    css_class, style = _age_style(card.get("age_band"))
    days = card.get("age_days")
    label = "¿? d" if days is None else f"{days} d"
    title = "Sin fecha de recepción" if days is None else f"{days} días hábiles desde la recepción"
    return f'<span class="{css_class}" style="{style}" title="{title}">{label}</span>'


def _stack(counts: dict[str, int], signals: list[dict[str, Any]], *, thin: bool = False) -> str:
    total = sum(counts.get(key, 0) for key in SIGNAL_ORDER)
    labels = {item["key"]: f'{item["emoji"]} {item["label"]}' for item in signals}
    segments = "".join(
        f'<i class="sig-bg-{key}" style="flex:{counts[key]}" title="{_text(labels.get(key, key))}: {counts[key]}"></i>'
        for key in SIGNAL_ORDER
        if counts.get(key, 0)
    )
    description = ", ".join(
        f"{labels.get(key, key)} {counts[key]}" for key in SIGNAL_ORDER if counts.get(key, 0)
    )
    css_class = "stack thin" if thin else "stack"
    if not total:
        return f'<div class="{css_class}" role="img" aria-label="Sin pedidos"></div>'
    return f'<div class="{css_class}" role="img" aria-label="{_text(description)}">{segments}</div>'


def _card(card: dict[str, Any]) -> str:
    progress = card.get("progress")
    meter = ""
    if progress is not None:
        width = max(0.0, min(float(progress), 1.0)) * 100
        meter = f'<div class="meter"><i class="sig-bg-{card["signal"]}" style="width:{width:.0f}%"></i></div>'
    else:
        meter = '<div class="meter"></div>'
    rank = (
        f'<span class="chip chip-rank" title="Prioridad por antigüedad">#{card["rank"]}</span>'
        if card.get("rank")
        else ""
    )
    source = (
        f'<span class="chip chip-source" title="Hoja {_text(card["source"])}">'
        f'{_text(card["source"])[:3].upper()}</span>'
        if card.get("show_source")
        else ""
    )
    title = " · ".join(
        filter(
            None,
            [
                _text(card.get("folio")),
                _text(card.get("patient")),
                _text(card.get("doctor")),
                _text(card.get("product")),
                _text(card.get("status")),
                _text(card.get("source")),
                _text(card.get("detail")),
            ],
        )
    )
    return (
        f'<div class="card sig-{card["signal"]}" title="{title}">'
        f'<div class="card-top"><span class="patient">{_text(card.get("patient") or "Sin paciente")}</span>'
        f"{_age_badge(card)}</div>"
        f'<div class="card-foot"><span class="folio">{_text(card.get("product_emoji"))} '
        f'{_text(card.get("folio"))}</span>{source}</div>'
        f"{meter}"
        f'<div class="card-foot"><span class="time">{_text(card.get("signal_emoji"))} '
        f'{_text(card.get("stage_time_label"))}</span>{rank}</div>'
        "</div>"
    )


def _stage(stage: dict[str, Any], signals: list[dict[str, Any]], kind: str, column: int) -> str:
    count = stage["count"]
    cards = "".join(_card(card) for card in stage["cards"])
    if not count:
        cards = '<div class="stage-empty">Sin pedidos</div>'
    more = f'<div class="stage-more">+{stage["more"]} más</div>' if stage.get("more") else ""
    zero = " is-zero" if not count else ""
    return (
        f'<article class="stage is-{kind}" style="grid-column:{column}">'
        f'<div class="stage-head" style="--stage:{_text(stage.get("accent", "#8a9aa8"))}">'
        f'<span class="stage-name" title="{_text(stage["name"])}">{_text(stage.get("emoji"))} {_text(stage["name"])}</span>'
        f'<strong class="stage-count{zero}">{count}</strong>'
        f'{_stack(stage["signals"], signals, thin=True)}'
        "</div>"
        f'<div class="stage-cards">{cards}</div>{more}'
        "</article>"
    )


def _flow(model: dict[str, Any]) -> str:
    """Una sola rejilla: todas las etapas miden igual y los grupos no se enciman."""
    template: list[str] = []
    parts: list[str] = []
    column = 1
    groups = [group for group in model["groups"] if group["stages"]]
    for position, group in enumerate(groups):
        if position:
            template.append(".4em")
            parts.append(f'<i class="flow-sep" style="grid-column:{column}"></i>')
            column += 1
        count = len(group["stages"])
        template.append(f"repeat({count},minmax(9.5em,1fr))")
        parts.append(
            f'<div class="flow-label" style="grid-column:{column} / span {count}">'
            f'{_text(group["label"])}</div>'
        )
        parts.extend(
            _stage(stage, model["signals"], group["kind"], column + offset)
            for offset, stage in enumerate(group["stages"])
        )
        column += count
    return (
        f'<section class="board-flow" style="grid-template-columns:{" ".join(template)}" '
        f'aria-label="Pedidos por etapa">{"".join(parts)}</section>'
    )


def _kpis(model: dict[str, Any]) -> str:
    sources = " · ".join(f"{_text(label)} {count}" for label, count in model["sources"])
    counts = {item["key"]: item["count"] for item in model["signals"]}
    legend = "".join(
        f'<li class="{"is-zero" if not item["count"] else ""}">{item["emoji"]} {_text(item["label"])}'
        f'<strong>{item["count"]}</strong></li>'
        for item in model["signals"]
    )
    oldest = model.get("oldest")
    if oldest:
        oldest_value = f'{oldest["days"]} d'
        oldest_sub = f'{_text(oldest["folio"])} · {_text(oldest["patient"])}'
    else:
        oldest_value, oldest_sub = "—", "Sin fechas de recepción"
    week = model["week"]
    return (
        '<section class="board-kpis">'
        '<div class="kpi kpi-hero"><span class="kpi-label">🦷 Pedidos activos</span>'
        f'<strong>{model["total"]}</strong><span class="kpi-sub">{sources}</span></div>'
        '<div class="kpi"><span class="kpi-label">🚦 Semáforo de la etapa actual</span>'
        f'{_stack(counts, model["signals"])}<ul class="legend">{legend}</ul></div>'
        '<div class="kpi"><span class="kpi-label">⏳ Más antiguo</span>'
        f'<strong>{oldest_value}</strong><span class="kpi-sub">{oldest_sub}</span></div>'
        '<div class="kpi"><span class="kpi-label">📅 Esta semana</span><div class="kpi-pair">'
        f'<div><strong>{week["received"]}</strong><span>📥 recibidos</span></div>'
        f'<div><strong>{week["sent"]}</strong><span>🚚 enviados</span></div></div>'
        f'<span class="kpi-sub">Hoy: {week["received_today"]} recibidos · {week["sent_today"]} enviados</span></div>'
        "</section>"
    )


def _priority(model: dict[str, Any]) -> str:
    rows = []
    for card in model["priority"]:
        rows.append(
            "<tr>"
            f'<td class="rank-cell">#{card["rank"]}</td>'
            f'<td class="muted" style="font-family:ui-monospace,Menlo,Consolas,monospace">{_text(card["folio"])}</td>'
            f'<td title="{_text(card["patient"])}">{_text(card["patient"]) or "—"}</td>'
            f'<td class="muted hide-narrow" title="{_text(card["doctor"])}">{_text(card["doctor"]) or "—"}</td>'
            f'<td class="hide-narrow">{_text(card["product_emoji"])} {_text(card["product"])}</td>'
            f'<td class="hide-narrow" title="{_text(card["status"])}">{_text(card["status_emoji"])} {_text(card["status"])}</td>'
            f'<td class="num">{_age_badge(card)}</td>'
            f'<td title="{_text(card["detail"])}">{_text(card["signal_emoji"])} {_text(card["stage_time_label"])}</td>'
            f'<td class="muted hide-narrow">{_text(card["source"])}</td>'
            "</tr>"
        )
    if not rows:
        rows.append('<tr><td class="empty" colspan="9">Sin pedidos activos 🎉</td></tr>')
    count = len(model["priority"])
    subtitle = f"los {count} pedidos activos más antiguos" if count > 1 else ""
    return (
        '<div class="panel priority">'
        f"<h3>🔥 Prioridad por antigüedad<span>{subtitle}</span></h3>"
        "<table><colgroup>"
        '<col style="width:3.2em"><col style="width:9.5em"><col><col class="hide-narrow" style="width:16%">'
        '<col class="hide-narrow" style="width:11%"><col class="hide-narrow" style="width:19%"><col style="width:5.4em">'
        '<col style="width:9.5em"><col class="hide-narrow" style="width:7.5em">'
        "</colgroup><thead><tr>"
        '<th>#</th><th>Folio</th><th>Paciente</th><th class="hide-narrow">Doctor</th>'
        '<th class="hide-narrow">Producto</th><th class="hide-narrow">Etapa</th><th class="num">Antig.</th>'
        '<th>En etapa</th><th class="hide-narrow">Hoja</th>'
        f"</tr></thead><tbody>{''.join(rows)}</tbody></table></div>"
    )


def _ages(model: dict[str, Any]) -> str:
    peak = max((item["count"] for item in model["ages"]), default=0) or 1
    rows = []
    for item in model["ages"]:
        band = item.get("band")
        color = "#8a9aa8" if band is None else AGE_BAND_COLORS[band]
        width = item["count"] / peak * 82
        bar = (
            f'<i class="age-bar" style="width:{width:.1f}%;background:{color}"></i>'
            if item["count"]
            else ""
        )
        rows.append(
            f'<div class="age-row" title="{_text(item["label"])}: {item["count"]} pedidos">'
            f'<span>{_text(item["label"])}</span>'
            f'<div class="age-track">{bar}<strong>{item["count"]}</strong></div></div>'
        )
    return (
        '<div class="panel ages"><h3>⏳ Antigüedad de los activos<span>pedidos por rango</span></h3>'
        f'<div class="age-rows">{"".join(rows)}</div>'
        '<p class="age-note">Días hábiles (lunes a viernes) desde la fecha de recepción; '
        "sin ella se usa el primer registro de la bitácora.</p></div>"
    )


def render_board_html(model: dict[str, Any]) -> str:
    """Devuelve el tablero completo como HTML autocontenido (estilos incluidos)."""

    stale = (
        f'<div class="board-stale">⚠️ {_text(model["stale"])}</div>' if model.get("stale") else ""
    )
    empty = ""
    if not model["total"]:
        empty = '<div class="board-stale" style="border-color:var(--ok)">🎉 No hay pedidos activos en este momento.</div>'
    screen = " is-screen" if model.get("screen") else ""
    return (
        f"<style>{BOARD_CSS}</style>"
        f'<div class="board{screen}">'
        '<header class="board-head"><div class="board-title">'
        '<span class="board-live"><i></i>EN VIVO</span>'
        f'<h2>{_text(model["title"])}</h2><p>{_text(model["subtitle"])}</p></div>'
        f'<div class="board-clock"><strong>{_text(model["clock"])}</strong>'
        f'<span>{_text(model["date_label"])} · actualizado {_text(model["updated"])}</span></div>'
        "</header>"
        f"{stale}{_kpis(model)}{empty}{_flow(model)}"
        f'<section class="board-bottom">{_priority(model)}{_ages(model)}</section>'
        '<footer class="board-foot">'
        "<span>Semáforo: tiempo de la etapa actual contra su plazo en PROCESOS POR PRODUCTO.</span>"
        "<span>Tarjetas ordenadas de la más antigua a la más reciente · #N = lugar en prioridad · POL = hoja Polanco.</span>"
        f'<span>{_text(model.get("footer", ""))}</span>'
        "</footer></div>"
    )
