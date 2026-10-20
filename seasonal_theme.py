"""Temas de temporada compartidos por las apps TD, sin recursos de red.

La selección es por sesión. En automático, Halloween está activo únicamente
durante octubre según la fecha de Ciudad de México.
"""
from __future__ import annotations

import base64
from datetime import date, datetime
from html import escape
from pathlib import Path
from zoneinfo import ZoneInfo


MEXICO_TZ = ZoneInfo("America/Mexico_City")
THEME_LABELS = {"auto": "Automático", "halloween": "Halloween", "off": "Habitual"}
APP_LABELS = {
    "app_v": "Ventas",
    "app_admin": "Administración",
    "app_a": "Recepción de pedidos",
    "app_i": "Almacén",
    "app_lab": "Laboratorio",
}


def resolve_theme(mode: str = "auto", today: date | datetime | None = None) -> str:
    """Resuelve el tema, convirtiendo instantes a la fecha local de México."""
    if mode == "off":
        return "off"
    if mode == "halloween":
        return "halloween"
    if today is None:
        today = datetime.now(MEXICO_TZ).date()
    elif isinstance(today, datetime):
        if today.tzinfo is None:
            today = today.replace(tzinfo=MEXICO_TZ)
        today = today.astimezone(MEXICO_TZ).date()
    return "halloween" if today.month == 10 else "off"


_PUMPKIN = """<svg viewBox="0 0 100 100" aria-hidden="true" focusable="false">
<path d="M48 28Q41 10 60 9L63 17Q51 17 55 30" fill="#8ab66b"/>
<ellipse cx="50" cy="58" rx="43" ry="33" fill="#f58b30"/>
<ellipse cx="50" cy="58" rx="29" ry="33" fill="#ffab49"/>
<ellipse cx="50" cy="58" rx="14" ry="33" fill="#ffbc63"/>
<path d="M25 52L40 43L39 58ZM75 52L60 43L61 58ZM44 62L50 54L56 62Z" fill="#301b38"/>
<path d="M27 67Q50 91 73 67L61 72L58 67L49 76L42 69L38 73Z" fill="#301b38"/>
</svg>"""

_GHOST = """<svg viewBox="0 0 80 100" aria-hidden="true" focusable="false">
<path d="M13 90V37C13 0 67 0 67 37V90L57 81L46 92L35 82L24 92Z" fill="#f6eefc"/>
<ellipse cx="30" cy="42" rx="4" ry="6" fill="#382443"/>
<ellipse cx="51" cy="42" rx="4" ry="6" fill="#382443"/>
<ellipse cx="41" cy="60" rx="5" ry="7" fill="#77558c"/>
<ellipse cx="23" cy="56" rx="6" ry="3" fill="#f8b7a5"/>
<ellipse cx="58" cy="56" rx="6" ry="3" fill="#f8b7a5"/>
</svg>"""

_BAT = """<svg viewBox="0 0 120 60" aria-hidden="true" focusable="false">
<path d="M60 23L53 11L50 23C28 6 16 11 3 2Q5 29 18 42Q30 28 42 47L51 39Q60 58 69 39L78 47Q90 28 102 42Q115 29 117 2C104 11 92 6 70 23L67 11Z" fill="currentColor"/>
</svg>"""

_WEB = """<svg viewBox="0 0 140 140" aria-hidden="true" focusable="false">
<g fill="none" stroke="currentColor" stroke-width="1.1">
<path d="M0 0H140M0 0V140M0 0L125 65M0 0L65 125M0 0L103 103"/>
<path d="M0 30Q13 32 14 27Q20 29 22 22Q29 20 27 14Q32 13 30 0M0 60Q24 65 28 54Q39 58 44 44Q58 39 54 28Q65 24 60 0M0 90Q37 97 42 81Q59 87 66 66Q87 59 81 42Q97 37 90 0M0 125Q51 134 59 113Q82 121 92 92Q121 82 113 59Q134 51 125 0"/>
</g></svg>"""


_CSS = """
/* Scope the chrome so operational tables, charts and status colors stay intact. */
.stApp:has(#td-seasonal-halloween) {
  --td-season-accent: #9a3412;
  --td-season-violet: #7140a3;
}
.stApp:has(#td-seasonal-halloween.td-seasonal-dark) {
  --td-season-accent: #ffb86b;
  --td-season-violet: #d0a7ff;
}
/* El color-scheme del navegador ya conoce el tema en el primer render,
   antes de que st.context reciba la preferencia del cliente. */
@supports (color: light-dark(white, black)) {
  .stApp:has(#td-seasonal-halloween.td-seasonal-banner) {
    --td-season-accent: light-dark(#9a3412, #ffb86b);
    --td-season-violet: light-dark(#7140a3, #d0a7ff);
  }
}
.stApp:has(#td-seasonal-halloween) [data-testid="stMain"] {
  background-image: radial-gradient(ellipse at 0 0, #f58b3012, transparent 42%),
                    radial-gradient(ellipse at 100% 0, #9760cf14, transparent 48%);
  background-repeat: no-repeat;
  background-size: 100% 700px;
}
.stApp:has(#td-seasonal-halloween) [data-testid="stSidebar"] {
  border-top: 3px solid #e88939;
  background-image: linear-gradient(165deg, #ed8a3110, #8450ba10) !important;
}
.stApp:has(#td-seasonal-halloween) button[data-testid="stBaseButton-primary"],
.stApp:has(#td-seasonal-halloween) button[data-testid="stBaseButton-primaryFormSubmit"] {
  background: #9a3412 !important;
  color: #fff !important;
  border-color: #9a3412 !important;
  box-shadow: 0 3px 9px #c65d1320;
}
.stApp:has(#td-seasonal-halloween) button[data-testid="stBaseButton-primary"]:hover,
.stApp:has(#td-seasonal-halloween) button[data-testid="stBaseButton-primaryFormSubmit"]:hover {
  background: #7c2d12 !important;
  border-color: #7c2d12 !important;
}
.stApp:has(#td-seasonal-halloween) button[data-testid="stBaseButton-primary"]:disabled,
.stApp:has(#td-seasonal-halloween) button[data-testid="stBaseButton-primaryFormSubmit"]:disabled {
  opacity: .5;
}
.stApp:has(#td-seasonal-halloween) [data-testid="stTabs"] [role="tab"][aria-selected="true"] {
  color: var(--td-season-accent) !important;
}
.stApp:has(#td-seasonal-halloween) [data-baseweb="tab-highlight"],
.stApp:has(#td-seasonal-halloween) [data-testid="stTabs"] [role="tab"] .react-aria-SelectionIndicator {
  background-color: var(--td-season-accent) !important;
}
.stApp:has(#td-seasonal-halloween) button:focus-visible {
  outline: 3px solid var(--td-season-violet) !important;
  outline-offset: 3px;
}
.td-seasonal-banner {
  position: relative;
  isolation: isolate;
  display: flex;
  align-items: center;
  gap: 19px;
  min-height: 122px;
  padding: 22px 30px;
  margin: 0 0 8px;
  overflow: hidden;
  border: 1px solid #d49a6660;
  border-radius: 20px;
  background: linear-gradient(110deg, #261932 0%, #392244 60%, #5b3150 100%);
  color: #fff3e5;
  box-shadow: 0 8px 24px #31153712;
  font-family: inherit;
}
.td-seasonal-banner::after {
  content: "";
  position: absolute;
  inset: auto 0 0;
  height: 3px;
  background: linear-gradient(90deg, #f7a34c, #bf8ce8 65%, #f7a34c);
}
.td-seasonal-brand {
  position: relative;
  z-index: 1;
  display: grid;
  place-items: center;
  flex: 0 0 65px;
  height: 65px;
  border: 1px solid #f1cba3;
  border-radius: 17px;
  background: #fffaf4;
  color: #382044;
  font-size: 15px;
  font-weight: 850;
  letter-spacing: -.5px;
}
.td-seasonal-brand img { width: 53px; height: 49px; object-fit: contain; }
.td-seasonal-hat {
  position: absolute;
  top: -22px;
  left: -7px;
  width: 48px;
  height: 37px;
  transform: rotate(-17deg);
  pointer-events: none;
}
.td-seasonal-hat::before {
  content: "";
  position: absolute;
  left: 7px;
  border-left: 16px solid transparent;
  border-right: 11px solid transparent;
  border-bottom: 31px solid #8553ac;
}
.td-seasonal-hat::after {
  content: "";
  position: absolute;
  bottom: 1px;
  width: 47px;
  height: 8px;
  border-top: 4px solid #f5a145;
  border-radius: 50%;
  background: #8553ac;
}
.td-seasonal-copy { position: relative; z-index: 1; flex: 1; }
.td-seasonal-eyebrow {
  display: block;
  margin-bottom: 6px;
  color: #f9bd80;
  font-size: 10px;
  font-weight: 750;
  letter-spacing: 2px;
  text-transform: uppercase;
}
.td-seasonal-title { display: block; color: #fff3e5; font-size: 25px; font-weight: 750; line-height: 1.18; }
.td-seasonal-caption { display: block; margin-top: 6px; color: #e6d4ec; font-size: 12px; line-height: 1.4; }
.td-seasonal-art { position: relative; flex: 0 0 220px; align-self: stretch; min-height: 76px; pointer-events: none; }
.td-seasonal-art img { width: 100%; height: 100%; display: block; }
.td-seasonal-pumpkin { position: absolute; width: 92px; height: 92px; right: 36px; bottom: -15px; transform: rotate(7deg); }
.td-seasonal-ghost { position: absolute; width: 53px; height: 68px; right: 121px; bottom: 0; transform: rotate(-8deg); }
.td-seasonal-moon { position: absolute; width: 65px; height: 65px; border-radius: 50%; right: 0; top: -9px; background: #f9d9a9; box-shadow: 0 0 35px #efb26230; }
.td-seasonal-bat { position: absolute; color: #b98ad4; width: 36px; height: 20px; right: 76px; top: -7px; transform: rotate(-11deg); }
.td-seasonal-star { position: absolute; color: #f8cd8b; font-size: 16px; top: 6px; right: 186px; }
.td-seasonal-star-small { top: auto; bottom: 7px; right: 7px; font-size: 11px; }
.td-seasonal-web { position: absolute; color: #dfbce9; opacity: .22; width: 105px; height: 105px; right: 0; top: 0; transform: rotate(90deg); pointer-events: none; }
.td-seasonal-web img { width: 100%; height: 100%; }
.td-seasonal-banner.td-seasonal-animated .td-seasonal-ghost { animation: td-seasonal-float 5s ease-in-out 4; }
.td-seasonal-banner.td-seasonal-animated .td-seasonal-bat { animation: td-seasonal-bat 7s ease-in-out 3; }
@keyframes td-seasonal-float { 0%, 100% { transform: translateY(0) rotate(-8deg); } 50% { transform: translateY(-9px) rotate(3deg); } }
@keyframes td-seasonal-bat { 0%, 100% { transform: translate(0,0) rotate(-11deg); } 50% { transform: translate(-10px,5px) rotate(4deg); } }
.td-seasonal-banner.td-seasonal-compact { min-height: 25px; padding: 3px 12px; margin: 0; border-radius: 6px; gap: 10px; }
.td-seasonal-compact .td-seasonal-title { font-size: 13px; line-height: 1.3; }
.td-seasonal-compact .td-seasonal-eyebrow { margin: 0; font-size: 9px; letter-spacing: 1px; }
.td-seasonal-compact .td-seasonal-copy { display: flex; gap: 12px; align-items: center; }
.td-seasonal-compact .td-seasonal-art { min-height: 22px; flex-basis: 30px; }
.td-seasonal-compact .td-seasonal-pumpkin { width: 27px; height: 27px; right: 0; bottom: -2px; transform: none; }
@media (max-width: 640px) {
  .td-seasonal-banner { min-height: 108px; padding: 23px 16px; gap: 13px; border-radius: 15px; }
  .td-seasonal-brand { flex-basis: 49px; height: 49px; border-radius: 13px; font-size: 11px; }
  .td-seasonal-brand img { width: 40px; height: 38px; }
  .td-seasonal-title { font-size: 20px; }
  .td-seasonal-eyebrow { font-size: 9px; letter-spacing: 1.3px; }
  .td-seasonal-caption { font-size: 11px; max-width: 220px; }
  .td-seasonal-art { display: none; }
  .td-seasonal-web { width: 63px; height: 63px; opacity: .16; }
  .td-seasonal-compact .td-seasonal-art { display: block; }
}
@media (prefers-reduced-motion: reduce) {
  .td-seasonal-banner *, .td-seasonal-banner *::before, .td-seasonal-banner *::after { animation: none !important; }
}
@media print {
  .td-seasonal-banner { display: none !important; }
  .stApp:has(#td-seasonal-halloween) [data-testid="stMain"],
  .stApp:has(#td-seasonal-halloween) [data-testid="stSidebar"] { background-image: none !important; }
}
"""


def _illustration(svg: str) -> str:
    # st.html sanitiza etiquetas SVG inline. Un recurso local de imagen mantiene
    # las ilustraciones visibles tanto en Streamlit 1.55 como en 1.63.
    svg = svg.replace('<svg ', '<svg xmlns="http://www.w3.org/2000/svg" ', 1)
    encoded = base64.b64encode(svg.encode("utf-8")).decode("ascii")
    return f'<img src="data:image/svg+xml;base64,{encoded}" alt="" aria-hidden="true">'


def halloween_html(
    app_id: str, *, brand_label: str = "TD", logo_data_uri: str = "",
    dark: bool = False, animations: bool = True, compact: bool = False,
) -> str:
    """Construye decoración local. Ningún dato de pedidos entra a este HTML."""
    classes = "td-seasonal-banner"
    if dark:
        classes += " td-seasonal-dark"
    if animations and not compact:
        classes += " td-seasonal-animated"
    if compact:
        classes += " td-seasonal-compact"
    label = escape(APP_LABELS.get(app_id, app_id))
    brand = escape(brand_label)
    logo = (
        f'<img src="{escape(logo_data_uri, quote=True)}" alt="{brand}">'
        if logo_data_uri.startswith("data:image/png;base64,") else brand
    )
    brand_tile = "" if compact else (
        f'<span class="td-seasonal-brand">{logo}'
        '<span class="td-seasonal-hat" aria-hidden="true"></span></span>'
    )
    caption = "" if compact else '<span class="td-seasonal-caption">Un toque de magia para tu día.</span>'
    art = f'<span class="td-seasonal-pumpkin">{_illustration(_PUMPKIN)}</span>'
    if not compact:
        art = (
            '<span class="td-seasonal-moon"></span>'
            f'<span class="td-seasonal-ghost">{_illustration(_GHOST)}</span>{art}'
            f'<span class="td-seasonal-bat">{_illustration(_BAT.replace("currentColor", "#b98ad4"))}</span>'
            '<span class="td-seasonal-star">✦</span>'
            '<span class="td-seasonal-star td-seasonal-star-small">✦</span>'
        )
    web = "" if compact else (
        '<span class="td-seasonal-web" aria-hidden="true">'
        f'{_illustration(_WEB.replace("currentColor", "#dfbce9"))}</span>'
    )
    return (
        f"<style>{_CSS}</style>"
        f'<div id="td-seasonal-halloween" class="{classes}" role="region" aria-label="Temporada de Halloween">'
        f'{brand_tile}<span class="td-seasonal-copy">'
        f'<span class="td-seasonal-eyebrow">{label} · Octubre</span>'
        f'<span class="td-seasonal-title">Temporada de Halloween</span>{caption}</span>'
        f'<span class="td-seasonal-art" aria-hidden="true">{art}</span>{web}</div>'
    )


def render_seasonal_theme(app_id: str, *, brand_label: str = "TD", compact: bool = False) -> None:
    """Montar una vez después de set_page_config, incluso antes del login.

    No se cachea la fecha ni el HTML; cada rerun reevalúa el modo automático.
    Los kioscos conservan el modo de su sesión en una franja estática compacta.
    """
    import streamlit as st

    mode_key = f"{app_id}_seasonal_theme"
    motion_key = f"{app_id}_seasonal_motion"
    if not compact:
        with st.sidebar.expander("🎨 Apariencia", expanded=False):
            st.selectbox(
                "Tema", options=list(THEME_LABELS),
                format_func=THEME_LABELS.get, key=mode_key,
                help="Automático activa Halloween del 1 al 31 de octubre, con la fecha de México.",
            )
            st.checkbox("Animaciones suaves", value=True, key=motion_key)
    mode = st.session_state.get(mode_key, "auto")
    if resolve_theme(mode) != "halloween":
        return
    logo_file = Path(__file__).resolve().parent / "assets" / "td_logo.png"
    logo_uri = ""
    if not compact and logo_file.is_file():
        try:
            logo_uri = "data:image/png;base64," + base64.b64encode(logo_file.read_bytes()).decode("ascii")
        except OSError:
            pass  # La marca de texto mantiene usable el tema sin el archivo local.
    st.html(halloween_html(
        app_id, brand_label=brand_label, logo_data_uri=logo_uri,
        dark=(st.context.theme.type or st.get_option("theme.base")) == "dark",
        animations=bool(st.session_state.get(motion_key, True)), compact=compact,
    ))
