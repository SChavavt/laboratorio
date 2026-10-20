"""Date boundaries, safe local artwork and session controls, without backend IO."""
from __future__ import annotations

import ast
import base64
from datetime import date, datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from xml.etree import ElementTree

import pytest

import seasonal_theme
from seasonal_theme import halloween_html, resolve_theme


class Elements(HTMLParser):
    def __init__(self, html: str):
        super().__init__()
        self.elements: list[tuple[str, dict[str, str | None]]] = []
        self.text: list[str] = []
        self.feed(html)

    def handle_starttag(self, tag, attrs):
        self.elements.append((tag, dict(attrs)))

    def handle_data(self, data):
        self.text.append(data)

    def matching(self, *, tag=None, css_class=None, element_id=None):
        return [attrs for name, attrs in self.elements
                if (tag is None or name == tag)
                and (css_class is None or css_class in (attrs.get("class") or "").split())
                and (element_id is None or attrs.get("id") == element_id)]


@pytest.mark.parametrize(("day", "expected"), [
    (date(2026, 9, 30), "off"),
    (date(2026, 10, 1), "halloween"),
    (date(2026, 10, 31), "halloween"),
    (date(2026, 11, 1), "off"),
    (date(2027, 10, 15), "halloween"),
])
def test_automatic_theme_is_limited_to_october_each_year(day, expected):
    assert resolve_theme("auto", day) == expected


@pytest.mark.parametrize(("instant", "expected"), [
    (datetime(2026, 10, 1, 5, 59, tzinfo=timezone.utc), "off"),
    (datetime(2026, 10, 1, 6, 0, tzinfo=timezone.utc), "halloween"),
    (datetime(2026, 11, 1, 5, 59, tzinfo=timezone.utc), "halloween"),
    (datetime(2026, 11, 1, 6, 0, tzinfo=timezone.utc), "off"),
    (datetime(2026, 10, 1, 0, 0), "halloween"),
])
def test_automatic_theme_uses_mexico_date_and_treats_naive_time_as_local(instant, expected):
    assert resolve_theme("auto", instant) == expected


def test_explicit_modes_override_the_calendar():
    assert resolve_theme("halloween", date(2026, 7, 15)) == "halloween"
    assert resolve_theme("off", date(2026, 10, 15)) == "off"


def test_brand_and_unknown_app_labels_cannot_inject_markup():
    brand = '\"><img src=x onerror="alert(1)">'
    app = '<script>alert("app")</script>'
    parsed = Elements(halloween_html(app, brand_label=brand))
    assert not parsed.matching(tag="script")
    assert all(image.get("alt") == "" for image in parsed.matching(tag="img"))
    assert brand in "".join(parsed.text)
    assert app in "".join(parsed.text)
    assert not any(key.startswith("on") for _, attrs in parsed.elements for key in attrs)


def test_external_logo_url_is_ignored_and_text_brand_remains_available():
    parsed = Elements(halloween_html("app_v", brand_label="TD", logo_data_uri="https://example.com/logo.png"))
    assert not [image for image in parsed.matching(tag="img") if image.get("alt") == "TD"]
    assert "TD" in "".join(parsed.text)


def test_motion_can_be_disabled_without_removing_the_seasonal_artwork():
    moving = Elements(halloween_html("app_v", animations=True))
    still = Elements(halloween_html("app_v", animations=False))
    assert moving.matching(css_class="td-seasonal-animated")
    assert not still.matching(css_class="td-seasonal-animated")
    assert still.matching(css_class="td-seasonal-pumpkin")
    assert still.matching(css_class="td-seasonal-ghost")


def test_compact_kiosk_banner_is_static_and_omits_large_brand_and_caption():
    parsed = Elements(halloween_html("app_i", compact=True, animations=True))
    assert parsed.matching(css_class="td-seasonal-compact")
    assert not parsed.matching(css_class="td-seasonal-animated")
    assert not parsed.matching(css_class="td-seasonal-brand")
    assert not parsed.matching(css_class="td-seasonal-caption")
    assert parsed.matching(css_class="td-seasonal-pumpkin")


def test_decorations_are_hidden_from_assistive_technology_and_artwork_is_local():
    parsed = Elements(halloween_html("app_v"))
    assert parsed.matching(element_id="td-seasonal-halloween")[0]["aria-label"]
    artwork = [image for image in parsed.matching(tag="img") if image.get("alt") == ""]
    assert artwork
    for image in artwork:
        assert image.get("aria-hidden") == "true"
        assert image["src"].startswith("data:image/svg+xml;base64,")
        svg = ElementTree.fromstring(base64.b64decode(image["src"].split(",", 1)[1], validate=True))
        assert svg.tag == "{http://www.w3.org/2000/svg}svg"
        assert svg.get("viewBox")
        assert len(svg) > 0
        assert not any(node.tag.endswith("script") for node in svg.iter())
        assert not any(value.startswith(("http:", "https:"))
                       for node in svg.iter() for value in node.attrib.values())
    assert parsed.matching(css_class="td-seasonal-art")[0]["aria-hidden"] == "true"
    assert not any(attrs.get("src", "").startswith(("https:", "http:"))
                   for _, attrs in parsed.elements)


def make_app(*, compact=False):
    from streamlit.testing.v1 import AppTest

    return AppTest.from_string(f'''
import streamlit as st
from seasonal_theme import render_seasonal_theme
st.set_page_config(layout="wide")
st.session_state.setdefault("app_v_seasonal_theme", "halloween")
render_seasonal_theme("app_v", compact={compact!r})
st.button("Continuar")
''').run()


def html_body(app):
    return "".join(element.proto.body for element in app.get("html"))


def test_sidebar_controls_persist_across_reruns_and_can_restore_normal_appearance():
    app = make_app()
    assert not app.exception
    assert app.selectbox[0].options == ["Automático", "Halloween", "Habitual"]
    assert app.checkbox[0].value is True
    assert len(Elements(html_body(app)).matching(element_id="td-seasonal-halloween")) == 1

    app.checkbox[0].uncheck().run()
    assert not app.exception
    assert not Elements(html_body(app)).matching(css_class="td-seasonal-animated")
    app.button[0].click().run()
    assert app.checkbox[0].value is False
    assert len(Elements(html_body(app)).matching(element_id="td-seasonal-halloween")) == 1

    app.selectbox[0].select("off").run()
    assert not app.exception
    assert not app.get("html")
    assert app.button[0].label == "Continuar"
    app.selectbox[0].select("halloween").run()
    assert not app.exception
    assert app.checkbox[0].value is False
    assert len(Elements(html_body(app)).matching(element_id="td-seasonal-halloween")) == 1


def test_compact_render_does_not_add_sidebar_controls():
    app = make_app(compact=True)
    assert not app.exception
    assert not app.selectbox
    assert not app.checkbox
    assert Elements(html_body(app)).matching(css_class="td-seasonal-compact")


def test_local_logo_is_embedded_without_network_access(tmp_path, monkeypatch):
    logo_bytes = base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYII=")
    assets = tmp_path / "assets"
    assets.mkdir()
    (assets / "td_logo.png").write_bytes(logo_bytes)
    monkeypatch.setattr(seasonal_theme, "__file__", str(tmp_path / "seasonal_theme.py"))
    app = make_app()
    assert not app.exception
    images = [image for image in Elements(html_body(app)).matching(tag="img") if image.get("alt") == "TD"]
    assert len(images) == 1
    assert images[0]["alt"] == "TD"
    assert base64.b64decode(images[0]["src"].split(",", 1)[1]) == logo_bytes


def test_missing_local_logo_uses_text_brand_without_blocking_the_app(tmp_path, monkeypatch):
    monkeypatch.setattr(seasonal_theme, "__file__", str(tmp_path / "seasonal_theme.py"))
    app = make_app()
    assert not app.exception
    parsed = Elements(html_body(app))
    assert not [image for image in parsed.matching(tag="img") if image.get("alt") == "TD"]
    assert "TD" in "".join(parsed.text)
    assert app.button[0].label == "Continuar"


ENTRYPOINTS = {
    "app_v.py": "app_v", "app_admin.py": "app_admin",
    "app_a-d.py": "app_a", "app_i-d.py": "app_i", "lab_pg.py": "app_lab",
}
ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize(("filename", "app_id"), [
    item for item in ENTRYPOINTS.items() if (ROOT / item[0]).is_file()
])
def test_each_entrypoint_mounts_theme_once_immediately_after_page_configuration(filename, app_id):
    tree = ast.parse((ROOT / filename).read_text())
    theme_calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)
                   and isinstance(node.func, ast.Name) and node.func.id == "render_seasonal_theme"]
    assert len(theme_calls) == 1
    assert theme_calls[0].args[0].value == app_id
    for parent in ast.walk(tree):
        body = getattr(parent, "body", None)
        if not isinstance(body, list):
            continue
        for index, node in enumerate(body[:-1]):
            if (isinstance(node, ast.Expr) and isinstance(node.value, ast.Call)
                    and isinstance(node.value.func, ast.Attribute)
                    and isinstance(node.value.func.value, ast.Name)
                    and node.value.func.value.id == "st"
                    and node.value.func.attr == "set_page_config"):
                following = body[index + 1]
                assert isinstance(following, ast.Expr)
                assert following.value is theme_calls[0]
                return
    pytest.fail("Theme must mount before login or backend work, immediately after page configuration")
