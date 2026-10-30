"""Vigilante de conexión: avisa o reconecta cuando la app se queda congelada."""
import json
import re
from pathlib import Path
import sys

import pytest
import streamlit
from streamlit.proto.BackMsg_pb2 import BackMsg
from streamlit.testing.v1 import AppTest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import connection_watchdog as watchdog

APPS = ["lab_pg.py", "alineadores_pg.py"]


def test_heartbeat_bytes_match_streamlit_protobuf():
    # Si Streamlit cambiara el mensaje, el latido dejaría de recibir respuesta.
    expected = list(BackMsg(app_heartbeat=True).SerializeToString())
    assert expected == [0x88, 0x01, 0x01]
    assert "new Uint8Array([0x88, 0x01, 0x01])" in watchdog.watchdog_html()


def test_streamlit_frontend_still_exposes_what_the_watchdog_reads():
    js_dir = Path(streamlit.__file__).parent / "static" / "static" / "js"
    bundle = "".join(path.read_text(encoding="utf-8", errors="ignore") for path in js_dir.glob("index.*.js"))
    for marker in ("data-test-connection-state", "PINGING_SERVER", "DISCONNECTED_FOREVER", "_stcore/stream"):
        assert marker in bundle, marker
    html = watchdog.watchdog_html()
    assert "data-test-connection-state" in html
    assert "/_stcore/stream" in html


def test_html_carries_the_configured_timeouts():
    html = watchdog.watchdog_html()
    assert "__CONFIG__" not in html
    config = json.loads(re.search(r"var TD_CONN_CONFIG = (\{.*?\});", html).group(1))
    assert config == watchdog.WATCHDOG_CONFIG
    assert watchdog.REPLY_GRACE_MS < watchdog.PROBE_TIMEOUT_MS < watchdog.RECONNECT_WAIT_MS
    assert watchdog.OFFLINE_NOTICE_MS < watchdog.OFFLINE_RELOAD_MS


def test_watchdog_installs_once_per_page_and_offers_reload():
    html = watchdog.watchdog_html()
    assert "if (host.__tdConnWatchdog) return;" in html
    assert "if (win.__tdConnWatchdog) return;" in html
    assert "Recargar página" in html


def test_install_renders_a_single_zero_height_component():
    script = (
        f"import sys\nsys.path.insert(0, {str(ROOT)!r})\n"
        "from connection_watchdog import install_connection_watchdog\n"
        "install_connection_watchdog()\n"
    )
    at = AppTest.from_string(script).run()
    assert not at.exception
    frames = at.get("iframe")
    assert len(frames) == 1
    assert frames[0].proto.srcdoc == watchdog.watchdog_html()


@pytest.mark.parametrize("app", APPS)
def test_every_app_installs_the_watchdog_right_after_page_config(app):
    source = (ROOT / app).read_text(encoding="utf-8")
    assert "from connection_watchdog import install_connection_watchdog" in source
    assert source.count("install_connection_watchdog()") == 1
    page_config = source.index("st.set_page_config(")
    install = source.index("install_connection_watchdog()")
    assert page_config < install
    # Nada de la app se dibuja entre la configuración y el vigilante.
    between = source[source.index(")", page_config):install]
    assert "st." not in between.replace("st.set_page_config", "")
