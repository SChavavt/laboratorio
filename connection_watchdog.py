"""Avisa (y repara cuando se puede) si la app perdió la conexión con el servidor.

Cuando una app de Streamlit se queda abierta mucho tiempo (computadora
suspendida, celular con la app en segundo plano, cambio de red, etc.), la
conexión en vivo (websocket) con el servidor puede morir sin que el navegador
se entere. Streamlit sigue creyendo que está conectado: cada clic se manda a
una conexión muerta, no pasa nada y la app parece congelada hasta que se
recarga la página.

``install_connection_watchdog()`` agrega a la página un vigilante que:

1. Nota cuando la app mandó algo al servidor y no llegó respuesta en unos
   segundos, o cuando lleva rato sin saber nada del servidor.
2. Lo confirma con un latido (``app_heartbeat``): el servidor lo contesta al
   instante aunque el script esté ocupado, así que solo se queda sin respuesta
   cuando la conexión de verdad está muerta.
3. Si está muerta, hace que Streamlit se reconecte (lo mismo que hace solo
   cuando sí detecta el corte) y muestra "Reconectando…". Lo que estaba
   escrito en pantalla se conserva y, al volver, avisa "Conexión restablecida".
4. Si no logra reconectar, muestra un aviso fijo con el botón
   "Recargar página".

Se instala una sola vez por página aunque se llame en cada rerun. Este archivo
es igual en app-ventas-td, app_almacen_TD y laboratorio.
"""

from __future__ import annotations

import json

import streamlit.components.v1 as components

# Tiempos del vigilante, en milisegundos.
REPLY_GRACE_MS = 3_000  # un clic sin respuesta → mandar un latido de prueba
PROBE_TIMEOUT_MS = 6_000  # el latido no regresa → la conexión está muerta
IDLE_PROBE_MS = 60_000  # pantalla visible sin noticias del servidor → probar
RECONNECT_WAIT_MS = 15_000  # la reconexión forzada no respondió → pedir recargar
OFFLINE_NOTICE_MS = 4_000  # Streamlit reconectando → mostrar "Reconectando…"
OFFLINE_RELOAD_MS = 20_000  # Streamlit no logra reconectar → pedir recargar

WATCHDOG_CONFIG = {
    "replyGraceMs": REPLY_GRACE_MS,
    "probeTimeoutMs": PROBE_TIMEOUT_MS,
    "idleProbeMs": IDLE_PROBE_MS,
    "reconnectWaitMs": RECONNECT_WAIT_MS,
    "offlineNoticeMs": OFFLINE_NOTICE_MS,
    "offlineReloadMs": OFFLINE_RELOAD_MS,
}

_WATCHDOG_SCRIPT = r"""
<script>
var TD_CONN_CONFIG = __CONFIG__;

(function () {
  var host;
  try {
    host = window.parent;
    if (!host || host === window) return;
    void host.document;
  } catch (e) {
    return;
  }
  // Este iframe mide 0 px: que tampoco deje hueco entre elementos.
  try {
    var frame = window.frameElement;
    var box = frame && frame.closest('[data-testid="stElementContainer"], .element-container');
    if (box) box.style.display = "none";
  } catch (e) {}
  if (host.__tdConnWatchdog) return;
  // Corre dentro de la página de la app (no en este iframe) para seguir vivo
  // aunque Streamlit vuelva a dibujar o quite este componente.
  try {
    var el = host.document.createElement("script");
    el.textContent = "(" + tdConnectionWatchdog.toString() + ")(window, " +
      JSON.stringify(TD_CONN_CONFIG) + ");";
    (host.document.head || host.document.documentElement).appendChild(el);
    el.parentNode.removeChild(el);
  } catch (e) {}
  if (!host.__tdConnWatchdog) {
    try { tdConnectionWatchdog(host, TD_CONN_CONFIG); } catch (e) {}
  }
})();

function tdConnectionWatchdog(win, cfg) {
  if (win.__tdConnWatchdog) return;
  var WS = win.WebSocket;
  if (!WS || !WS.prototype || typeof WS.prototype.send !== "function") return;
  var doc = win.document;
  var api = { version: 1 };
  win.__tdConnWatchdog = api;

  // BackMsg { app_heartbeat: true } en protobuf (campo 17, bool). El servidor
  // contesta con heartbeat_ack sin volver a correr el script.
  var HEARTBEAT = new Uint8Array([0x88, 0x01, 0x01]);
  var TICK_MS = 2000;
  var originalSend = WS.prototype.send;
  var seen = typeof win.WeakSet === "function" ? new win.WeakSet() : null;

  var sock = null;          // websocket de Streamlit visto por última vez
  var lastRx = Date.now();  // último mensaje recibido del servidor
  var waitingSince = 0;     // primer envío que sigue sin respuesta
  var probeAt = 0;          // latido de prueba pendiente
  var reconnectAt = 0;      // momento en que se forzó la reconexión
  var attempts = 0;         // reconexiones forzadas seguidas sin respuesta
  var offlineSince = 0;     // Streamlit reporta que no está conectado
  var probeSoon = false;    // volvió la red o la página: probar en cuanto se pueda
  var lastTick = Date.now();
  var banner = null;
  var bannerKind = "";
  var toastTimer = 0;

  function isStreamlitSocket(ws) {
    try {
      return String(ws.url || "").indexOf("/_stcore/stream") !== -1;
    } catch (e) {
      return false;
    }
  }

  function watch(ws) {
    if (ws === sock) return;
    sock = ws;
    if (seen) {
      if (seen.has(ws)) return;
      seen.add(ws);
    }
    ws.addEventListener("message", function () {
      if (ws === sock) heard();
    });
    ws.addEventListener("close", function () {
      if (ws === sock) {
        waitingSince = 0;
        probeAt = 0;
      }
    });
  }

  // Las conexiones nuevas (cuando Streamlit reconecta) se vigilan desde que se
  // crean, aunque todavía no manden nada.
  if (typeof win.Proxy === "function" && typeof win.Reflect === "object") {
    try {
      win.WebSocket = new win.Proxy(WS, {
        construct: function (target, args) {
          var ws = win.Reflect.construct(target, args);
          try {
            if (isStreamlitSocket(ws)) watch(ws);
          } catch (e) {}
          return ws;
        }
      });
    } catch (e) {}
  }

  // Streamlit manda todo por WebSocket.prototype.send: así se sabe qué socket
  // usa y cuándo espera respuesta, sin tocar su código.
  WS.prototype.send = function () {
    try {
      if (this.readyState === 1 && isStreamlitSocket(this)) {
        watch(this);
        if (!waitingSince) waitingSince = Date.now();
      }
    } catch (e) {}
    return originalSend.apply(this, arguments);
  };

  function heard() {
    lastRx = Date.now();
    waitingSince = 0;
    probeAt = 0;
    attempts = 0;
    reconnectAt = 0;
    if (bannerKind && bannerKind !== "ok") {
      showBanner("ok");
      win.clearTimeout(toastTimer);
      toastTimer = win.setTimeout(function () {
        if (bannerKind === "ok") hideBanner();
      }, 7000);
    }
  }

  function probe(now) {
    if (!sock || sock.readyState !== 1) return false;
    try {
      originalSend.call(sock, HEARTBEAT);
    } catch (e) {
      return false;
    }
    probeAt = now;
    return true;
  }

  // Simula el cierre que el navegador nunca avisó: Streamlit lo trata como un
  // corte normal, abre otra conexión y vuelve a pedir la pantalla con lo que
  // ya estaba escrito.
  function forceReconnect() {
    var ws = sock;
    if (!ws) return false;
    try {
      var ev = typeof win.CloseEvent === "function"
        ? new win.CloseEvent("close", { code: 4000, reason: "sin respuesta del servidor" })
        : new win.Event("close");
      ws.dispatchEvent(ev);
    } catch (e) {
      return false;
    }
    try { ws.close(); } catch (e) {}
    return true;
  }

  function dead(now) {
    waitingSince = 0;
    probeAt = 0;
    if (attempts < 2 && forceReconnect()) {
      attempts += 1;
      reconnectAt = now;
      showBanner("reconnecting");
    } else {
      reconnectAt = 0;
      showBanner("reload");
    }
  }

  function connectionState() {
    var root = doc.querySelector('[data-testid="stApp"]');
    return root ? root.getAttribute("data-test-connection-state") : null;
  }

  function tick() {
    var now = Date.now();
    var gap = now - lastTick;
    lastTick = now;
    if (gap > TICK_MS * 5) {
      // La página estuvo congelada (suspensión o pestaña en segundo plano):
      // ese tiempo no cuenta como espera de respuesta.
      if (waitingSince) waitingSince = now;
      if (probeAt) probeAt = now;
      if (reconnectAt) reconnectAt = now;
      if (offlineSince) offlineSince = now;
    }

    var state = connectionState();
    if (state === "DISCONNECTED_FOREVER") {
      showBanner("reload");
      return;
    }
    if (state === "PINGING_SERVER" || state === "CONNECTING") {
      if (!offlineSince) offlineSince = now;
      var offFor = now - offlineSince;
      var gaveUp = reconnectAt && now - reconnectAt >= cfg.reconnectWaitMs;
      if (gaveUp || offFor >= cfg.offlineReloadMs) showBanner("reload");
      else if (offFor >= cfg.offlineNoticeMs && bannerKind !== "reload") showBanner("reconnecting");
      return;
    }
    offlineSince = 0;
    if (state && state !== "CONNECTED") return;  // todavía cargando

    if (probeAt) {
      if (now - probeAt >= cfg.probeTimeoutMs) dead(now);
      return;
    }
    if (reconnectAt && now - reconnectAt >= cfg.reconnectWaitMs) {
      reconnectAt = 0;
      showBanner("reload");
    }
    // Con un aviso en pantalla o recién reconectado se confirma de inmediato
    // con un latido: si contesta, el aviso se quita solo.
    var recovering = reconnectAt || (bannerKind && bannerKind !== "ok");
    if (recovering ||
        (waitingSince && now - waitingSince >= cfg.replyGraceMs) ||
        (!doc.hidden && (probeSoon || now - lastRx >= cfg.idleProbeMs))) {
      if (probe(now)) probeSoon = false;
    }
  }

  function reloadPage() {
    // Recarga el marco más alto del mismo dominio (en Streamlit Cloud, la
    // página completa); si la app está dentro de otra, solo esta app.
    var target = win;
    try {
      while (target.parent && target.parent !== target) {
        var up = target.parent;
        void up.location.href;  // lanza error si es de otro dominio
        target = up;
      }
    } catch (e) {}
    try {
      target.location.reload();
    } catch (e) {
      win.location.reload();
    }
  }

  var CSS = [
    ".td-conn-banner{position:fixed;top:12px;left:50%;transform:translateX(-50%);",
    "z-index:2147483000;width:min(560px,calc(100vw - 24px));box-sizing:border-box;",
    "display:flex;gap:12px;align-items:center;padding:12px 14px;border-radius:12px;",
    "font:14px/1.4 -apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;",
    "box-shadow:0 10px 30px rgba(0,0,0,.25);text-align:left}",
    ".td-conn-banner[data-kind=reconnecting]{background:#eff6ff;color:#1e3a8a;border:1px solid #93c5fd}",
    ".td-conn-banner[data-kind=reload]{background:#fff7ed;color:#7c2d12;border:1px solid #fb923c}",
    ".td-conn-banner[data-kind=ok]{background:#f0fdf4;color:#14532d;border:1px solid #86efac}",
    ".td-conn-banner .td-conn-text{flex:1;min-width:0}",
    ".td-conn-banner strong{display:block;margin-bottom:2px}",
    ".td-conn-banner button{flex:none;border:0;border-radius:8px;padding:10px 14px;",
    "font-family:inherit;font-size:14px;font-weight:700;cursor:pointer;background:#ea580c;color:#fff}",
    "@media (max-width:520px){.td-conn-banner{flex-direction:column;align-items:stretch}",
    ".td-conn-banner button{width:100%}}"
  ].join("");

  var TEXTS = {
    reconnecting: [
      "🔄 Reconectando con el servidor…",
      "La app estuvo un rato sin conexión. Espera unos segundos, no hace falta recargar."
    ],
    reload: [
      "⚠️ La app perdió la conexión con el servidor",
      "Pasa cuando se deja abierta mucho tiempo sin usar. Recarga la página para seguir trabajando."
    ],
    offline: [
      "📶 Sin conexión a internet",
      "Revisa la red. Cuando regrese, la app intentará reconectarse; si no responde, recarga la página."
    ],
    ok: [
      "✅ Conexión restablecida",
      "Si algún botón no respondió, vuelve a presionarlo."
    ]
  };

  function showBanner(kind) {
    if (bannerKind === kind && banner && banner.parentNode) return;
    hideBanner();
    if (!doc.getElementById("td-conn-style")) {
      var style = doc.createElement("style");
      style.id = "td-conn-style";
      style.textContent = CSS;
      (doc.head || doc.documentElement).appendChild(style);
    }
    var offline = kind === "reload" && win.navigator && win.navigator.onLine === false;
    var text = TEXTS[offline ? "offline" : kind];
    var box = doc.createElement("div");
    box.className = "td-conn-banner";
    box.setAttribute("data-kind", kind);
    box.setAttribute("role", kind === "ok" ? "status" : "alert");
    var body = doc.createElement("div");
    body.className = "td-conn-text";
    var title = doc.createElement("strong");
    title.textContent = text[0];
    var detail = doc.createElement("span");
    detail.textContent = text[1];
    body.appendChild(title);
    body.appendChild(detail);
    box.appendChild(body);
    if (kind === "reload") {
      var button = doc.createElement("button");
      button.type = "button";
      button.textContent = "🔄 Recargar página";
      button.addEventListener("click", reloadPage);
      box.appendChild(button);
    }
    (doc.body || doc.documentElement).appendChild(box);
    banner = box;
    bannerKind = kind;
  }

  function hideBanner() {
    win.clearTimeout(toastTimer);
    if (banner && banner.parentNode) banner.parentNode.removeChild(banner);
    banner = null;
    bannerKind = "";
  }

  function checkSoon() {
    probeSoon = true;
    tick();
  }

  doc.addEventListener("visibilitychange", function () {
    if (!doc.hidden) tick();
  });
  win.addEventListener("online", checkSoon);
  win.addEventListener("pageshow", function (ev) {
    if (ev && ev.persisted) checkSoon();
  });
  win.setInterval(tick, TICK_MS);

  // Para revisar desde la consola del navegador.
  api.status = function () {
    return {
      connection: connectionState(),
      socketKnown: !!sock,
      secondsSinceServer: Math.round((Date.now() - lastRx) / 1000),
      waiting: !!waitingSince,
      probing: !!probeAt,
      reconnecting: !!reconnectAt,
      banner: bannerKind
    };
  };
  api.check = checkSoon;
}
</script>
"""


def watchdog_html() -> str:
    """HTML del componente con los tiempos de este módulo."""

    return _WATCHDOG_SCRIPT.replace("__CONFIG__", json.dumps(WATCHDOG_CONFIG))


def install_connection_watchdog() -> None:
    """Instala el vigilante de conexión. Llamar una vez por corrida, arriba del script."""

    components.html(watchdog_html(), height=0)
