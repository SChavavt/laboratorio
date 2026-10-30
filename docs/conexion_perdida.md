# App congelada tras mucho tiempo abierta

Si una app se deja abierta mucho rato (computadora suspendida, celular con la app en segundo plano, cambio de red), la conexión en vivo con el servidor de Streamlit puede morir sin que el navegador se entere. Streamlit sigue creyendo que está conectado: los botones, incluido "Recargar" de la propia app, no hacen nada hasta recargar la página desde el navegador.

`install_connection_watchdog()` (en `connection_watchdog.py`) se llama justo después de `set_page_config` en cada app y agrega un vigilante a la página:

- Si un clic no recibe respuesta en 3 s, o la pantalla lleva 1 min sin noticias del servidor, manda un latido (`app_heartbeat`). El servidor lo contesta al instante aunque el script esté ocupado, así que un script lento no dispara avisos.
- Si el latido no regresa en 6 s, la conexión está muerta: el vigilante hace que Streamlit se reconecte y muestra "🔄 Reconectando…". Lo escrito en pantalla se conserva. Al reconectar avisa "✅ Conexión restablecida" y pide volver a presionar el botón que no respondió (ese clic se pierde, nunca se repite solo).
- Si en 15 s no logra reconectar, o Streamlit lleva 20 s sin conexión, muestra "⚠️ La app perdió la conexión" con el botón "🔄 Recargar página". Sin internet, el aviso lo dice. Si la conexión vuelve sola, el aviso se quita.
- Al volver a la pestaña o al regresar la red se revisa la conexión de inmediato.

El vigilante se instala una sola vez por página, no dibuja nada mientras todo está bien y su iframe no deja hueco. En la consola del navegador, `__tdConnWatchdog.status()` muestra su estado.

`connection_watchdog.py` y su prueba son iguales en app-ventas-td, app_almacen_TD y laboratorio; un cambio va en las tres. Las pruebas confirman que el latido coincide con el protobuf de la versión de Streamlit instalada y que el frontend sigue exponiendo `data-test-connection-state`; si fallan al actualizar Streamlit, revisar el vigilante antes de publicar.
