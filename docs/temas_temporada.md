# Temas de temporada

La app de laboratorio comparte la temática de Halloween con las apps de ventas,
administración y almacén. El encabezado conserva la marca ARTTDLAB y añade
colores, ilustraciones y detalles de temporada.

En la barra lateral, abre **🎨 Apariencia** y usa el selector **Tema**:

- **Automático:** activa Halloween del 1 al 31 de octubre de cada año, según la
  fecha de `America/Mexico_City`; fuera de octubre usa la apariencia habitual.
- **Halloween:** permite ver la temática en cualquier fecha.
- **Habitual:** retira los detalles de temporada.

La elección se conserva durante la sesión de esa app. Desmarca **Animaciones
suaves** para dejar estáticos los adornos. También se respeta la preferencia de
reducir movimiento del dispositivo.

La decoración no intercepta clics ni cambia los datos, permisos, colores de
estados o semáforos operativos de las tablas. Los lectores de pantalla omiten los
adornos, y el diseño se adapta a pantallas pequeñas.

La integración se ejecuta una vez por carga de `main()`, inmediatamente después
de `st.set_page_config`, para cubrir tanto el acceso como las áreas Aparatos,
Alineadores y Guías. El módulo `seasonal_theme.py` contiene la lógica de fecha y
el estilo compartido.
