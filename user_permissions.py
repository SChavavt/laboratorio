"""Usuarios y permisos compartidos por las vistas del laboratorio."""

APP_USERS = ("Admin", "Jime", "Lesly", "Vero", "Juan", "Paula")
ORDER_EDIT_USERS = frozenset({"Admin", "Jime", "Lesly", "Vero"})
VIEW_AND_CREATE_USERS = frozenset({"Juan", "Paula"})
# Usuarios de consulta que además pueden cambiar la etapa (sólo STATUS) en estas hojas.
STATUS_ONLY_SHEETS = {"Juan": frozenset({"POLANCO", "AERIS"})}


def can_edit_orders(current_user: str) -> bool:
    return current_user in ORDER_EDIT_USERS


def can_create_orders(current_user: str) -> bool:
    return current_user in APP_USERS


def can_change_status(current_user: str, sheet_name: str) -> bool:
    """Cambiar la etapa de un pedido existente en la hoja indicada."""
    return can_edit_orders(current_user) or sheet_name in STATUS_ONLY_SHEETS.get(current_user, ())


def status_only(current_user: str, sheet_name: str) -> bool:
    """Puede cambiar la etapa pero ninguna otra columna."""
    return not can_edit_orders(current_user) and can_change_status(current_user, sheet_name)
