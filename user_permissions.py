"""Usuarios y permisos compartidos por las vistas del laboratorio."""

APP_USERS = ("Admin", "Jime", "Lesly", "Vero", "Juan", "Paula")
ORDER_EDIT_USERS = frozenset({"Admin", "Jime", "Lesly", "Vero"})
VIEW_AND_CREATE_USERS = frozenset({"Juan", "Paula"})


def can_edit_orders(current_user: str) -> bool:
    return current_user in ORDER_EDIT_USERS


def can_create_orders(current_user: str) -> bool:
    return current_user in APP_USERS
