"""Clientes a los que ya se les envió algo, para llenar la guía con su nombre.

Sale de las hojas que ya lee la vista Guías (``data_pedidos`` y
``datos_pedidos``): cada renglón con dirección en ``Direccion_Guia_Retorno``
aporta un destinatario. Ahí guardan su dirección DHL las solicitudes de guía
(de esta vista y de app_v) y las ventas foráneas con guía, de cualquier
vendedor. No hay hoja aparte: cada solicitud que se registra queda disponible
para la siguiente.

Por destinatario se ofrecen hasta 3 direcciones distintas, la más reciente
primero. Si una dirección se usó varias veces, los datos que le falten a la
última vez (teléfono, correo, referencias…) se completan con los de antes.

Nada de aquí usa Streamlit.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from typing import Mapping

import pandas as pd

from guias_lectura import ADDRESS_KEYS, leer_texto, nombre_estado, normalizar

MAX_ADDRESSES_PER_CLIENT = 3
SOURCE_SHEETS = ("data_pedidos", "datos_pedidos")

# Etiqueta que escribe build_dhl_mexico_address_payload (app_v y guias_pg) → campo.
PAYLOAD_LABELS = {
    "nombre": "nombre",
    "calle y numero": "calle_numero",
    "colonia": "colonia",
    "codigo postal": "codigo_postal",
    "ciudad": "ciudad",
    "estado": "estado",
    "pais": "pais",
    "telefono": "telefono",
    "interior / depto": "interior",
    "municipio / alcaldia": "municipio",
    "correo electronico": "correo",
    "referencias de direccion": "referencias",
}


@dataclass
class SavedAddress:
    """Una dirección de un destinatario, con la última vez que se usó."""

    key: str
    address: dict[str, str]
    client: str = ""  # columna Cliente, si es distinta del destinatario
    last_used: datetime | None = None
    uses: int = 1
    reference: str = ""  # Folio o ID de la última vez


def _clean(value) -> str:
    text = "" if value is None else str(value).strip()
    return "" if text.lower() in {"nan", "none", "nat"} else text


@lru_cache(maxsize=20_000)
def _parse_payload_cached(text: str) -> tuple[tuple[str, str], ...]:
    address: dict[str, str] = {}
    for line in text.splitlines():
        label, separator, value = line.partition(":")
        key = PAYLOAD_LABELS.get(normalizar(label))
        if separator and key and value.strip() and key not in address:
            address[key] = value.strip()
    if not address:
        # Dirección escrita a mano antes de que existieran los campos DHL.
        address = dict(leer_texto(text).direccion)
    return tuple(address.items())


def parse_address_payload(text: str) -> dict[str, str]:
    """Direccion_Guia_Retorno ("Nombre: …⏎Calle y número: …") → campos del formulario."""
    return dict(_parse_payload_cached(_clean(text)))


def _address_key(address: Mapping[str, str]) -> str:
    """Misma dirección aunque esté escrita distinto: calle (sin signos) y C.P."""
    street = re.sub(r"[^a-z0-9]", "", normalizar(address.get("calle_numero", "")))
    return f"{street}|{re.sub(r'[^0-9]', '', address.get('codigo_postal', ''))}"


def _name_key(name: str) -> str:
    return re.sub(r"[^a-z0-9 ]", "", normalizar(name)).strip()


def _rows(sources: Mapping[str, pd.DataFrame]) -> pd.DataFrame:
    columns = ["ID_Pedido", "Cliente", "Folio_Factura", "Hora_Registro", "Fecha_Entrega", "Direccion_Guia_Retorno"]
    frames = []
    for sheet in SOURCE_SHEETS:
        df = sources.get(sheet)
        if df is None or df.empty or "Direccion_Guia_Retorno" not in df.columns:
            continue
        df = df.reindex(columns=columns).fillna("").astype(str)
        df = df[df["Direccion_Guia_Retorno"].str.strip().ne("")].copy()
        df["Hoja"] = sheet
        frames.append(df)
    if not frames:
        return pd.DataFrame(columns=[*columns, "Hoja", "Fecha"])
    rows = pd.concat(frames, ignore_index=True)
    try:
        registered = pd.to_datetime(rows["Hora_Registro"], errors="coerce", format="mixed")
        delivery = pd.to_datetime(rows["Fecha_Entrega"], errors="coerce", format="mixed")
    except (TypeError, ValueError):
        registered = pd.to_datetime(rows["Hora_Registro"], errors="coerce")
        delivery = pd.to_datetime(rows["Fecha_Entrega"], errors="coerce")
    rows["Fecha"] = registered.fillna(delivery)
    # Un mismo registro puede estar en las dos hojas: se cuenta una sola vez.
    with_id = rows["ID_Pedido"].str.strip().ne("")
    rows = pd.concat([rows[with_id].drop_duplicates("ID_Pedido"), rows[~with_id]], ignore_index=True)
    return rows.sort_values("Fecha", ascending=False, na_position="last", kind="stable")


def build_client_directory(sources: Mapping[str, pd.DataFrame], *,
                           per_client: int = MAX_ADDRESSES_PER_CLIENT) -> list[SavedAddress]:
    """Direcciones guardadas, la usada más recientemente primero."""

    by_client: dict[str, dict[str, SavedAddress]] = {}
    order: list[SavedAddress] = []
    for row in _rows(sources).itertuples(index=False):
        address = parse_address_payload(row.Direccion_Guia_Retorno)
        client = _clean(row.Cliente)
        address.setdefault("nombre", client)
        if not address.get("nombre") or not (address.get("calle_numero") or address.get("codigo_postal")):
            continue
        name_key = _name_key(address["nombre"])
        address_key = _address_key(address)
        saved = by_client.setdefault(name_key, {})
        if address_key in saved:
            entry = saved[address_key]
            entry.uses += 1
            for key, value in address.items():
                if value and not entry.address.get(key):
                    entry.address[key] = value  # completa lo que le faltó a la última vez
            continue
        if len(saved) >= per_client:
            continue
        date = row.Fecha if isinstance(row.Fecha, datetime) and not pd.isna(row.Fecha) else None
        entry = SavedAddress(
            key=f"{name_key}::{address_key}",
            address={key: address.get(key, "") for key in ADDRESS_KEYS},
            client=client if client and _name_key(client) != name_key else "",
            last_used=date,
            reference=_clean(row.Folio_Factura) or _clean(row.ID_Pedido),
        )
        if entry.address.get("estado"):
            entry.address["estado"] = nombre_estado(entry.address["estado"])
        saved[address_key] = entry
        order.append(entry)
    return order


def saved_address_label(entry: SavedAddress) -> str:
    """Opción del buscador: nombre sin acentos (se busca igual escrito con o sin ellos)."""

    address = entry.address
    name = normalizar(address.get("nombre", "")).upper()
    place = ", ".join(part for part in (address.get("ciudad") or address.get("municipio"), address.get("estado"))
                      if part)
    parts = [name, " · ".join(part for part in (address.get("calle_numero", ""), place) if part)]
    if entry.client:
        parts.append(f"cliente {entry.client}")
    if entry.last_used:
        parts.append(f"último envío {entry.last_used:%d/%m/%y}")
    return " — ".join(part for part in parts if part)
