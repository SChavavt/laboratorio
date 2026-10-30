"""Clientes anteriores para Solicitar guía; datos inventados, sin Sheets."""
from datetime import datetime
from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import guias_pg as guides
from guias_clientes import build_client_directory, parse_address_payload, saved_address_label

GUIA = "📋 Solicitudes de Guía"
HILDA = {
    "nombre": "HILDA TORRE MARTÍNEZ", "calle_numero": "Octavio Paz 940", "colonia": "Ladrón de Guevara",
    "codigo_postal": "44600", "ciudad": "Guadalajara", "estado": "Jalisco", "pais": "México",
    "telefono": "3312345678", "interior": "", "municipio": "", "correo": "", "referencias": "",
}


def payload(**cambios):
    return guides.build_dhl_mexico_address_payload({**HILDA, **cambios})


def row(pedido, hora, direccion, cliente="", tipo=GUIA, folio=""):
    return {"ID_Pedido": pedido, "Hora_Registro": hora, "Cliente": cliente, "Tipo_Envio": tipo,
            "Folio_Factura": folio, "Direccion_Guia_Retorno": direccion}


def test_payload_de_app_v_y_de_esta_vista_se_lee_de_regreso():
    assert parse_address_payload(payload(interior="3B", referencias="Portón negro")) == {
        key: value for key, value in {**HILDA, "interior": "3B", "referencias": "Portón negro"}.items() if value
    }
    # Dirección escrita a mano (antes de los campos DHL): se interpreta como texto.
    libre = parse_address_payload("Calle Morelos 45, Col. Centro, C.P. 64000, Monterrey, NL. Tel 8123456789")
    assert (libre["calle_numero"], libre["codigo_postal"], libre["telefono"]) == (
        "Calle Morelos 45", "64000", "8123456789")
    assert parse_address_payload("") == {}


def test_directorio_junta_envios_repetidos_y_limita_direcciones_por_cliente():
    sources = {
        "data_pedidos": pd.DataFrame([
            row("PED-3", "2026-09-10 12:38:00", payload(telefono=""), cliente="HILDA TORRE MARTÍNEZ"),
            # Venta foránea con guía: el destinatario es Hilda, el cliente la clínica.
            row("PED-4", "2026-10-01 09:00:00", payload(calle_numero="Av. México 2000", codigo_postal="44680"),
                cliente="CLÍNICA DENTAL SUR", tipo="🚚 Pedido Foráneo", folio="F9001"),
            row("PED-5", "2026-10-02 09:00:00", ""),  # sin dirección: no aporta nada
            row("PED-6", "2026-08-01 09:00:00", payload(nombre="Pedro Luna", calle_numero="", codigo_postal="")),
        ]),
        "datos_pedidos": pd.DataFrame([
            # La misma dirección escrita distinto, y una copia de PED-3 en el histórico.
            row("PED-1", "2026-06-01 09:00:00", payload(nombre="Hilda Torre Martinez", calle_numero="OCTAVIO PAZ #940",
                                                         correo="hilda@example.com")),
            row("PED-3", "2026-09-10 12:38:00", payload(telefono="")),
            row("PED-0", "2026-05-01 09:00:00", payload(calle_numero="Calle 1", codigo_postal="44100")),
            row("PED-00", "2026-04-01 09:00:00", payload(calle_numero="Calle 2", codigo_postal="44200")),
        ]),
    }
    directory = build_client_directory(sources)
    assert [entry.address["calle_numero"] for entry in directory] == ["Av. México 2000", "Octavio Paz 940", "Calle 1"]
    venta, ultima, _ = directory
    assert venta.client == "CLÍNICA DENTAL SUR" and venta.reference == "F9001"
    assert venta.last_used == datetime(2026, 10, 1, 9)
    # PED-3 se cuenta una vez; PED-1 es la misma dirección y completa teléfono y correo.
    assert ultima.uses == 2 and ultima.client == "" and ultima.reference == "PED-3"
    assert (ultima.address["telefono"], ultima.address["correo"]) == ("3312345678", "hilda@example.com")
    assert ultima.address["nombre"] == "HILDA TORRE MARTÍNEZ"
    assert saved_address_label(venta) == (
        "HILDA TORRE MARTINEZ — Av. México 2000 · Guadalajara, Jalisco — cliente CLÍNICA DENTAL SUR"
        " — último envío 01/10/26"
    )


def test_sin_columna_de_direccion_no_hay_directorio():
    assert build_client_directory({"data_pedidos": pd.DataFrame([{"ID_Pedido": "PED-1"}])}) == []
    assert build_client_directory({}) == []
