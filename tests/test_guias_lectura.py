"""Lectura de direcciones para Solicitar guía; datos inventados, sin servicios externos."""
from io import BytesIO
from pathlib import Path
import shutil
import sys

import pytest
from PIL import Image, ImageDraw, ImageFont

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import guias_lectura as lectura
from guias_lectura import Lectura, combinar_lecturas, leer_archivo, leer_texto, normalizar
from pdf_prueba import factura_pdf, nota_venta_pdf, pdf_renglones


# Correo de guía como los que se mandaban a pedidos: remitente (TD) y destinatario.
CORREO_GUIA = """GUIA HILDA TORRE
Graphy.TD <graphy.td@gmail.com>
jue, 10 sept, 12:38
para pedidos, TD

REMITENTE
JUAN MEJIA
Acapulco 36,
Roma Norte
06700 Cuauhtémoc
+52 55 14647413

DESTINATARIO
HILDA TORRE MARTÍNEZ
CALLE:  Octavio Paz
NO. EXTERIOR:  940
NO. INTERIOR:
COLONIA: Ladrón de Guevara
C.P.: 44600
CIUDAD: Guadalajara
ESTADO: Jal.
TELÉFONO: 33 1234 5678
REFERENCIAS: Consultorio frente al parque"""

WHATSAPP = """Mesa del Fraile No. 11, Fraccionamiento Misión San Joaquín, Corregidora Querét.
C.P. 76904
6:58 p.m.
Reenviado
Ana Lucía Pérez Soto
Teléfono
: 4421234567
7:43 p.m."""


def test_correo_usa_el_destinatario_y_descarta_remitente_y_encabezado():
    leido = leer_texto(CORREO_GUIA)
    assert not leido.motivo and leido.fuente == "el texto pegado"
    assert leido.direccion == {
        "nombre": "HILDA TORRE MARTÍNEZ", "calle_numero": "Octavio Paz 940", "colonia": "Ladrón de Guevara",
        "codigo_postal": "44600", "ciudad": "Guadalajara", "estado": "Jalisco", "telefono": "3312345678",
        "referencias": "Consultorio frente al parque", "pais": "México",
    }


def test_correo_con_interior_municipio_y_destinatario_en_la_misma_linea():
    leido = leer_texto(
        "REMITENTE: JUAN MEJIA\nAcapulco 36, Roma Norte\n06700 Cuauhtémoc\n\n"
        "Destinatario: Dra. Laura Gómez\nCalle: Av. Vallarta\nNum. Ext.: 1500\nNum. Int.: 3B\n"
        "Colonia: Americana\nCódigo postal: 44160\nMunicipio / Alcaldía: Guadalajara\nEstado: Jalisco\n"
        "Cel: +52 1 33 9876 5432\nCorreo: laura@example.com\nEntre calles: Chapultepec y Marsella"
    )
    d = leido.direccion
    assert d["nombre"] == "Dra. Laura Gómez"
    assert (d["calle_numero"], d["interior"], d["colonia"], d["codigo_postal"]) == (
        "Av. Vallarta 1500", "3B", "Americana", "44160")
    assert (d["municipio"], d["ciudad"], d["estado"]) == ("Guadalajara", "Guadalajara", "Jalisco")
    assert (d["telefono"], d["correo"]) == ("3398765432", "laura@example.com")
    assert d["referencias"] == "Entre calles Chapultepec y Marsella"
    assert "Acapulco" not in str(d) and "06700" not in str(d)


def test_whatsapp_separa_direccion_nombre_y_telefono():
    d = leer_texto(WHATSAPP).direccion
    assert d["nombre"] == "Ana Lucía Pérez Soto"
    assert (d["calle_numero"], d["colonia"], d["codigo_postal"]) == (
        "Mesa del Fraile No. 11", "Fraccionamiento Misión San Joaquín", "76904")
    assert (d["ciudad"], d["estado"], d["pais"], d["telefono"]) == ("Corregidora", "Querétaro", "México", "4421234567")


def test_datos_etiquetados_con_lada_y_codigo_postal_con_o():
    d = leer_texto("""Nombre: Ana Lucía Pérez Soto
Calle: Avenida 1 # 2
Colonia: Centro
Municipio: Corregidora
Estado: Qro.
C.P.: 769O4
Teléfono: +52 (442) 123-4567
Consultorio: 2
Correo: ejemplo@example.com
Referencias: Entregar en recepción""").direccion
    assert (d["calle_numero"], d["codigo_postal"], d["estado"], d["ciudad"]) == (
        "Avenida 1 # 2", "76904", "Querétaro", "Corregidora")
    assert (d["telefono"], d["interior"], d["correo"], d["referencias"]) == (
        "4421234567", "Consultorio 2", "ejemplo@example.com", "Entregar en recepción")


@pytest.mark.parametrize("texto, esperado", [
    ("Dra. Laura Gómez Ruiz\nAv. Juárez 123 int 4\nCol. Centro\nGuadalajara, Jalisco\nC.P. 44100\nCel 33 1111 2222",
     {"nombre": "Dra. Laura Gómez Ruiz", "calle_numero": "Av. Juárez 123 int 4", "colonia": "Centro",
      "ciudad": "Guadalajara", "estado": "Jalisco", "codigo_postal": "44100", "telefono": "3311112222"}),
    # Sin "C.P.": el número de 5 dígitos suelto, y la ciudad que le sigue.
    ("Carlos Ramírez\nCalle Morelos 45, Col. Centro, 64000 Monterrey, N.L.\nTel. 81 2345 6789",
     {"nombre": "Carlos Ramírez", "calle_numero": "Calle Morelos 45", "colonia": "Centro", "codigo_postal": "64000",
      "ciudad": "Monterrey", "estado": "Nuevo León", "telefono": "8123456789"}),
    ("Contacto: Ana López\nDomicilio: Reforma 10, Col. Juárez, C.P. 06600, Cuauhtémoc, CDMX\nWhatsApp: 55 9876 5432",
     {"nombre": "Ana López", "calle_numero": "Reforma 10", "colonia": "Juárez", "codigo_postal": "06600",
      "ciudad": "Cuauhtémoc", "estado": "CDMX", "telefono": "5598765432"}),
])
def test_direcciones_escritas_sin_formato(texto, esperado):
    d = leer_texto(texto).direccion
    assert {clave: d.get(clave, "") for clave in esperado} == esperado


def test_un_no_suelto_en_la_conversacion_no_es_etiqueta():
    d = leer_texto("Ana López\nNo\nCalle 5 de Mayo 20, Col. Centro, C.P. 64000, Monterrey, NL\nTel 8112345678").direccion
    assert d["calle_numero"] == "Calle 5 de Mayo 20" and d["nombre"] == "Ana López"


@pytest.mark.parametrize("texto", ["Gracias por su ayuda\n7:43 p.m.", "Cuenta destino: 4421234567", "", "76904"])
def test_no_inventa_direcciones_con_texto_irrelevante(texto):
    leido = leer_texto(texto)
    assert not leido.direccion and "no se reconoció" in leido.motivo


def test_factura_da_cliente_folio_y_consignado_no_el_domicilio_fiscal():
    leido = leer_archivo("F200101.pdf", factura_pdf())
    assert leido.es_factura and leido.folio == "F200101" and leido.fuente == "la factura F200101"
    assert leido.direccion == {
        "nombre": "ANA LUCÍA PÉREZ SOTO", "calle_numero": "Av. Colón # 450", "colonia": "Americana",
        "codigo_postal": "44160", "telefono": "3336150000", "ciudad": "Guadalajara", "estado": "Jalisco",
        "pais": "México",
    }


def test_nota_de_venta_da_cliente_sin_numero_y_domicilio():
    leido = leer_archivo("NV_4770.pdf", nota_venta_pdf())
    assert not leido.es_factura and leido.fuente == "la nota de venta 4770" and not leido.folio
    assert leido.direccion == {
        "nombre": "ROBERTO CARLOS NÚÑEZ GARZA", "calle_numero": "PRIVADA LAS ENCINAS 812 CONS 4",
        "colonia": "Obispado", "codigo_postal": "64060", "ciudad": "Monterrey", "estado": "Nuevo León",
        "municipio": "Monterrey", "telefono": "8112345678", "pais": "México",
    }


def test_correo_guardado_en_pdf_se_lee_como_texto():
    leido = leer_archivo("correo.pdf", pdf_renglones(CORREO_GUIA.splitlines()))
    assert leido.fuente == "el PDF correo.pdf"
    assert leido.direccion["nombre"] == "HILDA TORRE MARTÍNEZ"
    assert leido.direccion["calle_numero"] == "Octavio Paz 940"


def test_archivos_que_no_se_pueden_leer():
    assert "sólo se leen" in leer_archivo("pedido.xlsx", b"x").motivo
    assert leer_archivo("roto.pdf", b"no es pdf").motivo
    assert leer_archivo("captura.png", b"no es una imagen").motivo == lectura.MOTIVO_SIN_OCR


def captura_whatsapp():
    imagen = Image.new("RGB", (600, 185), (14, 18, 22))
    dibujar = ImageDraw.Draw(imagen)
    font_path = Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf")
    fuente = ImageFont.truetype(str(font_path), 13) if font_path.exists() else ImageFont.load_default(size=13)
    dibujar.rounded_rectangle((16, 18, 584, 67), radius=5, fill=(32, 38, 38))
    dibujar.rounded_rectangle((16, 74, 310, 175), radius=5, fill=(32, 38, 38))
    for y, texto in (
        (26, "Mesa del Fraile No. 11, Fraccionamiento Misión San Joaquín, Corregidora Querét."),
        (46, "C.P. 76904"), (77, "Reenviado"), (96, "Ana Lucía Pérez Soto"), (116, "Teléfono"),
    ):
        dibujar.text((25, y), texto, font=fuente, fill=(245, 245, 240))
    dibujar.text((25, 137), "4421234567", font=fuente, fill=(38, 220, 106))
    salida = BytesIO()
    imagen.save(salida, format="PNG")
    return salida.getvalue()


def test_sin_ocr_disponible_la_captura_queda_para_captura_manual(monkeypatch):
    def no_disponible(_):
        raise FileNotFoundError("tesseract")
    monkeypatch.setattr(lectura, "extraer_texto_imagen", no_disponible)
    leido = leer_archivo("captura.png", captura_whatsapp())
    assert not leido.direccion and leido.motivo == lectura.MOTIVO_SIN_OCR


@pytest.mark.skipif(not shutil.which("tesseract"), reason="OCR nativo no instalado")
def test_ocr_real_de_captura_oscura():
    leido = leer_archivo("captura.png", captura_whatsapp())
    assert not leido.motivo and leido.fuente == "la captura captura.png"
    d = leido.direccion
    assert normalizar(d["nombre"]) == "ana lucia perez soto"
    assert (d["calle_numero"], d["codigo_postal"], d["telefono"], d["estado"]) == (
        "Mesa del Fraile No. 11", "76904", "4421234567", "Querétaro")


def test_factura_manda_y_la_captura_del_mismo_cliente_completa():
    factura = leer_archivo("F1.pdf", factura_pdf(consignado=("ENVIO Calle:Av. Colón # 450 Col. Americana CP.",
                                                            "44160 Guadalajara, Jalisco MEX")))
    captura = Lectura(direccion={"nombre": "Ana Lucía Pérez Soto", "telefono": "3311110000",
                                 "calle_numero": "Otra 1"}, fuente="la captura c.png")
    combinada, avisos = combinar_lecturas([captura, factura, Lectura(fuente="x.png", motivo="sin texto")])
    assert combinada.direccion["calle_numero"] == "Av. Colón # 450"
    assert combinada.direccion["telefono"] == "3311110000"
    assert combinada.folio == "F200101" and combinada.es_factura
    assert combinada.fuente == "la factura F200101 y la captura c.png"
    assert avisos == ["No se leyó x.png: sin texto."]


def test_archivo_de_otro_destinatario_no_se_mezcla():
    base = Lectura(direccion={"nombre": "Ana", "calle_numero": "Calle 1"}, fuente="el PDF a.pdf")
    otra = Lectura(direccion={"nombre": "Pedro", "calle_numero": "Calle 2", "telefono": "1"}, fuente="la captura b.png")
    combinada, avisos = combinar_lecturas([base, otra])
    assert combinada.direccion == {"nombre": "Ana", "calle_numero": "Calle 1"}
    assert avisos == ["La captura b.png es de otro destinatario (Pedro); se usó el PDF a.pdf."]
    assert combinar_lecturas([Lectura(fuente="x", motivo="no")]) == (None, ["No se leyó x: no."])
