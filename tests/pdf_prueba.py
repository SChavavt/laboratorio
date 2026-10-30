"""PDFs de prueba con el acomodo de las facturas y notas de venta TD&S.

Es el mismo generador de app_v (tests/facturas_prueba.py): las posiciones salen
de documentos reales; clientes, direcciones y teléfonos son inventados.
"""

ALTO_PAGINA = 792


def pdf_posicionado(*paginas):
    """PDF mínimo en Helvetica: cada página es una lista de (x, top, texto[, tamaño])."""

    def _escapar(texto):
        return texto.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")

    objetos = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /Encoding /WinAnsiEncoding >>",
    ]
    hojas = []
    for textos in paginas:
        partes = []
        for x, top, texto, *tamano in textos:
            tamano = tamano[0] if tamano else 7.3
            y = ALTO_PAGINA - top - tamano
            partes.append(f"BT /F1 {tamano} Tf {x} {y} Td ({_escapar(texto)}) Tj ET")
        contenido = "\n".join(partes).encode("cp1252")
        objetos.append(b"<< /Length %d >>\nstream\n" % len(contenido) + contenido + b"\nendstream")
        objetos.append(
            f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 {ALTO_PAGINA}] /Contents {len(objetos)} 0 R "
            "/Resources << /Font << /F1 3 0 R >> >> >>".encode()
        )
        hojas.append(f"{len(objetos)} 0 R")
    objetos[1] = f"<< /Type /Pages /Kids [{' '.join(hojas)}] /Count {len(hojas)} >>".encode()

    salida = b"%PDF-1.4\n"
    posiciones = []
    for numero, objeto in enumerate(objetos, start=1):
        posiciones.append(len(salida))
        salida += f"{numero} 0 obj\n".encode() + objeto + b"\nendobj\n"
    inicio_xref = len(salida)
    salida += f"xref\n0 {len(objetos) + 1}\n0000000000 65535 f \n".encode()
    salida += "".join(f"{p:010d} 00000 n \n" for p in posiciones).encode()
    salida += (
        f"trailer\n<< /Size {len(objetos) + 1} /Root 1 0 R >>\nstartxref\n{inicio_xref}\n%%EOF"
    ).encode()
    return salida


def factura_pdf(
    folio="F200101",
    nombre=("ANA LUCÍA PÉREZ SOTO",),
    consignado=(
        "ENVIO Calle:Av. Colón # 450 Col. Americana CP.",
        "44160 Tel: (33) 3615-0000 Guadalajara, Jalisco MEX",
    ),
    vendedor=("KAREN JAQUELINE", "CARDENAS GONZALEZ"),
):
    """Factura TD&S inventada; ``nombre``, ``consignado`` y ``vendedor`` son renglones."""
    x_vendedor = 490.8
    pagina = [
        (28.2, 30, "TD&S", 11),
        (500, 30, folio, 11),
        (28.2, 52, "R.F.C. TD&111116A4A"),
        (430, 52, "Tipo comprobante: Ingreso"),
        (28.2, 146.3, "Receptor de CFDI"),
        (270, 146.3, "Consignado a"),
        (x_vendedor, 146.3, "Vendedor"),
        (28.2, 156.5, "RFC: XAXX010101000"),
        (28.2, 164.9, "Código: 20337"),
        (28.2, 173.3, "Régimen fiscal: 616"),
    ]
    top = 181.7
    for i, renglon in enumerate(nombre):
        pagina.append((28.2, top, ("Nombre: " if i == 0 else "") + renglon))
        top += 8.4
    pagina += [
        # El domicilio fiscal no es la dirección de envío.
        (28.2, top, "Domicilio fiscal: Av. Hidalgo # 10 Col.Centro C.P."),
        (28.2, top + 8.4, "45000 Zapopan, Jalisco MEX Tel. 3311112222"),
    ]
    for i, renglon in enumerate(consignado):
        pagina.append((270, 185.9 + 8.4 * i, renglon))
    for i, renglon in enumerate(vendedor):
        pagina.append((x_vendedor, 185.9 + 8.4 * i, renglon))
    pagina += [
        (28.2, 233.9, "Uso Cfdi"),
        (145.2, 233.9, "Método pago"),
        (303.6, 233.9, "Forma pago"),
        (487.8, 233.9, "Condición Moneda"),
        (28.2, 244, "S01 - Sin efectos fiscales"),
        (28.2, 260, "1 Pieza BRK0161 42152700 KIT BRACKET $1,000.00 $1,000.00"),
        (450, 620, "Total:$1,160.00"),
    ]
    return pdf_posicionado(pagina)


def nota_venta_pdf(folio="4770", atencion="15185 ROBERTO CARLOS NÚÑEZ GARZA",
                   domicilio="PRIVADA LAS ENCINAS 812 CONS 4", colonia="Obispado", localidad="Monterrey.",
                   municipio="Monterrey", estado="Nuevo León", cp="64060", telefono="8112345678"):
    """Nota de venta TD&S ("Orden de surtido"); los campos vacíos el sistema los imprime como "."."""
    return pdf_posicionado([
        (248.7, 29.7, "TD&S, S.A. DE C.V.", 7.8),
        (474.9, 29.7, f"Orden de surtido: #{folio}", 7.8),
        (367.5, 37.4, "Dirección:", 7.2),
        (346.5, 45.2, "CARRETERA NACIONAL", 7.2),
        (356.4, 60.8, "Col. El Encino; CP", 7.2),
        (376.5, 68.6, "64987", 7.2),
        (346.8, 76.4, "Monterrey Nuevo León", 7.2),
        (28.5, 108.3, "Atención:", 7.8),
        (28.5, 116.7, atencion, 7.8),
        (28.5, 125.1, f"Domicilio: {domicilio}", 7.8),
        (28.5, 133.5, f"Colonia: {colonia}", 7.8),
        (150, 133.5, f"Localidad: {localidad}", 7.8),
        (28.5, 141.9, f"Municipio: {municipio}", 7.8),
        (150, 141.9, f"Estado: {estado}", 7.8),
        (28.5, 150.3, f"C.P: {cp}", 7.8),
        (28.5, 158.7, "RFC: XAXX010101000", 7.8),
        (28.5, 167.1, f"Teléfono: {telefono}", 7.8),
        (28.5, 175.5, "Este documento no es un comprobante de pago.", 7.8),
        (28.5, 183.9, "Vendedor: ALEJANDRO", 7.8),
        (38.7, 198.3, "Cantidad Unidad Código Descripción Loc Peso", 7.8),
    ])


def pdf_renglones(renglones):
    """PDF con texto corrido, un renglón debajo de otro (p. ej. un correo guardado en PDF)."""
    return pdf_posicionado([(40, 40 + 14 * i, renglon, 10) for i, renglon in enumerate(renglones)])
