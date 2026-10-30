"""Lectura de la dirección de envío para "📋 Solicitar guía".

Se llena el formulario desde tres fuentes, todas leídas en el servidor (nada se
manda a servicios externos):

- Texto pegado tal como lo mandó el cliente: WhatsApp, correo o el chat de la
  cotización. Si trae los bloques "REMITENTE" y "DESTINATARIO" (formato de los
  correos de guía), el del remitente se ignora: es TD, no el cliente.
- Factura TD&S en PDF: cliente, folio y el recuadro "Consignado a", igual que la
  🪄 de app_v. También notas de venta ("Orden de surtido").
- Capturas de pantalla (OCR con tesseract) y PDFs con texto, como un correo
  guardado con "Convertir a PDF".

La lectura de facturas, notas de venta y conversaciones es la misma de app_v
(``lector_facturas.py`` y ``contactos_imagen.py``): si allá se corrige un caso,
conviene traerlo también aquí.

Nada de aquí usa Streamlit. Devuelve la dirección con las claves del formulario
(``nombre``, ``calle_numero``…) para que la vista la muestre y se revise antes
de registrar.
"""
from __future__ import annotations

import re
import subprocess
import unicodedata
from dataclasses import dataclass, field
from functools import lru_cache
from io import BytesIO

ADDRESS_KEYS = (
    "nombre", "calle_numero", "colonia", "codigo_postal", "ciudad", "estado", "pais", "telefono",
    "interior", "municipio", "correo", "referencias",
)
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp"}
UPLOAD_TYPES = ["pdf", "jpg", "jpeg", "png", "webp"]
MAX_PAGES = 4


@dataclass
class Lectura:
    """Lo que se reconoció en un archivo o texto.

    ``direccion`` sólo trae los campos reconocidos. ``fuente`` dice de dónde
    salió ("la factura F200101", "el texto pegado"…); ``motivo`` por qué no se
    pudo leer y ``aviso`` qué revisar aunque sí se leyó.
    """

    direccion: dict[str, str] = field(default_factory=dict)
    folio: str = ""
    fuente: str = ""
    motivo: str = ""
    aviso: str = ""  # se leyó, pero hay algo que revisar
    es_factura: bool = False


# ---------------------------------------------------------------------------
# Utilidades de texto (las mismas de app_v)
# ---------------------------------------------------------------------------

def _sin_acentos(texto: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", str(texto or "")) if unicodedata.category(c) != "Mn"
    )


def normalizar(texto: str) -> str:
    """Texto comparable: sin acentos, mayúsculas ni espacios de más."""
    return re.sub(r"\s+", " ", _sin_acentos(texto).lower()).strip()


def _limpiar(texto: str) -> str:
    return re.sub(r"\s+", " ", str(texto or "")).strip(" ,;:-")


def _extension(nombre: str) -> str:
    coincidencia = re.search(r"\.[A-Za-z0-9]+$", str(nombre or ""))
    return coincidencia.group(0).lower() if coincidencia else ""


def _quitar_cliente_repetido(calle: str, cliente: str) -> str:
    """Quita el nombre completo del cliente sólo si encabeza la calle."""
    calle = _limpiar(calle)
    nombre = re.findall(r"\w+", _sin_acentos(cliente).casefold())
    if not nombre:
        return calle
    prefijo = list(re.finditer(r"\w+", calle))[:len(nombre)]
    if not prefijo or prefijo[0].start() != 0:
        return calle
    if [_sin_acentos(p.group()).casefold() for p in prefijo] != nombre:
        return calle
    resto = re.sub(r"^[\s,;:.\-]+", "", calle[prefijo[-1].end():])
    # Una calle puede llamarse igual que el cliente ("NICOLAS BRAVO # 237").
    if resto and not any(c.isalpha() for c in resto):
        return calle
    return _limpiar(resto)


# ---------------------------------------------------------------------------
# Estados, teléfonos y códigos postales
# ---------------------------------------------------------------------------

_ESTADOS = {
    "aguascalientes": "Aguascalientes", "ags": "Aguascalientes",
    "baja california sur": "Baja California Sur", "bcs": "Baja California Sur",
    "baja california": "Baja California", "bc": "Baja California",
    "campeche": "Campeche", "camp": "Campeche", "chiapas": "Chiapas", "chis": "Chiapas",
    "chihuahua": "Chihuahua", "chih": "Chihuahua",
    "ciudad de mexico": "CDMX", "cdmx": "CDMX", "d.f": "CDMX", "df": "CDMX", "cd. de mexico": "CDMX",
    "coahuila": "Coahuila", "coah": "Coahuila", "colima": "Colima", "col": "Colima",
    "durango": "Durango", "dgo": "Durango", "guanajuato": "Guanajuato", "gto": "Guanajuato",
    "guerrero": "Guerrero", "gro": "Guerrero", "hidalgo": "Hidalgo", "hgo": "Hidalgo",
    "jalisco": "Jalisco", "jal": "Jalisco",
    "estado de mexico": "Estado de México", "edo. de mexico": "Estado de México",
    "edo de mexico": "Estado de México", "edomex": "Estado de México", "edo. mex": "Estado de México",
    "michoacan": "Michoacán", "mich": "Michoacán", "morelos": "Morelos", "mor": "Morelos",
    "nayarit": "Nayarit", "nay": "Nayarit",
    "nuevo leon": "Nuevo León", "n.l": "Nuevo León", "nl": "Nuevo León", "oaxaca": "Oaxaca", "oax": "Oaxaca",
    "puebla": "Puebla", "pue": "Puebla", "queretaro": "Querétaro", "queret": "Querétaro", "qro": "Querétaro",
    "quintana roo": "Quintana Roo", "q. roo": "Quintana Roo", "q.roo": "Quintana Roo",
    "san luis potosi": "San Luis Potosí", "slp": "San Luis Potosí", "s.l.p": "San Luis Potosí",
    "sinaloa": "Sinaloa", "sin": "Sinaloa", "sonora": "Sonora", "son": "Sonora",
    "tabasco": "Tabasco", "tab": "Tabasco", "tamaulipas": "Tamaulipas", "tamps": "Tamaulipas",
    "tlaxcala": "Tlaxcala", "tlax": "Tlaxcala", "veracruz": "Veracruz", "ver": "Veracruz",
    "yucatan": "Yucatán", "yuc": "Yucatán", "zacatecas": "Zacatecas", "zac": "Zacatecas",
}
# Al final de un renglón sólo se reconocen los nombres completos y las
# abreviaturas inequívocas; "Col" o "Ver" sueltos podrían ser otra cosa.
_ESTADOS_AL_FINAL = {
    clave for clave in _ESTADOS
    if len(clave) > 3 or clave in {"cdmx", "jal", "qro", "gto", "nl", "n.l", "slp", "edomex", "df", "d.f"}
}
_ESTADO_FINAL = re.compile(
    r"(?:^|[\s,])(" + "|".join(re.escape(e) for e in sorted(_ESTADOS_AL_FINAL, key=len, reverse=True))
    + r")\.?$"
)
_CP = re.compile(r"\b(?:C\.?\s*P\.?|C[oó]digo\s+postal)\s*[:\-]?\s*([0-9Oo]{5})\b", re.IGNORECASE)
_TELEFONO = re.compile(r"(?<!\d)(?:\+?\s*52\s*1?\s*)?(\d(?:[ ()\-.]*\d){9})(?![ ()\-.]*\d)")
_TELEFONO_FACTURA = r"\(?\+?\d[\d() .\-]*\d"
_COLONIA = re.compile(r"\b(fraccionamiento|fracc\.?|colonia|col\.?)(?!\w)\s*[:.]?\s*", re.IGNORECASE)
_HORA = re.compile(r"\s*\d{1,2}:\d{2}\s*(?:[ap]\.?\s*m\.?|[ap]m)?\s*[✓✔]*$", re.IGNORECASE)
_CORREO = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)*\.[A-Za-z]{2,}")


def nombre_estado(texto: str) -> str:
    """"Jal." → "Jalisco"; un estado que no se reconoce se deja como viene."""
    limpio = _limpiar(texto).rstrip(".")
    return _ESTADOS.get(normalizar(limpio), limpio)


def telefono_diez_digitos(texto: str) -> str:
    """Primer teléfono de 10 dígitos (sin +52); si no hay, los dígitos que traiga."""
    encontrado = _TELEFONO.search(str(texto or ""))
    if encontrado:
        return re.sub(r"\D", "", encontrado[1])
    digitos = re.sub(r"\D", "", str(texto or ""))
    return digitos if len(digitos) >= 7 else ""


def _lugar(texto: str) -> tuple[str, str]:
    """"Guadalajara, Jalisco" → ("Guadalajara", "Jalisco"); sin estado → (texto, "")."""
    texto = _limpiar(texto)
    sin_pais = re.sub(r"[\s,]+(?:m[eé]xico|mex)\.?$", "", texto, flags=re.IGNORECASE)
    m = _ESTADO_FINAL.search(_sin_acentos(texto).lower())
    if not m and sin_pais != texto:
        texto = sin_pais
        m = _ESTADO_FINAL.search(_sin_acentos(texto).lower())
    if not m:
        return texto, ""
    ciudad, estado = _limpiar(texto[:m.start()]), _ESTADOS[m[1]]
    if not ciudad and estado == "CDMX":
        ciudad = "Ciudad de México"  # "Ciudad de México" o "CDMX" solos también son la ciudad
    return ciudad, estado


# ---------------------------------------------------------------------------
# Facturas TD&S y notas de venta (igual que lector_facturas.py de app_v)
# ---------------------------------------------------------------------------

def _buscar_frase(palabras: list[dict], frase: str, *, cerca_de: float | None = None) -> dict | None:
    """Primera palabra de ``frase`` (en el mismo renglón); opcionalmente a la altura ``cerca_de``."""
    buscadas = [p.casefold() for p in frase.split()]
    for i, palabra in enumerate(palabras):
        grupo = palabras[i:i + len(buscadas)]
        if [p["text"].casefold() for p in grupo] != buscadas:
            continue
        if any(abs(p["top"] - palabra["top"]) > 2 for p in grupo):
            continue
        if cerca_de is not None and abs(palabra["top"] - cerca_de) > 3:
            continue
        return palabra
    return None


def _lineas_recuadro(pagina, x0: float, x1: float, arriba: float, abajo: float) -> list[str]:
    texto = pagina.within_bbox((max(0, x0), arriba, min(pagina.width, x1), abajo)).extract_text() or ""
    return [linea.strip() for linea in texto.splitlines() if linea.strip()]


def _nombre_receptor(lineas: list[str]) -> str:
    etiquetas = re.compile(r"^(domicilio fiscal|rfc|c[oó]digo|r[eé]gimen fiscal|uso cfdi)\b", re.IGNORECASE)
    partes: list[str] = []
    for linea in lineas:
        if partes:
            if etiquetas.match(linea):
                break
            partes.append(linea)
            continue
        coincidencia = re.match(r"^nombre\s*:\s*(.*)$", linea, re.IGNORECASE)
        if coincidencia:
            partes.append(coincidencia.group(1))
    return _limpiar(" ".join(partes))


def parsear_direccion_consignado(texto: str, *, cliente: str = "") -> dict[str, str]:
    """Separa "[alias] [Calle:]calle Col. colonia CP. 12345 Tel: tel Ciudad, Estado MEX"."""
    resto = _limpiar(texto)
    direccion: dict[str, str] = {}
    pais = re.search(r"[\s,]+(MEX|M[EÉ]XICO)\.?$", resto, re.IGNORECASE)
    if pais:
        direccion["pais"] = "México"
        resto = resto[:pais.start()]

    cp = re.search(r"\bC\.?\s?P\.?\s*:?\s*(\d{5})\b", resto, re.IGNORECASE)
    if not cp:
        return direccion
    direccion["codigo_postal"] = cp.group(1)
    antes, despues = resto[:cp.start()], resto[cp.end():]

    # "Col." o "Colonia"; no la calle "Colón". Si se repite, la última es la colonia.
    colonia = None
    for colonia in re.finditer(r"\bCol(?:\.|onia\b\.?)\s*:?\s*", antes, re.IGNORECASE):
        pass
    calle = antes[:colonia.start()] if colonia else antes
    if colonia:
        direccion["colonia"] = _limpiar(antes[colonia.end():])
    etiqueta_calle = re.search(r"\bCalle\s*:\s*", calle, re.IGNORECASE)
    if etiqueta_calle:
        calle = calle[etiqueta_calle.end():]
    # "ENVIO" es el nombre del consignatario que pone el sistema, no parte de la calle.
    calle = re.sub(r"^\s*ENV[IÍ]O\b\s*", "", calle, flags=re.IGNORECASE)
    if not etiqueta_calle:
        calle = _quitar_cliente_repetido(calle, cliente)
    direccion["calle_numero"] = _limpiar(calle)

    etiqueta_telefono = re.match(r"\s*(?:Tel|Cel)\.?\s*:?", despues, re.IGNORECASE)
    if etiqueta_telefono:
        despues = despues[etiqueta_telefono.end():]
        numeros = re.match(
            rf"(?:\s*(?:[,/;]|\by\b|(?:Tel|Cel)\.?\s*:?)?\s*{_TELEFONO_FACTURA})+", despues, re.IGNORECASE
        )
        if numeros:
            telefonos = [re.sub(r"\D", "", t) for t in re.findall(_TELEFONO_FACTURA, numeros.group(0))]
            direccion["telefono"] = telefonos[0]
            despues = despues[numeros.end():]
    lugares = [_limpiar(p) for p in despues.split(",") if _limpiar(p)]
    if lugares:
        direccion["estado"] = nombre_estado(lugares[-1])
    if len(lugares) >= 2 and not re.search(r"\d|\b(?:tel|cel)\b", lugares[-2], re.IGNORECASE):
        direccion["ciudad"] = lugares[-2]
    return direccion


def leer_factura_pdf(contenido: bytes) -> Lectura | None:
    """Factura TD&S: cliente, folio y "Consignado a". ``None`` si no tiene ese formato."""
    import pdfplumber

    try:
        with pdfplumber.open(BytesIO(contenido)) as pdf:
            if not pdf.pages:
                return None
            pagina = pdf.pages[0]
            palabras = pagina.extract_words()
            receptor = _buscar_frase(palabras, "Receptor de CFDI")
            if not receptor:
                return None
            consignado = _buscar_frase(palabras, "Consignado a", cerca_de=receptor["top"])
            if not consignado:
                return None
            vendedor = _buscar_frase(palabras, "Vendedor", cerca_de=receptor["top"])
            uso_cfdi = _buscar_frase([p for p in palabras if p["top"] > receptor["bottom"]], "Uso Cfdi")
            arriba = receptor["bottom"] + 0.5
            abajo = uso_cfdi["top"] - 0.5 if uso_cfdi else receptor["bottom"] + 100
            limite_vendedor = vendedor["x0"] - 1 if vendedor else pagina.width
            receptor_lineas = _lineas_recuadro(pagina, 0, consignado["x0"] - 1, arriba, abajo)
            consignado_lineas = _lineas_recuadro(pagina, consignado["x0"] - 1, limite_vendedor, arriba, abajo)
            candidatos = [
                p["text"] for p in palabras
                if p["top"] < receptor["top"] and p["x0"] > pagina.width / 2
                and re.fullmatch(r"[A-Z]{1,4}\d{3,}", p["text"])
            ]
            folio = next((c for c in candidatos if c.startswith("F")), candidatos[0] if candidatos else "")
    except Exception:
        return None

    cliente = _nombre_receptor(receptor_lineas)
    texto_consignado = " ".join(consignado_lineas)
    direccion = parsear_direccion_consignado(texto_consignado, cliente=cliente) if texto_consignado.strip() else {}
    if cliente:
        direccion["nombre"] = cliente
    return Lectura(
        direccion=direccion,
        folio=folio,
        fuente=f"la factura {folio}".strip(),
        aviso="" if texto_consignado.strip() else f"La factura {folio} no trae el recuadro 'Consignado a'.",
        es_factura=True,
    )


_FOLIO_NOTA_VENTA = re.compile(r"Orden de surtido\s*:?\s*#?\s*(\d+)", re.IGNORECASE)
_ATENCION = re.compile(r"Atenci[oó]n\s*:", re.IGNORECASE)
_FIN_CLIENTE_NOTA = re.compile(
    r"Este documento no es un comprobante|Vendedor\s*:|Cantidad\s+Unidad", re.IGNORECASE
)
_ETIQUETAS_CLIENTE_NOTA = re.compile(
    r"(?<![\w.])(Domicilio|Colonia|Localidad|Municipio|Estado|C\.?\s?P|R\.?F\.?C|CURP|Tel[eé]fono|Celular)"
    r"\s*\.?\s*:",
    re.IGNORECASE,
)


def _valor_nota(texto: str) -> str:
    """"Monterrey." → "Monterrey"; "." (campo vacío en el sistema) → ""."""
    return _limpiar(texto).strip(" .")


def parsear_nota_venta(texto: str) -> Lectura | None:
    """Cliente y domicilio del texto de una nota de venta TD&S ("Orden de surtido")."""
    folio = _FOLIO_NOTA_VENTA.search(texto or "")
    atencion = _ATENCION.search(texto or "")
    if not folio or not atencion:
        return None
    bloque = texto[atencion.end():]
    fin = _FIN_CLIENTE_NOTA.search(bloque)
    partes = _ETIQUETAS_CLIENTE_NOTA.split(bloque[:fin.start()] if fin else bloque)
    # El número de cliente del sistema va antes del nombre: "15185 CESAR IVAN …".
    numero = re.match(r"^(\d+)\s*[-.]?\s*", _limpiar(partes[0]))
    cliente = _limpiar(partes[0])[numero.end():] if numero else _limpiar(partes[0])
    campos: dict[str, str] = {}
    for etiqueta, valor in zip(partes[1::2], partes[2::2]):
        campos.setdefault(re.sub(r"[^a-z]", "", _sin_acentos(etiqueta).lower()), valor)
    codigo_postal = re.search(r"\d{5}", campos.get("cp", ""))
    municipio = _valor_nota(campos.get("municipio", ""))
    telefono = telefono_diez_digitos(campos.get("telefono", "") + " " + campos.get("celular", ""))
    direccion = {
        "nombre": cliente,
        "calle_numero": re.sub(r"^Calle\s*[.:]\s*", "", _valor_nota(campos.get("domicilio", "")), flags=re.IGNORECASE),
        "colonia": _valor_nota(campos.get("colonia", "")),
        "codigo_postal": codigo_postal.group(0) if codigo_postal else "",
        # "Localidad" suele venir vacía ("."): la ciudad para la guía es el municipio.
        "ciudad": _valor_nota(campos.get("localidad", "")) or municipio,
        "estado": nombre_estado(_valor_nota(campos.get("estado", ""))),
        "municipio": municipio,
        "telefono": telefono,
        "pais": "México",
    }
    return Lectura(
        direccion={clave: valor for clave, valor in direccion.items() if valor},
        fuente=f"la nota de venta {folio.group(1)}",
    )


# ---------------------------------------------------------------------------
# Texto de conversaciones y correos
# ---------------------------------------------------------------------------

_RENGLONES_IGNORADOS = {"reenviado", "reenviado muchas veces", "hoy", "ayer"}

# Encabezados de bloque; el del remitente (TD) se descarta completo.
_ENCABEZADO_REMITENTE = re.compile(
    r"^(?:datos\s+del?\s+)?(?:remitente|quien\s+env[ií]a)\s*(?::.*)?$|^origen\s*:?$", re.IGNORECASE
)
_ENCABEZADO_DESTINATARIO = re.compile(
    r"^(?:datos\s+(?:del?\s+)?)?(?:destinatario|destino|consignatario|datos\s+de\s+env[ií]o"
    r"|direcci[oó]n\s+de\s+(?:env[ií]o|entrega)|enviar\s+a|entregar\s+a)\s*:?$",
    re.IGNORECASE,
)

# Etiqueta (sin acentos y en minúsculas) → campo. El orden importa: "no. interior"
# antes que "no." y "número de teléfono" antes que "número".
_ETIQUETAS = (
    ("interior", r"(?:(?:n[uo]m(?:ero)?|no|n[°º]|#)\.?\s*)?int(?:erior|\.)?|depto\.?|departamento|dpto\.?"
                 r"|consultorio|oficina|suite"),
    ("telefono", r"(?:n[uo]m(?:ero)?\.?\s+de\s+)?(?:tel[eé]fonos?|tel\.?|tels\.?|cel(?:ular)?\.?|whats?app"
                 r"|m[oó]vil|contacto)"),
    ("exterior", r"(?:(?:n[uo]m(?:ero)?|no|n[°º]|#)\.?\s*)?ext(?:erior|\.)?|n[uo]m(?:ero)?\.?|no\.?|#"),
    ("calle", r"calle(?:\s+y\s+(?:n[uo]m(?:ero)?|no)\.?)?|calle\s*/\s*avenida|avenida|av\.?"),
    ("direccion", r"direcci[oó]n(?:\s+de\s+(?:env[ií]o|entrega))?|domicilio"),
    ("colonia", r"colonia|col\.?|fraccionamiento|fracc\.?|barrio"),
    ("codigo_postal", r"c\.?\s?p\.?|c[oó]digo\s+postal|cod\.?\s+postal"),
    ("ciudad", r"ciudad|localidad|poblaci[oó]n"),
    ("municipio", r"municipio|alcald[ií]a|delegaci[oó]n|mpio\.?"),
    ("estado", r"estado|entidad(?:\s+federativa)?|edo\.?"),
    ("pais", r"pa[ií]s"),
    ("correo", r"correo(?:\s+electr[oó]nico)?|e-?mail|mail"),
    ("referencias", r"referencias?|entre\s+calles|indicaciones"),
    ("nombre", r"nombre(?:\s+completo)?(?:\s+del?\s+destinatario)?|destinatario|consignatario|recibe"
               r"|a\s+nombre\s+de|atenci[oó]n|atn\.?|cliente"),
)
_ETIQUETAS_RE = tuple((campo, re.compile(rf"(?:{patron})", re.IGNORECASE)) for campo, patron in _ETIQUETAS)
# Una etiqueta sola en su renglón toma el valor del siguiente ("Teléfono" ⏎ "442…"),
# salvo las cortas que también son palabras comunes ("No", "#", "Int", "Av"…).
_ETIQUETAS_SOLO_CON_DOS_PUNTOS = {"no", "#", "num", "nom", "int", "ext", "av", "col", "tel", "cel", "edo", "cp", "c.p"}
# Interiores que conservan su etiqueta: "Consultorio: 204" → "Consultorio 204".
_INTERIOR_CON_ETIQUETA = re.compile(r"^(consultorio|oficina|suite|depto|departamento|dpto)\b", re.IGNORECASE)
# Campos que, si vienen etiquetados, describen la dirección por sí solos.
_CAMPOS_DE_DIRECCION = ("calle", "exterior", "colonia", "codigo_postal", "ciudad", "municipio", "estado")


def _campo_de_etiqueta(texto: str) -> str:
    """"NO. EXTERIOR" → "exterior"; "Ciudad / Municipio" → "ciudad"; "" si no es etiqueta."""
    etiqueta = _sin_acentos(_limpiar(texto)).strip(" .*").lower()
    if not etiqueta or len(etiqueta) > 40:
        return ""
    for parte in (etiqueta, re.split(r"\s*/\s*|\s+o\s+", etiqueta)[0]):
        for campo, patron in _ETIQUETAS_RE:
            if patron.fullmatch(parte):
                return campo
    return ""


def _etiqueta(linea: str) -> tuple[str, str | None] | None:
    """("campo", valor) si el renglón es "Etiqueta: valor"; valor None si sólo es la etiqueta."""
    partido = re.match(r"^\s*([^:：]{1,40}?)\s*[:：]\s*(.*)$", linea)
    if partido:
        campo = _campo_de_etiqueta(partido[1])
        return (campo, _limpiar(partido[2])) if campo else None
    if _sin_acentos(linea).strip(" .:*").lower() in _ETIQUETAS_SOLO_CON_DOS_PUNTOS:
        return None
    campo = _campo_de_etiqueta(linea)
    return (campo, None) if campo else None


def _lineas(texto: str) -> list[str]:
    limpias = []
    for linea in unicodedata.normalize("NFC", str(texto or "")).replace("\t", " ").splitlines():
        linea = _limpiar(_HORA.sub("", linea))
        if linea and normalizar(linea) not in _RENGLONES_IGNORADOS:
            limpias.append(linea)
    return limpias


def _inicio_de_bloque(linea: str) -> str:
    """"remitente", "destinatario" o "" según el renglón abra uno de esos bloques."""
    if _ENCABEZADO_REMITENTE.match(linea):
        return "remitente"
    if _ENCABEZADO_DESTINATARIO.match(linea):
        return "destinatario"
    if re.match(r"^\s*(?:destinatario|consignatario)\s*[:：]\s*\S", linea, re.IGNORECASE):
        return "destinatario"  # "Destinatario: Ana" también abre su bloque
    return ""


def _solo_destinatario(lineas: list[str]) -> list[str]:
    """Quita el bloque del remitente (TD); los encabezados de bloque tampoco se quedan.

    Con remitente y destinatario (formato de correo) sólo queda el bloque del
    destinatario: lo de antes es el encabezado del correo (asunto, quién lo mandó).
    """
    bloques = [_inicio_de_bloque(linea) for linea in lineas]
    solo_destinatario = "remitente" in bloques and "destinatario" in bloques
    salida, actual = [], ""
    for linea, bloque in zip(lineas, bloques):
        if bloque:
            actual = bloque
            etiqueta = _etiqueta(linea)
            if bloque == "remitente" or not (etiqueta and etiqueta[1]):
                continue  # el encabezado solo, sin dato
        if actual == "destinatario" or (actual == "" and not solo_destinatario):
            salida.append(linea)
    return salida


def _parece_nombre(linea: str) -> bool:
    if not re.fullmatch(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ .'-]+", linea) or _etiqueta(linea):
        return False
    palabras = linea.split()
    if not 2 <= len(palabras) <= 8:
        return False
    if re.search(
        r"\b(?:hola|gracias|buenos?|buenas?|saludos|telefono|celular|direccion|calle|avenida|colonia"
        r"|fraccionamiento|guia|envio|paqueteria|favor)\b",
        normalizar(linea),
    ):
        return False
    return not _ESTADO_FINAL.search(_sin_acentos(linea).lower())


def _nombre_libre(lineas: list[str]) -> str:
    """Nombre sin etiqueta: el renglón con forma de nombre junto a un teléfono o una dirección."""
    indice_cp = next((i for i, linea in enumerate(lineas) if _CP.search(linea)), -1)
    for i in range(len(lineas) - 1, -1, -1):
        linea = lineas[i]
        if not _parece_nombre(linea):
            continue
        siguiente = lineas[i + 1] if i + 1 < len(lineas) else ""
        empieza_direccion = bool(
            re.search(r"\b(?:calle|avenida|direcci[oó]n)\b|\bno\.?(?=\s*\d)|#", siguiente, re.IGNORECASE)
            or (_etiqueta(siguiente) or ("",))[0] in {*_CAMPOS_DE_DIRECCION, "direccion", "interior"}
        )
        sigue_telefono = bool(re.search(r"tel[eé]fono|\b(?:tel|cel|celular)\b", siguiente, re.IGNORECASE)
                              or _TELEFONO.search(siguiente))
        if indice_cp >= 0 and i < indice_cp and not (empieza_direccion or sigue_telefono):
            continue
        cercanas = " ".join(lineas[i + 1:i + 3])
        if re.search(r"tel[eé]fono|\b(?:tel|cel|celular)\b", cercanas, re.IGNORECASE) or _TELEFONO.search(cercanas):
            if i > indice_cp + 1 and _parece_nombre(lineas[i - 1]) and len((lineas[i - 1] + " " + linea).split()) <= 8:
                return lineas[i - 1] + " " + linea
            return linea
        if empieza_direccion:
            return linea
    return ""


def _direccion_libre(lineas: list[str], nombre: str) -> dict[str, str]:
    """Calle, colonia, C.P., ciudad y estado de una dirección escrita sin etiquetas.

    Toma el bloque inmediatamente anterior al C.P. ("C.P. 12345", o un número de 5
    dígitos suelto si no hay ninguno marcado) y, si después del C.P. sigue la
    ciudad ("06700 Cuauhtémoc, CDMX"), también la usa.
    """
    marcados = [i for i, linea in enumerate(lineas) if _CP.search(linea)]
    cp_suelto = not marcados
    if cp_suelto:
        # Sin "C.P.": el último número de 5 dígitos suelto que no sea parte de un teléfono.
        for i in range(len(lineas) - 1, -1, -1):
            sin_telefonos = _TELEFONO.sub(" ", lineas[i])
            suelto = list(re.finditer(r"(?<![\d#])(?<!No\. )(?<!No )\b(\d{5})\b(?![\d-])", sin_telefonos))
            if suelto:
                inicio = suelto[-1].start()
                lineas = list(lineas)
                lineas[i] = sin_telefonos[:inicio] + "C.P. " + sin_telefonos[inicio:]
                marcados = [i]
                break
    if not marcados:
        return {}
    inicio = marcados[0]
    linea_cp = lineas[inicio]
    marca = _CP.search(linea_cp)
    direccion = {"codigo_postal": marca[1].replace("O", "0").replace("o", "0")}

    cercanas = lineas[max(0, inicio - 3):inicio + 1]
    candidatos = []
    for i, linea in enumerate(cercanas):
        marca_linea = _CP.search(linea)
        parte = _limpiar(linea[:marca_linea.start()] if marca_linea else linea)
        if (not parte or _etiqueta(parte) or _TELEFONO.fullmatch(parte) or parte == nombre
                or re.search(r"^(?:tel[eé]fono|tel\.?|celular|cel\.?)\b", parte, re.IGNORECASE)):
            continue
        if re.search(r"\d|\bcalle\b|\bav\.|\bavenida\b", parte, re.IGNORECASE) or candidatos:
            candidatos.append(i)

    def _sin_cp(linea):
        marca_linea = _CP.search(linea)
        return _limpiar(linea[:marca_linea.start()] if marca_linea else linea)

    # Renglones de la dirección unidos con coma: "Calle 1" ⏎ "Col. Centro" no se mezclan.
    antes = _limpiar(" , ".join(_sin_cp(cercanas[i]) for i in candidatos)) if candidatos else _sin_cp(linea_cp)
    antes = re.sub(r"^(?:direcci[oó]n|domicilio)\s*:\s*", "", antes, flags=re.IGNORECASE)
    antes = _quitar_cliente_repetido(antes, nombre)
    colonia = _COLONIA.search(antes)
    if colonia:
        direccion["calle_numero"] = _limpiar(antes[:colonia.start()])
        partes = [_limpiar(p) for p in antes[colonia.end():].split(",") if _limpiar(p)]
        if partes:
            prefijo = "Fraccionamiento " if normalizar(colonia[1]).startswith("fracc") else ""
            direccion["colonia"] = prefijo + partes[0]
        if len(partes) > 1:
            direccion["ciudad"], direccion["estado"] = _lugar(", ".join(partes[1:]))
    else:
        partes = [_limpiar(p) for p in antes.split(",") if _limpiar(p)]
        if partes:
            direccion["calle_numero"] = partes[0]
        if len(partes) > 2:
            # "Calle 1, Colonia, Ciudad, Estado": el segundo pedazo es la colonia.
            direccion["colonia"] = partes[1]
            direccion["ciudad"], direccion["estado"] = _lugar(", ".join(partes[2:]))
        elif len(partes) == 2:
            ciudad, estado = _lugar(partes[1])
            if estado:
                direccion["ciudad"], direccion["estado"] = ciudad, estado
            else:
                direccion["colonia"] = partes[1]

    # Lo que sigue al C.P. en su renglón (o en el siguiente) suele ser ciudad y estado,
    # hasta el siguiente dato ("… SLP. Recibe: Ana. Tel 444…").
    despues = re.split(r"(?:\.\s+|\s)(?:tel[eé]fono|tel|cel(?:ular)?|recibe|nombre|referencias?|correo)\b"
                       r"|\b[A-Za-zÁÉÍÓÚáéíóú]+\s*:", linea_cp[marca.end():], maxsplit=1, flags=re.IGNORECASE)[0]
    despues = _limpiar(despues).rstrip(".")
    if not despues and inicio + 1 < len(lineas) and not _etiqueta(lineas[inicio + 1]):
        siguiente = lineas[inicio + 1]
        if not _TELEFONO.search(siguiente) and _ESTADO_FINAL.search(_sin_acentos(siguiente).lower()):
            despues = siguiente
    if despues and not re.search(r"\d|\b(?:tel|cel)\b", despues, re.IGNORECASE):
        ciudad, estado = _lugar(despues)
        if ciudad and not direccion.get("ciudad"):
            direccion["ciudad"] = ciudad
        if estado and not direccion.get("estado"):
            direccion["estado"] = estado
    if cp_suelto and not any(direccion.get(clave) for clave in ("calle_numero", "colonia", "ciudad")):
        return {}  # Un número de 5 dígitos solo no es una dirección.
    return {clave: valor for clave, valor in direccion.items() if valor}


def _campos_etiquetados(lineas: list[str]) -> dict[str, str]:
    """Valores de los renglones "Etiqueta: valor" (también etiqueta y valor en dos renglones).

    Una etiqueta vacía ("NO. INTERIOR:") queda como "" para que no se rellene con
    lo que adivine la lectura libre.
    """
    campos: dict[str, str] = {}
    referencias: list[str] = []
    for i, linea in enumerate(lineas):
        leida = _etiqueta(linea)
        if not leida:
            continue
        campo, valor = leida
        if valor is None:
            siguiente = lineas[i + 1] if i + 1 < len(lineas) else ""
            valor = "" if not siguiente or _etiqueta(siguiente) else _limpiar(siguiente)
        if campo == "referencias":
            if valor:
                prefijo = "Entre calles " if normalizar(linea).startswith("entre") else ""
                referencias.append(prefijo + valor)
            continue
        if campo == "nombre" and valor and telefono_diez_digitos(valor) and not re.search(r"[A-Za-z]{3}", valor):
            campo = "telefono"  # "Nombre: 33 1234 5678"
        elif campo == "telefono" and valor and not telefono_diez_digitos(valor) and _parece_nombre(valor):
            campo = "nombre"  # "Contacto: Ana López"
        if campo == "telefono":
            valor = telefono_diez_digitos(valor)
        if campo == "interior" and valor:
            etiqueta = _INTERIOR_CON_ETIQUETA.match(_limpiar(re.split(r"[:：]", linea)[0]))
            if etiqueta and not normalizar(valor).startswith(normalizar(etiqueta[1])):
                valor = f"{etiqueta[1].capitalize()} {valor}"
        if campo in campos and campos[campo]:
            continue  # se queda el primero
        campos[campo] = valor
    if referencias:
        campos["referencias"] = ". ".join(referencias)
    return campos


def leer_texto(texto: str, *, fuente: str = "el texto pegado") -> Lectura:
    """Dirección del texto tal como lo mandó el cliente (WhatsApp, correo o chat)."""
    lineas = _solo_destinatario(_lineas(texto))
    etiquetados = _campos_etiquetados(lineas)
    libres = [linea for linea in lineas if not _etiqueta(linea)]
    direccion: dict[str, str] = {}

    direccion["nombre"] = etiquetados.get("nombre") or _nombre_libre(lineas)
    if not direccion["nombre"]:
        # Correo con bloque "DESTINATARIO": el nombre es el primer renglón sin etiqueta.
        direccion["nombre"] = next((linea for linea in libres if _parece_nombre(linea)), "")

    con_etiquetas = sum(1 for campo in _CAMPOS_DE_DIRECCION if campo in etiquetados) >= 2
    if not con_etiquetas:
        direccion.update(_direccion_libre(lineas, direccion["nombre"]))
    if etiquetados.get("direccion"):
        # "Dirección: Calle 1 # 2, Col. Centro, C.P. 64000, Monterrey, NL" en un renglón.
        for clave, valor in _direccion_libre([etiquetados["direccion"]], direccion["nombre"]).items():
            direccion.setdefault(clave, valor) if con_etiquetas else direccion.update({clave: valor})
        if not direccion.get("calle_numero") and not _CP.search(etiquetados["direccion"]):
            direccion["calle_numero"] = etiquetados["direccion"]

    calle = etiquetados.get("calle")
    if calle is not None:
        direccion["calle_numero"] = calle
    exterior = etiquetados.get("exterior", "")
    if exterior:
        calle = direccion.get("calle_numero", "")
        if not re.search(rf"(?<!\w){re.escape(exterior)}(?!\w)", calle):
            direccion["calle_numero"] = _limpiar(f"{calle} {exterior}") if calle else ""
    for campo in ("colonia", "ciudad", "municipio", "interior", "correo", "referencias", "pais"):
        if campo in etiquetados:
            direccion[campo] = etiquetados[campo]
    if "colonia" in etiquetados:
        direccion["colonia"] = re.sub(r"^(?:col(?:onia)?\.?)\s+", "", direccion["colonia"], flags=re.IGNORECASE)
    if "codigo_postal" in etiquetados:
        valor = etiquetados["codigo_postal"]
        cp = re.search(r"[0-9Oo]{5}", valor)
        direccion["codigo_postal"] = cp[0].replace("O", "0").replace("o", "0") if cp else ""
        resto = _limpiar(valor[cp.end():]) if cp else ""
        if resto and not re.search(r"\d", resto):  # "C.P.: 44100 Guadalajara, Jal."
            ciudad, estado = _lugar(resto)
            direccion.setdefault("ciudad", ciudad)
            if estado:
                direccion.setdefault("estado", estado)
    if etiquetados.get("ciudad"):
        ciudad, estado = _lugar(etiquetados["ciudad"])
        if estado:  # "Ciudad: Guadalajara, Jalisco"
            direccion["ciudad"] = ciudad or etiquetados["ciudad"]
            direccion.setdefault("estado", estado)
    if "estado" in etiquetados:
        direccion["estado"] = nombre_estado(etiquetados["estado"])
    if not direccion.get("ciudad") and direccion.get("municipio"):
        direccion["ciudad"] = direccion["municipio"]

    telefonos = list(dict.fromkeys(
        re.sub(r"\D", "", m[1]) for linea in lineas for m in _TELEFONO.finditer(linea)
    ))
    direccion["telefono"] = etiquetados.get("telefono") or (telefonos[0] if telefonos else "")
    if not direccion.get("correo"):
        # "Nombre <correo>" es el encabezado de un correo (quién lo mandó), no el cliente.
        correo = _CORREO.search(re.sub(r"<[^>]*>", " ", " ".join(lineas)))
        direccion["correo"] = correo[0] if correo else ""
    direccion = {clave: _limpiar(valor) for clave, valor in direccion.items() if _limpiar(valor)}

    reconocido = any(direccion.get(clave) for clave in ("calle_numero", "codigo_postal", "colonia")) or (
        direccion.get("telefono") and direccion.get("nombre")
    )
    if not reconocido:
        return Lectura(fuente=fuente, motivo="no se reconoció una dirección de envío")
    direccion.setdefault("pais", "México")
    return Lectura(direccion=direccion, fuente=fuente)


# ---------------------------------------------------------------------------
# Archivos: factura, nota de venta, PDF con texto o captura
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def _idioma_ocr() -> str:
    idiomas = subprocess.run(["tesseract", "--list-langs"], capture_output=True, timeout=5, check=True)
    return "spa+eng" if "spa" in idiomas.stdout.decode("utf-8", errors="replace").splitlines() else "eng"


def _ocr(imagen) -> str:
    """OCR en el servidor con tesseract; la imagen no se manda a servicios externos."""
    from PIL import Image, ImageOps, ImageStat

    if imagen.width * imagen.height > 20_000_000:
        raise ValueError("imagen demasiado grande")
    gris = ImageOps.grayscale(ImageOps.exif_transpose(imagen))
    if ImageStat.Stat(gris).mean[0] < 128:  # modo oscuro de WhatsApp
        gris = ImageOps.invert(gris)
    gris = ImageOps.autocontrast(gris)
    escala = min(3.0, max(1.0, 1800 / max(gris.size)))
    gris = gris.resize((int(gris.width * escala), int(gris.height * escala)), Image.Resampling.LANCZOS)
    preparado = BytesIO()
    gris.save(preparado, format="PNG")
    resultado = subprocess.run(
        ["tesseract", "stdin", "stdout", "-l", _idioma_ocr(), "--psm", "6"],
        input=preparado.getvalue(), capture_output=True, timeout=30, check=True,
    )
    return resultado.stdout.decode("utf-8", errors="replace")


def extraer_texto_imagen(contenido: bytes) -> str:
    from PIL import Image

    with Image.open(BytesIO(contenido)) as imagen:
        return _ocr(imagen)


def _texto_pdf(contenido: bytes) -> tuple[str, bool]:
    """(texto, escaneado). Un PDF escaneado se lee con OCR de su primera página."""
    import pdfplumber

    with pdfplumber.open(BytesIO(contenido)) as pdf:
        texto = "\n".join(pagina.extract_text() or "" for pagina in pdf.pages[:MAX_PAGES])
        if re.search(r"[A-Za-z0-9]{2}", texto) or not pdf.pages:
            return texto, False
        imagen = pdf.pages[0].to_image(resolution=200).original
    return _ocr(imagen), True


MOTIVO_SIN_OCR = "no se pudo leer la imagen; pega el texto o captura los datos a mano"


def leer_archivo(nombre: str, contenido: bytes) -> Lectura:
    """Factura TD&S, nota de venta, PDF con texto o captura de pantalla."""
    from PIL import Image, UnidentifiedImageError

    extension = _extension(nombre)
    if extension in IMAGE_EXTENSIONS:
        try:
            texto = extraer_texto_imagen(contenido)
        except (UnidentifiedImageError, Image.DecompressionBombError, OSError, ValueError, subprocess.SubprocessError):
            return Lectura(fuente=nombre, motivo=MOTIVO_SIN_OCR)
        return leer_texto(texto, fuente=f"la captura {nombre}")
    if extension != ".pdf":
        return Lectura(fuente=nombre, motivo="sólo se leen PDF e imágenes (JPG, PNG, WebP)")
    factura = leer_factura_pdf(contenido)
    if factura:
        return factura
    try:
        texto, escaneado = _texto_pdf(contenido)
    except (OSError, ValueError, subprocess.SubprocessError):
        return Lectura(fuente=nombre, motivo="es un PDF escaneado y no se pudo leer; pega el texto")
    except Exception:
        return Lectura(fuente=nombre, motivo="no se pudo abrir el PDF")
    nota = parsear_nota_venta(texto)
    if nota:
        return nota
    return leer_texto(texto, fuente=f"{'el PDF escaneado' if escaneado else 'el PDF'} {nombre}")


def combinar_lecturas(lecturas: list[Lectura]) -> tuple[Lectura | None, list[str]]:
    """Una sola dirección de varios archivos, más los avisos para revisar.

    Manda la factura (o el primero que traiga dirección); los demás sólo completan
    campos vacíos y si son del mismo destinatario. El folio sale de las facturas.
    """
    avisos = [f"No se leyó {lectura.fuente}: {lectura.motivo}." for lectura in lecturas if lectura.motivo]
    con_datos = sorted((l for l in lecturas if l.direccion), key=lambda l: not l.es_factura)
    if not con_datos:
        return None, avisos
    base = con_datos[0]
    direccion = dict(base.direccion)
    for otra in con_datos[1:]:
        nombre, otro_nombre = normalizar(direccion.get("nombre", "")), normalizar(otra.direccion.get("nombre", ""))
        misma_calle = normalizar(direccion.get("calle_numero", "")) == normalizar(otra.direccion.get("calle_numero", ""))
        if nombre and otro_nombre and nombre != otro_nombre and not misma_calle:
            avisos.append(f"{otra.fuente.capitalize()} es de otro destinatario ({otra.direccion['nombre']}); "
                          f"se usó {base.fuente}.")
            continue
        for clave, valor in otra.direccion.items():
            direccion.setdefault(clave, valor)
    folios = list(dict.fromkeys(l.folio for l in lecturas if l.folio))
    fuentes = " y ".join(dict.fromkeys(l.fuente for l in con_datos))
    return Lectura(direccion=direccion, folio=" / ".join(folios), fuente=fuentes, es_factura=base.es_factura), avisos
