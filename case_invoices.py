"""Facturas PDF por orden; el vínculo vive en S3 y no modifica Google Sheets."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from pathlib import PurePosixPath
from typing import Any, Callable
from urllib.parse import quote, unquote
from uuid import NAMESPACE_URL, uuid4, uuid5
from zoneinfo import ZoneInfo

import boto3
import streamlit as st

CASE_TYPES = {"aparatos": "Aparatos", "alineadores": "Alineadores", "polanco": "Polanco"}
ALLOWED_USERS = frozenset({"Admin", "Jime", "Lesly", "Vero"})
SERVICE_SECTION = "📋 Servicio y archivos"
TIMEZONE = ZoneInfo("America/Mexico_City")


def case_prefix(case_type: str, identifier: str) -> str:
    identifier = str(identifier).strip()
    if case_type not in CASE_TYPES or not identifier:
        raise ValueError("Selecciona una orden válida antes de adjuntar facturas.")
    return f"facturas/{case_type}/{quote(identifier, safe='')}/"


def pdf_file(upload: Any) -> tuple[str, bytes]:
    name = PurePosixPath(str(upload.name).replace("\\", "/")).name
    name = "".join(char for char in name if ord(char) >= 32 and ord(char) != 127).strip()
    body = upload.getvalue()
    if not name.lower().endswith(".pdf") or not body or b"%PDF-" not in body[:1024]:
        raise ValueError(f"{name or 'Archivo'}: selecciona una factura en formato PDF.")
    return name, body


def require_unique_case(frame, identifier: str, id_column: str) -> None:
    if id_column not in frame or len(frame[frame[id_column].astype(str).str.strip().eq(str(identifier).strip())]) != 1:
        raise ValueError("La orden ya no existe o su folio está duplicado. Actualiza los casos antes de adjuntar.")


@dataclass(frozen=True)
class Invoice:
    key: str
    name: str
    size: int
    modified: datetime | None = None


@dataclass
class InvoiceStore:
    client: Any
    bucket: str

    def list(self, case_type: str, identifier: str) -> list[Invoice]:
        prefix = case_prefix(case_type, identifier)
        files = []
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for item in page.get("Contents", []):
                key = item["Key"]
                if key.startswith(prefix) and key.lower().endswith(".pdf"):
                    files.append(Invoice(key, unquote(key.rsplit("/", 1)[-1]),
                                         item.get("Size", 0), item.get("LastModified")))
        return sorted(files, key=lambda item: (item.modified.isoformat() if item.modified else "", item.key), reverse=True)

    def upload(self, case_type: str, identifier: str, uploads: list[Any], current_user: str,
               verify_case: Callable[[], None]) -> tuple[list[str], list[str]]:
        if current_user not in ALLOWED_USERS:
            raise ValueError("Tu usuario no tiene permiso para adjuntar facturas.")
        prefix = case_prefix(case_type, identifier)
        # Valida todo el lote y la orden antes de la primera escritura.
        prepared = [(upload, *pdf_file(upload)) for upload in uploads]
        if not prepared:
            raise ValueError("Selecciona al menos una factura PDF.")
        verify_case()
        saved, errors = [], []
        for upload, name, body in prepared:
            # Un reintento del mismo archivo de Streamlit conserva su objeto.
            # Dos selecciones distintas con el mismo nombre tienen IDs distintos.
            file_id = getattr(upload, "file_id", None) or uuid4().hex
            token = uuid5(NAMESPACE_URL, prefix + str(file_id)).hex
            key = f"{prefix}{token}/{quote(name, safe='')}"
            try:
                self.client.put_object(Bucket=self.bucket, Key=key, Body=body, ContentType="application/pdf",
                                       Metadata={"uploaded_by": current_user, "order_id": quote(str(identifier).strip(), safe=''),
                                                 "case_type": case_type})
                saved.append(name)
            except Exception:
                errors.append(f"No se pudo adjuntar {name}. Vuelve a intentar guardar las facturas.")
        return saved, errors

    def url(self, case_type: str, identifier: str, invoice: Invoice, *, download: bool = False) -> str:
        if not invoice.key.startswith(case_prefix(case_type, identifier)) or not invoice.key.lower().endswith(".pdf"):
            raise ValueError("Esta factura no pertenece a la orden seleccionada.")
        disposition = "attachment" if download else "inline"
        return self.client.generate_presigned_url("get_object", Params={
            "Bucket": self.bucket, "Key": invoice.key, "ResponseContentType": "application/pdf",
            "ResponseContentDisposition": f"{disposition}; filename*=UTF-8''{quote(invoice.name, safe='')}",
        }, ExpiresIn=1800)


@st.cache_resource(show_spinner=False)
def get_store() -> InvoiceStore:
    keys = ("aws_access_key_id", "aws_secret_access_key", "aws_region", "s3_bucket_name")
    aws = st.secrets.get("aws", {})
    config = {key: st.secrets.get(key, aws.get(key, "")) for key in keys}
    if not all(config.values()):
        raise ValueError("Falta configurar el almacenamiento de facturas de la app.")
    return InvoiceStore(boto3.client("s3", aws_access_key_id=config["aws_access_key_id"],
                                    aws_secret_access_key=config["aws_secret_access_key"], region_name=config["aws_region"]),
                        config["s3_bucket_name"])


@st.cache_data(ttl=30, show_spinner=False)
def list_invoices(case_type: str, identifier: str) -> list[Invoice]:
    return get_store().list(case_type, identifier)


def render_section(case_type: str, identifier: str, current_user: str, *,
                   verify_case: Callable[[], None], rerun: Callable[[], None]) -> None:
    """Guardar facturas sólo actualiza archivos; los borradores de la ficha se conservan."""
    if current_user not in ALLOWED_USERS:
        return
    token = sha256(f"{case_type}\0{identifier}".encode()).hexdigest()[:16]
    with st.container(horizontal=True, vertical_alignment="center"):
        st.markdown("**🧾 Facturas de esta orden**")
        if st.button("Actualizar facturas", key=f"invoice_refresh_{token}"):
            list_invoices.clear(case_type, identifier)
            rerun()
    version_key = f"invoice_version_{token}"
    feedback_key = f"invoice_feedback_{token}"
    for message in st.session_state.pop(feedback_key, {}).get("errors", []):
        st.error(message)
    try:
        store = get_store()
        invoices = list_invoices(case_type, identifier)
    except Exception:
        st.warning("No se pudieron consultar las facturas. Revisa la configuración de almacenamiento de la app.")
        return
    if not invoices:
        st.caption("Esta orden todavía no tiene facturas adjuntas.")
    else:
        st.caption(f"{len(invoices)} factura(s) adjunta(s)")
    for invoice in invoices:
        with st.container(horizontal=True, vertical_alignment="center"):
            st.write(invoice.name)
            if invoice.modified:
                st.caption(invoice.modified.astimezone(TIMEZONE).strftime("%d/%m/%Y %H:%M"))
            st.link_button("Abrir PDF", store.url(case_type, identifier, invoice), icon="📄")
            st.link_button("Descargar PDF", store.url(case_type, identifier, invoice, download=True), icon="⬇️")
    version = st.session_state.get(version_key, 0)
    uploads = st.file_uploader("Adjuntar facturas PDF", type=["pdf"], accept_multiple_files=True,
                               key=f"invoice_upload_{token}_{version}",
                               help="Puedes adjuntar varias facturas a la misma orden, ahora o en cargas posteriores.")
    if st.button("Guardar facturas", key=f"invoice_save_{token}", disabled=not uploads):
        try:
            saved, errors = store.upload(case_type, identifier, uploads, current_user, verify_case)
        except ValueError as exc:
            st.error(str(exc))
            return
        except Exception:
            st.error("No se pudieron guardar las facturas. Intenta de nuevo.")
            return
        if saved:
            list_invoices.clear(case_type, identifier)
            st.toast(f"{len(saved)} factura(s) adjunta(s) a la orden {identifier}.", icon="🧾")
        st.session_state[feedback_key] = {"errors": errors}
        if not errors:
            st.session_state[version_key] = version + 1
        rerun()
