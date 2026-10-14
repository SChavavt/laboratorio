"""Facturas PDF por orden; el vínculo vive en S3 y no modifica Google Sheets."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
import logging
from pathlib import PurePosixPath
from typing import Any, Callable
from urllib.parse import quote, unquote
from uuid import NAMESPACE_URL, uuid4, uuid5
from zoneinfo import ZoneInfo

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, ConnectionError as BotoConnectionError, HTTPClientError
import streamlit as st
from streamlit.errors import StreamlitSecretNotFoundError
from user_permissions import APP_USERS, ORDER_EDIT_USERS, can_edit_orders

CASE_TYPES = {"aparatos": "Aparatos", "alineadores": "Alineadores", "polanco": "Polanco"}
ALLOWED_USERS = ORDER_EDIT_USERS
VIEW_USERS = frozenset(APP_USERS)
SERVICE_SECTION = "📋 Servicio y archivos"
TIMEZONE = ZoneInfo("America/Mexico_City")
AWS_KEYS = ("aws_access_key_id", "aws_secret_access_key", "aws_region", "s3_bucket_name")
LOGGER = logging.getLogger(__name__)


class StorageConfigurationError(ValueError):
    def __init__(self, missing: tuple[str, ...]):
        super().__init__("Falta configurar el almacenamiento de facturas de la app.")
        self.missing = missing


def storage_error(error: Exception, operation: str) -> tuple[str, str]:
    """Mensajes seguros: nunca mostrar la respuesta AWS ni valores de Secrets."""
    if isinstance(error, StorageConfigurationError):
        return ("Falta configurar el almacenamiento de facturas. Pide al administrador revisar la configuración de la app.",
                "Configura en Streamlit Secrets, en la raíz o en [aws]: " + ", ".join(error.missing) + ".")
    if isinstance(error, ClientError):
        code = error.response.get("Error", {}).get("Code", "")
        if code in {"AccessDenied", "AllAccessDisabled"}:
            permission = "s3:ListBucket" if operation == "consultar" else "s3:PutObject"
            return (f"La app no tiene permiso para {operation} las facturas. Pide al administrador revisar el acceso al almacenamiento.",
                    f"AWS rechazó la operación ({code}). Revisa {permission} para el prefijo facturas/; "
                    "para abrir los PDFs también se necesita s3:GetObject.")
        if code in {"InvalidAccessKeyId", "SignatureDoesNotMatch", "ExpiredToken", "InvalidToken", "TokenRefreshRequired"}:
            return ("AWS rechazó las credenciales del almacenamiento. Pide al administrador revisar las claves de la app.",
                    f"AWS: {code}. Revisa las claves en Streamlit Secrets y aws_session_token si son temporales.")
        if code == "NoSuchBucket":
            return ("No se encontró el almacenamiento configurado. Pide al administrador revisar la configuración de la app.",
                    "AWS: NoSuchBucket. Revisa s3_bucket_name en Streamlit Secrets.")
        if code in {"PermanentRedirect", "IncorrectEndpoint", "AuthorizationHeaderMalformed", "IllegalLocationConstraintException"}:
            return ("La ubicación del almacenamiento no coincide con la configuración de la app.",
                    f"AWS: {code}. Revisa que aws_region sea la región del bucket configurado.")
    if isinstance(error, (BotoConnectionError, HTTPClientError)):
        return ("No se pudo conectar al almacenamiento de facturas. Intenta actualizar las facturas en unos momentos.",
                "Revisa la conexión de la app a S3 y la región configurada.")
    return (f"No se pudieron {operation} las facturas. Intenta de nuevo; si continúa, avisa al administrador.",
            "Revisa los registros del despliegue y la configuración de S3.")


def show_storage_error(error: Exception, operation: str, current_user: str) -> None:
    message, detail = storage_error(error, operation)
    # La clase de error ayuda a localizar el fallo sin registrar secretos ni datos de la orden.
    LOGGER.warning("Invoice storage operation %s failed (%s)", operation, type(error).__name__)
    st.warning(message)
    if current_user == "Admin":
        st.caption(detail)


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
            except Exception as exc:
                message, detail = storage_error(exc, "guardar")
                LOGGER.warning("Invoice storage operation guardar failed (%s)", type(exc).__name__)
                errors.append(f"No se pudo adjuntar {name}. {message}" +
                              (f" {detail}" if current_user == "Admin" else ""))
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
    try:
        aws = st.secrets.get("aws", {})
        aws = aws if isinstance(aws, Mapping) else {}
        config = {}
        for key in (*AWS_KEYS, "aws_session_token"):
            root = str(st.secrets.get(key, "") or "").strip()
            config[key] = root or str(aws.get(key, "") or "").strip()
    except (StreamlitSecretNotFoundError, FileNotFoundError):
        raise StorageConfigurationError(AWS_KEYS) from None
    missing = tuple(key for key in AWS_KEYS if not config[key])
    if missing:
        raise StorageConfigurationError(missing)
    credentials = {"aws_session_token": config["aws_session_token"]} if config["aws_session_token"] else {}
    return InvoiceStore(boto3.client("s3", aws_access_key_id=config["aws_access_key_id"],
                                    aws_secret_access_key=config["aws_secret_access_key"], region_name=config["aws_region"],
                                    config=Config(signature_version="s3v4"), **credentials),
                        config["s3_bucket_name"])


@st.cache_data(ttl=30, show_spinner=False)
def list_invoices(case_type: str, identifier: str) -> list[Invoice]:
    return get_store().list(case_type, identifier)


def render_section(case_type: str, identifier: str, current_user: str, *,
                   verify_case: Callable[[], None], rerun: Callable[[], None]) -> None:
    """Guardar facturas sólo actualiza archivos; los borradores de la ficha se conservan."""
    if current_user not in VIEW_USERS:
        return
    token = sha256(f"{case_type}\0{identifier}".encode()).hexdigest()[:16]
    with st.container(horizontal=True, vertical_alignment="center"):
        st.markdown("**🧾 Facturas de esta orden**")
        if st.button("Actualizar facturas", key=f"invoice_refresh_{token}"):
            list_invoices.clear(case_type, identifier)
            rerun()
    version_key = f"invoice_version_{token}"
    feedback_key = f"invoice_feedback_{token}"
    feedback = st.session_state.pop(feedback_key, {})
    if feedback.get("saved"):
        st.success(f"{len(feedback['saved'])} factura(s) guardada(s) en esta orden.")
    for message in feedback.get("errors", []):
        st.error(message)
    try:
        store = get_store()
    except Exception as exc:
        show_storage_error(exc, "consultar", current_user)
        st.caption("La consulta falló; no se puede comprobar qué facturas tiene esta orden.")
        return
    try:
        invoices = list_invoices(case_type, identifier)
    except Exception as exc:
        show_storage_error(exc, "consultar", current_user)
        st.caption("La consulta falló; no se puede comprobar qué facturas tiene esta orden. "
                   "Si ya guardaste una factura, actualiza la lista cuando se restablezca el acceso.")
        invoices = None
    if invoices == []:
        st.caption("Esta orden todavía no tiene facturas adjuntas.")
    elif invoices:
        st.caption(f"{len(invoices)} factura(s) adjunta(s)")
    for invoice in invoices or []:
        with st.container(horizontal=True, vertical_alignment="center"):
            st.write(invoice.name)
            if invoice.modified:
                st.caption(invoice.modified.astimezone(TIMEZONE).strftime("%d/%m/%Y %H:%M"))
            st.link_button("Abrir PDF", store.url(case_type, identifier, invoice), icon="📄")
            st.link_button("Descargar PDF", store.url(case_type, identifier, invoice, download=True), icon="⬇️")
    if not can_edit_orders(current_user):
        return
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
        except Exception as exc:
            show_storage_error(exc, "guardar", current_user)
            return
        if saved:
            list_invoices.clear(case_type, identifier)
            st.toast(f"{len(saved)} factura(s) adjunta(s) a la orden {identifier}.", icon="🧾")
        st.session_state[feedback_key] = {"saved": saved, "errors": errors}
        if not errors:
            st.session_state[version_key] = version + 1
        rerun()
