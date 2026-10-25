"""Immutable local static-image uploads, without multipart or user filenames."""
from __future__ import annotations

import hashlib
from pathlib import Path
from uuid import UUID, uuid4

from fastapi import APIRouter, Request
from fastapi.responses import FileResponse
from starlette.concurrency import run_in_threadpool

from .artifacts import _publish_file, _safe_path
from .db import connect
from .domain import DomainError
from .media import MAX_UPLOAD_BYTES, STATIC_FORMATS, decode_image, read_image

INPUT_FIELDS = ("id", "relative_path", "content_fingerprint", "media_type", "width", "height", "byte_count")


def resolve_input(conn, input_id: UUID | str, artifact_root: Path) -> dict:
    try:
        identifier = UUID(str(input_id))
    except (ValueError, TypeError, AttributeError) as error:
        raise DomainError("NOT_FOUND", "Input does not exist", 404) from error
    row = conn.execute("SELECT id,relative_path,content_fingerprint,media_type,width,height,byte_count FROM uploaded_inputs WHERE id=%s", (identifier,)).fetchone()
    if row is None:
        raise DomainError("NOT_FOUND", "Input does not exist", 404)
    item = {key: row[key] for key in INPUT_FIELDS}
    item["id"] = str(item["id"])
    try:
        extension = {"image/png": "png", "image/jpeg": "jpg"}[item["media_type"]]
        if item["id"] != str(identifier) or item["relative_path"] != f"inputs/{identifier}.{extension}":
            raise ValueError("Input path does not belong to this identifier")
        target = _safe_path(artifact_root, item["relative_path"])
        if not 0 < item["byte_count"] <= MAX_UPLOAD_BYTES or target.stat().st_size != item["byte_count"]:
            raise ValueError("Input byte count differs from its reference")
        read_image(artifact_root, item)
    except (ValueError, KeyError, TypeError, OSError) as error:
        raise DomainError("INPUT_UNAVAILABLE", "Uploaded input is missing, invalid or corrupted", 409) from error
    return item


def store_input(conn, content: bytes, media_type: str, artifact_root: Path) -> dict:
    """Publish a candidate before inserting its reference in the caller's transaction."""
    try:
        image = decode_image(content, media_type)
    except ValueError as error:
        raise DomainError("INVALID_INPUT", str(error), 422) from error
    identifier = uuid4()
    extension = "png" if media_type == "image/png" else "jpg"
    item = {"id": str(identifier), "relative_path": f"inputs/{identifier}.{extension}",
            "content_fingerprint": hashlib.sha256(content).hexdigest(), "media_type": media_type,
            "width": image.width, "height": image.height, "byte_count": len(content)}
    _publish_file(artifact_root, item["relative_path"], content)
    conn.execute("""INSERT INTO uploaded_inputs(id,relative_path,content_fingerprint,media_type,width,height,byte_count)
        VALUES (%s,%s,%s,%s,%s,%s,%s)""", tuple(item[key] for key in INPUT_FIELDS))
    return item


async def read_upload(request: Request) -> bytes:
    content = bytearray()
    async for chunk in request.stream():
        if len(content) + len(chunk) > MAX_UPLOAD_BYTES:
            raise DomainError("INPUT_TOO_LARGE", "Upload must not exceed 8 MiB", 413)
        content.extend(chunk)
    return bytes(content)


def build_input_router(settings) -> APIRouter:
    router = APIRouter(prefix="/api/v1/inputs", tags=["inputs"])

    def save(content: bytes, media_type: str) -> dict:
        with connect(settings) as conn:
            return store_input(conn, content, media_type, settings.artifact_root)

    @router.post("", status_code=201)
    async def upload(request: Request):
        media_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
        if media_type not in STATIC_FORMATS:
            raise DomainError("UNSUPPORTED_MEDIA_TYPE", "Only image/png and image/jpeg raw bodies are supported", 415)
        content = await read_upload(request)
        return await run_in_threadpool(save, content, media_type)

    @router.get("/{input_id}")
    def metadata(input_id: UUID):
        with connect(settings) as conn:
            return resolve_input(conn, input_id, settings.artifact_root)

    @router.get("/{input_id}/content")
    def content(input_id: UUID):
        with connect(settings) as conn:
            item = resolve_input(conn, input_id, settings.artifact_root)
        path = _safe_path(settings.artifact_root, item["relative_path"])
        return FileResponse(path, media_type=item["media_type"], filename=path.name,
                            headers={"X-Content-Type-Options": "nosniff"})

    return router
