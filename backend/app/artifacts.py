"""Immutable, attempt-scoped JSON files and committed-batch manifests.

Files are candidates until the repository accepts their metadata under its lease
and fencing checks.  In particular, finding a file is never evidence of a commit.
Artifact ``metadata`` is the manifest itself, not a nested wrapper.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path, PurePosixPath, PureWindowsPath
import re
from typing import Any
from uuid import UUID, uuid4


def _component(value: Any) -> str:
    value = str(value)
    if (
        not value
        or value in {".", ".."}
        or any(char in value for char in '/\\:<>"|?*')
        or value.endswith((".", " "))
        or any(ord(char) < 32 for char in value)
    ):
        raise ValueError("Unsafe artifact path component")
    return value


def _safe_path(root: Path, relative_path: str) -> Path:
    if not isinstance(relative_path, str) or not relative_path:
        raise ValueError("Artifact path must be a nonempty relative path")
    path = PurePosixPath(relative_path)
    if (
        path.is_absolute()
        or PureWindowsPath(relative_path).drive
        or "\\" in relative_path
        or any(part in {"", ".", ".."} for part in relative_path.split("/"))
    ):
        raise ValueError("Artifact path must stay beneath the artifact root")
    for part in path.parts:
        _component(part)
    base = Path(root).resolve()
    target = base.joinpath(*path.parts)
    if not target.resolve().is_relative_to(base):
        raise ValueError("Artifact path escapes the artifact root")
    return target


def _json_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _write_json_file(
    root: Path, claim: dict[str, Any], label: str, payload: dict[str, Any],
) -> tuple[str, str, str]:
    return _write_file(root, claim, label, _json_bytes(payload), "json")


def _write_file(
    root: Path, claim: dict[str, Any], label: str, content: bytes, extension: str,
) -> tuple[str, str, str]:
    identifier = str(uuid4())
    parts = [_component(claim[key]) for key in ("task_id", "step_key", "attempt_id")]
    relative_path = "/".join([*parts, f"{identifier}-{_component(label)}.{_component(extension)}"])
    _publish_file(root, relative_path, content)
    return identifier, relative_path, hashlib.sha256(content).hexdigest()


def _publish_file(root: Path, relative_path: str, content: bytes) -> None:
    """Atomically publish bytes without replacing any existing immutable file."""
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    target = _safe_path(root, relative_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    # Recheck after creating directories so an existing escaping symlink cannot
    # redirect a write. The artifact volume is not writable by untrusted users.
    target = _safe_path(root, relative_path)
    temporary = target.with_name(f".{uuid4()}.tmp")
    try:
        with temporary.open("xb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        # A hard-link publication is atomic and exclusive on both Windows and
        # POSIX. Unlike replace(), it can never overwrite a published file.
        os.link(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)


def write_json_artifact(
    root: Path, claim: dict[str, Any], kind: str, payload: dict[str, Any],
) -> dict[str, Any]:
    """Write a candidate checkpoint/output; only the repository can commit it."""
    if kind not in {"checkpoint", "output"}:
        raise ValueError("Unsupported artifact kind")
    identifier, relative_path, fingerprint = _write_json_file(root, claim, kind, payload)
    # Copy via JSON to avoid later mutation of a caller's manifest changing the
    # metadata while the immutable file retains its original contents.
    return {
        "id": identifier,
        "kind": kind,
        "relative_path": relative_path,
        "content_fingerprint": fingerprint,
        "metadata": json.loads(_json_bytes(payload)),
    }


def write_batch_artifact(
    root: Path, claim: dict[str, Any], index: int, payload: dict[str, Any],
) -> dict[str, Any]:
    """A batch is registered only through a subsequently committed manifest."""
    if type(index) is not int or index < 0:
        raise ValueError("Invalid batch index")
    _, relative_path, fingerprint = _write_json_file(root, claim, f"batch-{index}", payload)
    return {
        "index": index,
        "relative_path": relative_path,
        "content_fingerprint": fingerprint,
    }


def _read_verified(root: Path, reference: dict[str, Any]) -> bytes:
    if not isinstance(reference, dict):
        raise ValueError("Invalid artifact reference")
    fingerprint = reference.get("content_fingerprint")
    if not isinstance(fingerprint, str) or not re.fullmatch(r"[0-9a-f]{64}", fingerprint):
        raise ValueError("Invalid artifact fingerprint")
    target = _safe_path(root, reference.get("relative_path"))
    try:
        content = target.read_bytes()
    except OSError as error:
        raise ValueError("Artifact file is unavailable") from error
    if hashlib.sha256(content).hexdigest() != fingerprint:
        raise ValueError("Artifact fingerprint does not match its file")
    return content


def _validate_batches(root: Path, manifest: dict[str, Any], *, complete: bool) -> None:
    if type(manifest.get("schema_version")) is not int or manifest["schema_version"] != 1:
        raise ValueError("Unsupported manifest schema")
    if not isinstance(manifest.get("compatibility_key"), str) or not manifest["compatibility_key"]:
        raise ValueError("Missing checkpoint compatibility key")
    total = manifest.get("total_batches")
    batches = manifest.get("completed_batches")
    if type(total) is not int or total < 1 or not isinstance(batches, list):
        raise ValueError("Invalid batch manifest")
    if len(batches) > total or (complete and len(batches) != total):
        raise ValueError("Invalid completed batch count")
    paths: set[str] = set()
    for index, batch in enumerate(batches):
        if not isinstance(batch, dict) or type(batch.get("index")) is not int or batch["index"] != index:
            raise ValueError("Committed batches must be an ordered contiguous prefix")
        content = _read_verified(root, batch)
        if manifest.get("media_pipeline"):
            from .media import validate_frame_batch

            validate_frame_batch(root, manifest, batch, content)
        if batch["relative_path"] in paths:
            raise ValueError("Duplicate batch file")
        paths.add(batch["relative_path"])


def _load_artifact(root: Path, artifact: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(artifact, dict) or artifact.get("kind") not in {"checkpoint", "output"}:
        raise ValueError("Invalid artifact kind")
    try:
        UUID(str(artifact["id"]))
    except (KeyError, ValueError, TypeError, AttributeError) as error:
        raise ValueError("Invalid artifact identifier") from error
    content = _read_verified(root, artifact)
    try:
        manifest = json.loads(content)
    except (ValueError, UnicodeError) as error:
        raise ValueError("Artifact is not a JSON manifest") from error
    if not isinstance(manifest, dict) or artifact.get("metadata") != manifest:
        raise ValueError("Artifact metadata must equal the file manifest directly")
    if artifact["kind"] == "checkpoint" or "completed_batches" in manifest:
        _validate_batches(root, manifest, complete=artifact["kind"] == "output")
    from .media import validate_media_references

    validate_media_references(root, artifact, manifest)
    return manifest


def validate_artifact(root: Path, artifact: dict[str, Any]) -> bool:
    """Return True or raise ValueError, including for missing/corrupted batches."""
    _load_artifact(root, artifact)
    return True


def read_checkpoint(
    root: Path, artifact: dict[str, Any], compatibility_key: str,
    *, total_batches: int | None = None,
) -> dict[str, Any]:
    """Recover only a caller-supplied committed, compatible batch prefix."""
    if not isinstance(artifact, dict) or artifact.get("kind") != "checkpoint":
        raise ValueError("Expected a checkpoint artifact")
    manifest = _load_artifact(root, artifact)
    if manifest["compatibility_key"] != compatibility_key:
        raise ValueError("Checkpoint is incompatible with this execution")
    if total_batches is not None and manifest["total_batches"] != total_batches:
        raise ValueError("Checkpoint batch count is incompatible")
    return manifest
