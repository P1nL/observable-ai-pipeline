"""Small, binding-scoped wire contract shared by the supervisor and executor."""

from __future__ import annotations

import base64
from datetime import datetime, timezone
import hashlib
import hmac
import json
from typing import Any
from uuid import UUID


BINDING_FIELDS = ("executor_id", "job_id", "task_id", "step_id", "attempt_id", "generation", "worker_id")
STATES = {"STARTING", "RUNNING", "EXITED", "UNCERTAIN"}
PROTOCOL_VERSION = 1
REQUIRED_CAPABILITIES = {"durable-start-id", "bound-auth", "renewable-lease", "confirmed-exit",
                         "ordered-events", "shared-artifacts-v1", "committed-checkpoint-v1"}


def validate_capabilities(data: Any, binding: dict, claim: dict) -> None:
    """Refuse incompatible dispatch; no job is started by this handshake."""
    if not isinstance(data, dict) or type(data.get("protocol_version")) is not int or data["protocol_version"] != PROTOCOL_VERSION:
        raise ValueError("Unsupported external protocol version")
    if data.get("executor_id") != binding["executor_id"]:
        raise ValueError("Capabilities belong to another executor")
    for field in ("capabilities", "step_keys", "input_kinds"):
        if not isinstance(data.get(field), list) or not all(isinstance(item, str) for item in data[field]):
            raise ValueError("Invalid executor capabilities")
    versions = data.get("artifact_schema_versions")
    named = data.get("named_step_processors", [])
    if not isinstance(named, list) or not all(isinstance(item, str) for item in named):
        raise ValueError("Invalid named-step processor capabilities")
    processors = data.get("processors")
    spec = claim["spec"]
    if (not REQUIRED_CAPABILITIES.issubset(data["capabilities"])
            or not isinstance(versions, list) or not all(type(v) is int for v in versions) or 1 not in versions
            or not isinstance(processors, dict) or not isinstance(processors.get(spec["processor"]), list)
            or spec["processor_version"] not in processors[spec["processor"]]
            or (claim["step_key"] not in data["step_keys"]
                and spec["processor"] not in named)
            or claim["input"].get("kind") not in data["input_kinds"]):
        raise ValueError("Executor does not support this step, input, version or required guarantees")


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def utc_time(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("Expected an absolute UTC ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset().total_seconds() != 0:
            raise ValueError("Expected UTC")
        return parsed.astimezone(timezone.utc)
    except (ValueError, OverflowError, AttributeError) as error:
        raise ValueError("Expected an absolute UTC ISO timestamp") from error


def validate_binding(value: Any) -> dict:
    base = set(BINDING_FIELDS)
    if not isinstance(value, dict) or set(value) not in (base, base | {"node_id", "resource_id"}):
        raise ValueError("Incomplete execution binding")
    binding = dict(value)
    for key in ("task_id", "step_id", "attempt_id", "job_id"):
        try:
            binding[key] = str(UUID(str(binding[key])))
        except (ValueError, TypeError, AttributeError) as error:
            raise ValueError("Invalid execution UUID") from error
    if binding["job_id"] != binding["attempt_id"]:
        raise ValueError("A job is bound to exactly one Attempt")
    if type(binding["generation"]) is not int or binding["generation"] < 1:
        raise ValueError("Invalid execution generation")
    for key in ("executor_id", "worker_id"):
        item = binding[key]
        if not isinstance(item, str) or not item or len(item) > 128 or any(ord(char) < 32 for char in item):
            raise ValueError("Invalid execution identity")
    if "node_id" in binding:
        import re
        if any(not isinstance(binding[k], str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", binding[k]) for k in ("node_id", "resource_id")) or binding["node_id"] == "legacy":
            raise ValueError("Invalid node/resource execution binding")
    return binding


def binding_for(claim: dict, executor_id: str | None = None) -> dict:
    """Also accept UUID objects from repository cleanup rows, never change owner."""
    try:
        return validate_binding({
            "executor_id": executor_id if executor_id is not None else claim["executor_id"],
            "job_id": claim.get("external_job_id") or claim["attempt_id"],
            **{key: claim[key] for key in ("task_id", "step_id", "attempt_id", "generation", "worker_id")},
            **({"node_id": claim["node_id"], "resource_id": claim["resource_device_id"]} if claim.get("node_id", "legacy") != "legacy" else {}),
        })
    except (KeyError, TypeError) as error:
        raise ValueError("Incomplete execution claim") from error


def _secret(secret: str) -> bytes:
    if not isinstance(secret, str) or len(secret) < 32:
        raise ValueError("Executor secret must contain at least 32 characters")
    return secret.encode("utf-8")


def auth_headers(secret: str, binding: dict, *, scope: str = "job") -> dict[str, str]:
    if scope not in {"job", "capabilities"}:
        raise ValueError("Unsupported authentication scope")
    raw = canonical_json(validate_binding(binding)).encode("utf-8")
    signed = b"capabilities:" + raw if scope == "capabilities" else raw
    signature = hmac.new(_secret(secret), signed, hashlib.sha256).hexdigest()
    return {
        "Authorization": "Bearer " + signature,
        "X-Execution-Binding": base64.urlsafe_b64encode(raw).decode("ascii"),
        "X-Execution-Protocol": str(PROTOCOL_VERSION),
        **({"X-Execution-Scope": "capabilities"} if scope == "capabilities" else {}),
    }


def authenticate(secret: str, authorization: str, header: str, *, scope: str = "job") -> dict:
    """No body, paths or credentials supplied here become executable commands."""
    try:
        raw = header if header.startswith("{") else base64.b64decode(header, altchars=b"-_", validate=True).decode("utf-8")
        binding = validate_binding(json.loads(raw))
        expected = auth_headers(secret, binding, scope=scope)["Authorization"]
        if not hmac.compare_digest(authorization.encode("utf-8"), expected.encode("utf-8")):
            raise ValueError("Invalid authentication")
        return binding
    except (ValueError, TypeError, UnicodeError) as error:
        raise ValueError("Invalid execution authentication") from error


def validate_event(event: Any) -> dict:
    if not isinstance(event, dict):
        raise ValueError("Invalid executor event")
    kind = event.get("type")
    if not isinstance(kind, str):
        raise ValueError("Invalid executor event type")
    if kind == "progress":
        if not isinstance(event.get("progress"), dict):
            raise ValueError("Invalid progress event")
    elif kind in {"checkpoint", "result"}:
        if not isinstance(event.get("artifact"), dict):
            raise ValueError("Invalid artifact event")
    elif kind == "error":
        if not isinstance(event.get("error_code"), str) or not isinstance(event.get("message"), str) or type(event.get("retryable")) is not bool:
            raise ValueError("Invalid error event")
    else:
        raise ValueError("Unsupported executor event")
    canonical_json(event)
    return event


def validate_status(data: Any, binding: dict) -> dict:
    expected = validate_binding(binding)
    if not isinstance(data, dict) or validate_binding(data.get("binding")) != expected:
        raise ValueError("Executor status belongs to another execution")
    state, confirmed, code = data.get("state"), data.get("exit_confirmed"), data.get("returncode")
    if not isinstance(state, str) or state not in STATES or type(confirmed) is not bool or (code is not None and type(code) is not int):
        raise ValueError("Invalid executor status")
    if (state == "EXITED") != confirmed or (not confirmed and code is not None):
        raise ValueError("Exit status is not confirmed")
    if type(data.get("start_count")) is not int or not 0 <= data["start_count"] <= 1:
        raise ValueError("Invalid start count")
    utc_time(data.get("observed_at"))
    events = data.get("events")
    if not isinstance(events, list):
        raise ValueError("Invalid executor event stream")
    previous = 0
    for item in events:
        if not isinstance(item, dict) or type(item.get("seq")) is not int or item["seq"] <= previous:
            raise ValueError("Executor events must be ordered by sequence")
        validate_event(item.get("event"))
        previous = item["seq"]
    return {**data, "binding": expected}
