"""Configured node source and persisted observations, not execution authority.

The source is replaceable at this call boundary. It does not discover nodes,
create Attempts, release resources, or infer business completion from health.
"""
from datetime import datetime, timezone
import hashlib
import json
import re
from uuid import UUID, uuid4

from psycopg.types.json import Jsonb

from .artifacts import _publish_file, _safe_path


NODE_CAPABILITIES = {"node-binding-v1", "shared-storage-inspection-v1"}


def configured_nodes(settings):
    return tuple(settings.execution_nodes)


def resolve_node(settings, node_id, route=None):
    node = next((n for n in configured_nodes(settings) if n.node_id == node_id), None)
    if node is None or (route is not None and (
            set(route) != {*node.route(), "state_id"} or
            any(route.get(key) != value for key, value in node.route().items()))):
        raise ValueError("Original execution node route is not configured; keep ownership quarantined")
    return node


def storage_reference(settings):
    """A protocol consumer proves that both mounts expose the same namespace."""
    relative = "_system/storage-id.json"
    target = _safe_path(settings.artifact_root, relative)
    if not target.exists():
        data = {"id": str(uuid4()), "storage_id": settings.artifact_storage_id}
        content = json.dumps(data, sort_keys=True, separators=(",", ":")).encode()
        try:
            _publish_file(settings.artifact_root, relative, content)
        except FileExistsError:
            pass  # Another Worker atomically published the same root's identity.
    content = target.read_bytes()
    data = json.loads(content)
    if data.get("storage_id") != settings.artifact_storage_id:
        raise ValueError("Artifact root belongs to another storage namespace")
    return {"id": data["id"], "kind": "storage-probe", "relative_path": relative,
            "content_fingerprint": hashlib.sha256(content).hexdigest(), "metadata": data}


def inspection_binding(node, claim=None):
    from .external_protocol import binding_for
    identity = str(uuid4())
    return binding_for({"task_id": str((claim or {}).get("task_id", identity)),
        "step_id": str((claim or {}).get("step_id", identity)), "attempt_id": (claim or {}).get("attempt_id", identity),
        "generation": (claim or {}).get("generation", 1), "worker_id": (claim or {}).get("worker_id", "node-observer"),
        "executor_id": node.executor_id, "node_id": node.node_id, "resource_device_id": "inspection"})


def validate_report(data, node):
    if (not isinstance(data, dict) or data.get("node_id") != node.node_id
            or data.get("executor_id") != node.executor_id or data.get("storage_id") != node.storage_id
            or not isinstance(data.get("capabilities"), list)
            or not all(isinstance(flag, str) for flag in data["capabilities"])
            or not NODE_CAPABILITIES.issubset(data.get("capabilities", []))):
        raise ValueError("Node identity/storage/capability mismatch")
    if not isinstance(data.get("state_id"), str):
        raise ValueError("Missing executor state-store identity")
    UUID(data["state_id"])
    if (type(data.get("cpu_capacity")) is not int or data["cpu_capacity"] < 0
            or type(data.get("cpu_in_use")) is not int or data["cpu_in_use"] < 0):
        raise ValueError("Invalid CPU scheduling observation")
    slots = data.get("mock_gpu_slots")
    if not isinstance(slots, list):
        raise ValueError("Missing mock device observations")
    seen = set()
    for slot in slots:
        device = slot.get("device_id") if isinstance(slot, dict) else None
        if (not isinstance(device, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", device)
                or device in seen or slot.get("state") not in {"AVAILABLE", "RUNNING", "QUARANTINED"}):
            raise ValueError("Invalid or duplicated mock device observation")
        seen.add(device)
    if type(data.get("references_readable")) is not bool:
        raise ValueError("Missing shared-storage inspection result")
    return data


def public_capabilities(data):
    keys = {"protocol_version", "node_id", "executor_id", "storage_id", "state_id", "capabilities", "processors",
            "step_keys", "named_step_processors", "input_kinds", "artifact_schema_versions", "cpu_capacity",
            "cpu_in_use", "mock_gpu_slots", "references_readable", "inspection_error"}
    return {key: value for key, value in data.items() if key in keys}


def sync_nodes(conn, settings):
    ids = [n.node_id for n in configured_nodes(settings)]
    conn.execute("UPDATE execution_nodes SET configured=false,accepting=false WHERE node_id<>'legacy' AND NOT(node_id=ANY(%s))", (ids,))
    for node in configured_nodes(settings):
        old = conn.execute("SELECT * FROM execution_nodes WHERE node_id=%s FOR NO KEY UPDATE", (node.node_id,)).fetchone()
        changed = old is None or any(old[k] != v for k, v in {
            "executor_id": node.executor_id, "origin": node.url, "accepting": node.accepting,
            "cpu_capacity": node.cpu_capacity, "storage_id": node.storage_id, "processors": node.processors}.items())
        conn.execute("""INSERT INTO execution_nodes(node_id,executor_id,origin,accepting,cpu_capacity,storage_id,processors)
            VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(node_id) DO UPDATE SET
            executor_id=excluded.executor_id,origin=excluded.origin,accepting=excluded.accepting,
            cpu_capacity=excluded.cpu_capacity,storage_id=excluded.storage_id,processors=excluded.processors,configured=true""",
            (node.node_id, node.executor_id, node.url, node.accepting, node.cpu_capacity, node.storage_id, Jsonb(node.processors)))
        conn.execute("UPDATE resource_slots SET enabled=false WHERE node_id=%s AND NOT(device_id=ANY(%s))", (node.node_id, node.mock_gpu_slots))
        for device in node.mock_gpu_slots:
            conn.execute("""INSERT INTO resource_slots(node_id,device_id,kind,state) VALUES (%s,%s,'mock-gpu','AVAILABLE')
                ON CONFLICT(node_id,device_id) DO UPDATE SET enabled=true""", (node.node_id, device))
        if changed:
            conn.execute("INSERT INTO node_events(node_id,type,payload) VALUES (%s,'NODE_CONFIGURED',%s)",
                (node.node_id, Jsonb({"accepting": node.accepting, "cpu_capacity": node.cpu_capacity, "devices": node.mock_gpu_slots})))


def observe(conn, node, data=None, error=None):
    old = conn.execute("SELECT * FROM execution_nodes WHERE node_id=%s FOR NO KEY UPDATE", (node.node_id,)).fetchone()
    if old is None:
        return
    status = "ONLINE" if data is not None else "OFFLINE"
    conn.execute("""UPDATE execution_nodes SET status=%s,last_error=%s,last_probe_at=clock_timestamp(),
        last_seen_at=CASE WHEN %s THEN clock_timestamp() ELSE last_seen_at END,
        capabilities=CASE WHEN %s THEN %s ELSE capabilities END WHERE node_id=%s""",
        (status, error, data is not None, data is not None, Jsonb(public_capabilities(data or {})), node.node_id))
    if old["status"] != status or old["last_error"] != error:
        conn.execute("INSERT INTO node_events(node_id,type,payload) VALUES (%s,%s,%s)",
            (node.node_id, "NODE_ONLINE" if data is not None else "NODE_UNREACHABLE", Jsonb({"error": error})))


def read_nodes(conn, settings):
    rows = conn.execute("SELECT * FROM execution_nodes WHERE node_id<>'legacy' ORDER BY node_id").fetchall()
    result = []
    for row in rows:
        item = dict(row)
        if item["last_probe_at"] and (datetime.now(timezone.utc)-item["last_probe_at"]).total_seconds() > settings.node_probe_interval_seconds * 3:
            item["status"] = "STALE"
        count = conn.execute("""SELECT count(*) AS n FROM step_attempts a JOIN steps s ON s.id=a.step_id
            WHERE a.node_id=%s AND s.spec->>'resource_kind'='cpu'
            AND (a.status='RUNNING' OR (a.resource_cleanup_state='CPU_PROCESS_UNCONFIRMED' AND a.process_exit_confirmed_at IS NULL))""", (row["node_id"],)).fetchone()["n"]
        item["cpu"] = {"capacity": min(row["cpu_capacity"], row["capabilities"].get("cpu_capacity", row["cpu_capacity"])),
                       "configured_capacity": row["cpu_capacity"], "in_use": max(count, row["capabilities"].get("cpu_in_use", 0))}
        slots = conn.execute("""SELECT r.*,s.task_id,s.step_key,a.worker_id FROM resource_slots r
            LEFT JOIN step_attempts a ON a.id=r.owner_attempt_id LEFT JOIN steps s ON s.id=a.step_id
            WHERE r.node_id=%s ORDER BY r.device_id""", (row["node_id"],)).fetchall()
        reports = {s["device_id"]: s for s in row["capabilities"].get("mock_gpu_slots", [])}
        item["slots"] = []
        for slot in slots:
            value = {k: str(v) if type(v).__name__ == "UUID" else v for k, v in slot.items()}
            reported = reports.get(slot["device_id"], {})
            value["observed_state"] = reported.get("state")
            if value["state"] == "AVAILABLE" and reported.get("state", "AVAILABLE") != "AVAILABLE":
                value["state"] = "QUARANTINED"  # Observation does NOT mutate/free authoritative ownership.
                value["reported_owner_attempt_id"] = reported.get("owner_attempt_id")
            item["slots"].append(value)
        item["events"] = [dict(e) for e in conn.execute("SELECT seq,type,payload,created_at FROM node_events WHERE node_id=%s ORDER BY seq DESC LIMIT 10", (row["node_id"],)).fetchall()]
        result.append(item)
    return result
