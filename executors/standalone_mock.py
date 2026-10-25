"""Independent synthetic-only executor for SPEC 12/13/14; Python 3.12 + psutil 7.

Run with python -I /absolute/path/standalone_mock.py --executor-id NAME
--artifact-root PATH --state-dir PATH --port 8766. No central application imports.
The secret is read from --secret-env and never given to the --compute child.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import ipaddress
import json
import math
import os
from pathlib import Path
import re
import signal
import socket
import sqlite3
import ssl
import subprocess
import sys
import threading
import time
import unicodedata
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit
from uuid import UUID, uuid4

# The fixed compute program needs only stdlib. On Windows, use the base Python
# executable rather than a venv redirector so the recorded PID IS the computation.
if sys.argv[1:] != ["--compute"]:
    import psutil

COMPUTE_PYTHON = getattr(sys, "_base_executable", sys.executable) if os.name == "nt" else sys.executable

VERSION = "independent-mock-v1"
STEPS = ("normalize_asset", "segment_subject", "analyze_motion", "generate_frames",
         "encode_package", "publish_result")
SAFE_STEP_KEY = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}\Z")
CAPABILITIES = ["durable-start-id", "bound-auth", "renewable-lease", "confirmed-exit",
                "ordered-events", "shared-artifacts-v1", "committed-checkpoint-v1"]
BINDING_KEYS = {"executor_id", "job_id", "task_id", "step_id", "attempt_id", "generation", "worker_id"}
NODE_BINDING_KEYS = {"node_id", "resource_id"}
NODE_CLAIM_KEYS = {"node_id", "resource_device_id", "artifact_storage_id", "executor_state_id"}
CLAIM_KEYS = (BINDING_KEYS - {"job_id"}) | {
    "external_job_id", "execution_backend", "step_key", "spec", "attempt_number",
    "recovery_round", "input", "input_asset", "compatibility_key", "dependencies",
    "checkpoint", "runtime_policy", "retry_policy", "lease_expires_at", "started_at",
    "first_started_at", "created_at", "deadline_at", "resource_slot_id"}
SPEC_KEYS = {"key", "processor", "processor_version", "depends_on", "parameters",
             "cache_enabled", "checkpoint_mode", "resource_kind"}
PARAM_KEYS = {"duration_seconds", "batch_duration_seconds", "batch_count",
              "frames_per_batch", "fail_first_attempts", "seed"}
RUNTIME_KEYS = {"heartbeat_interval_seconds", "lease_ttl_seconds",
                "coordinator_scan_interval_seconds", "attempt_hard_timeout_seconds",
                "task_deadline_seconds", "step_soft_warning_seconds", "task_soft_warning_seconds"}
FINGERPRINT = re.compile(r"[0-9a-f]{64}\Z")
HERE = Path(__file__).resolve()


class Rejection(Exception):
    def __init__(self, status: int, detail: str):
        super().__init__(detail)
        self.status, self.detail = status, detail


def require(condition, detail, status=422):
    if not condition:
        raise Rejection(status, detail)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=True, allow_nan=False).encode("utf-8")


def read_json(raw):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, "Duplicate JSON member")
            result[key] = value
        return result
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=unique)
        canonical(value)  # Reject non-finite JSON, including overflowing numbers.
        return value
    except (ValueError, UnicodeError, TypeError, RecursionError) as exc:
        raise Rejection(422, "Expected finite UTF-8 JSON") from exc


def exact(value, keys, label):
    require(isinstance(value, dict) and set(value) == keys,
            f"{label} must contain exactly: {', '.join(sorted(keys))}")


def integer(value, minimum=0):
    return type(value) is int and value >= minimum


def number(value, minimum=0):
    return type(value) in (int, float) and math.isfinite(value) and value >= minimum


def uuid_text(value):
    try:
        return isinstance(value, str) and str(UUID(value)) == value
    except ValueError:
        return False


def identity(value):
    return (isinstance(value, str) and bool(value) and
            not any(unicodedata.category(c) == "Cc" for c in value))


def utc(value):
    require(isinstance(value, str) and value.endswith(("Z", "+00:00")),
            "Expected an absolute UTC ISO8601 timestamp")
    try:
        parsed = datetime.fromisoformat(value)
        require(parsed.utcoffset().total_seconds() == 0, "Expected UTC")
        return parsed.timestamp()
    except (ValueError, OverflowError, AttributeError) as exc:
        raise Rejection(422, "Invalid UTC timestamp") from exc


def now_text():
    return datetime.now(timezone.utc).isoformat()


def validate_binding(binding):
    require(isinstance(binding, dict) and set(binding) in (BINDING_KEYS, BINDING_KEYS | NODE_BINDING_KEYS),
            "Invalid binding fields")
    require(all(uuid_text(binding[k]) for k in ("job_id", "task_id", "step_id", "attempt_id")),
            "Binding UUIDs must be canonical lowercase")
    require(binding["job_id"] == binding["attempt_id"] and integer(binding["generation"], 1),
            "Invalid Attempt binding")
    require(identity(binding["executor_id"]) and identity(binding["worker_id"]),
            "Invalid executor or worker identity")
    if "node_id" in binding:
        require(all(isinstance(binding[k], str) and SAFE_STEP_KEY.fullmatch(binding[k])
                    for k in NODE_BINDING_KEYS), "Invalid node/resource identity")


def validate_claim(claim, binding):
    node_mode = "node_id" in binding
    exact(claim, CLAIM_KEYS | NODE_CLAIM_KEYS if node_mode else CLAIM_KEYS, "Claim")
    expected = {k: claim[k] for k in BINDING_KEYS - {"job_id"}}
    expected["job_id"] = claim["external_job_id"]
    if node_mode:
        expected.update(node_id=claim["node_id"], resource_id=claim["resource_device_id"])
    require(canonical(expected) == canonical(binding), "Claim does not match authenticated binding", 403)
    require(claim["execution_backend"] == "external", "Only external execution is supported")
    spec = claim["spec"]
    exact(spec, SPEC_KEYS, "Step specification")
    key = claim["step_key"]
    dynamic = spec["processor"] == "mock-step"
    require(isinstance(key, str) and (SAFE_STEP_KEY.fullmatch(key) if dynamic else key in STEPS)
            and spec["key"] == key, "Unsupported step")
    require(spec["processor"] in ("mock", "mock-step") and spec["processor_version"] == VERSION,
            f"Only mock and mock-step / {VERSION} are supported; no legacy smoke processor")
    require(isinstance(claim["input"], dict) and claim["input"].get("kind") == "synthetic"
            and claim["input_asset"] is None,
            "Only synthetic JSON input is supported; uploaded/media input is unsupported")
    require(isinstance(claim["compatibility_key"], str) and bool(claim["compatibility_key"]),
            "A compatibility_key is required")
    require(integer(claim["attempt_number"], 1) and integer(claim["recovery_round"]),
            "Invalid attempt_number or recovery_round")
    require(type(spec["cache_enabled"]) is bool, "cache_enabled must be boolean")
    require(not dynamic or not spec["cache_enabled"], "mock-step does not support cross-task cache")
    batched = dynamic or key == "generate_frames"
    resource_valid = (spec["resource_kind"] in ("cpu", "mock-gpu") if dynamic else
                      spec["resource_kind"] == ("mock-gpu" if batched else "cpu"))
    require(resource_valid and
            spec["checkpoint_mode"] == ("committed-batch-manifest" if batched else "none"),
            "Unsupported resource_kind or checkpoint_mode")
    require(integer(claim["resource_slot_id"], 1) if spec["resource_kind"] == "mock-gpu" else claim["resource_slot_id"] is None,
            "Invalid resource_slot_id")
    require(claim["checkpoint"] is None or batched, "Step does not support checkpoints")
    dependencies = spec["depends_on"]
    require(isinstance(dependencies, list) and
            all(isinstance(k, str) and (SAFE_STEP_KEY.fullmatch(k) if dynamic else k in STEPS)
                and k != key for k in dependencies), "Invalid depends_on")
    require(len(set(dependencies)) == len(dependencies), "Duplicate dependency step")
    require(not dynamic or len(dependencies) <= 1, "mock-step accepts only one serial predecessor")
    require(isinstance(claim["dependencies"], list), "dependencies must be an artifact array")
    if not dynamic and key in ("encode_package", "publish_result"):
        require(dependencies == (["generate_frames"] if key == "encode_package" else ["encode_package"]),
                "Package/publication requires its documented single upstream step")
    parameters = spec["parameters"]
    require(isinstance(parameters, dict) and set(parameters) <= (PARAM_KEYS - {"seed"} if dynamic else PARAM_KEYS),
            "Unsupported calculation parameter")
    for field in ("duration_seconds", "batch_duration_seconds"):
        if field in parameters:
            require(number(parameters[field]), f"Invalid {field}")
    for field in ("batch_count", "frames_per_batch", "fail_first_attempts", "seed"):
        if field in parameters:
            require(integer(parameters[field], 1 if field in ("batch_count", "frames_per_batch") else 0),
                    f"Invalid {field}")
            if dynamic and field in ("batch_count", "frames_per_batch"):
                require(parameters[field] <= 1000, f"{field} exceeds mock-step limit 1000")
    runtime = claim["runtime_policy"]
    require(isinstance(runtime, dict) and RUNTIME_KEYS <= set(runtime), "Incomplete runtime_policy")
    for field in RUNTIME_KEYS:
        value = runtime[field]
        optional = field not in ("heartbeat_interval_seconds", "lease_ttl_seconds", "coordinator_scan_interval_seconds")
        require((optional and value is None) or number(value), f"Invalid {field}")
    require(isinstance(claim["retry_policy"], dict), "retry_policy must be a frozen JSON object")
    for field in ("lease_expires_at", "started_at", "first_started_at", "created_at", "deadline_at"):
        if claim[field] is not None:
            utc(claim[field])


def shared_path(root, relative):
    require(isinstance(relative, str) and "\\" not in relative and ":" not in relative,
            "Invalid shared artifact path")
    parts = relative.split("/")
    require(len(parts) == 4 and all(p not in ("", ".", "..") for p in parts),
            "Artifact paths must be task/step/attempt/UUID-label.json")
    require(uuid_text(parts[0]) and SAFE_STEP_KEY.fullmatch(parts[1]) and uuid_text(parts[2]) and
            uuid_text(parts[3][:36]) and parts[3][36:37] == "-" and
            parts[3].endswith(".json") and len(parts[3]) > 42, "Invalid artifact namespace")
    candidate = root.joinpath(*parts)
    require(candidate.resolve().is_relative_to(root), "Artifact symlink escapes shared root")
    return candidate


def read_blob(path, reference):
    require(isinstance(reference, dict), "Invalid artifact reference")
    digest = reference.get("content_fingerprint")
    require(isinstance(digest, str) and FINGERPRINT.fullmatch(digest), "Invalid artifact fingerprint")
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise Rejection(422, "Committed artifact is not readable") from exc
    require(hashlib.sha256(raw).hexdigest() == digest, "Committed artifact fingerprint mismatch")
    return raw


def read_reference(root, reference):
    require(isinstance(reference, dict), "Invalid artifact reference")
    raw = read_blob(shared_path(root, reference.get("relative_path")), reference)
    data = read_json(raw)
    require(isinstance(data, dict), "Artifact JSON must be an object")
    return data


def batches(root, data, total=None, step_key=None):
    step_key = step_key or data.get("step_key", "generate_frames")
    require(type(data.get("schema_version")) is int and data["schema_version"] == 1,
            "Unsupported checkpoint/batch manifest schema")
    require(isinstance(data.get("compatibility_key"), str) and data["compatibility_key"],
            "Batch manifest lacks compatibility_key")
    require(integer(data.get("total_batches"), 1) and
            (total is None or data["total_batches"] == total), "Batch count mismatch")
    refs = data.get("completed_batches")
    require(isinstance(refs, list) and len(refs) <= data["total_batches"], "Invalid batch prefix")
    seen = set()
    for index, ref in enumerate(refs):
        exact(ref, {"index", "relative_path", "content_fingerprint"}, "Batch reference")
        require(type(ref["index"]) is int and ref["index"] == index, "Non-contiguous batch prefix")
        path = ref["relative_path"]
        require(isinstance(path, str) and path not in seen, "Duplicate batch path")
        require(path.split("/")[1:2] == [step_key], "Batch is outside its step namespace")
        seen.add(path)
        read_reference(root, ref)
    return refs


def short_reference(artifact):
    return {key: artifact[key] for key in ("id", "relative_path", "content_fingerprint")}


def validate_manifest(root, manifest):
    require(isinstance(manifest, dict) and manifest.get("source") == "mock" and
            manifest.get("format") == "mock-animation-package-v1", "Invalid result manifest")
    frames = manifest.get("frame_manifests")
    require(isinstance(frames, list) and len(frames) == 1, "Expected one generate_frames manifest")
    ref = frames[0]
    require(isinstance(ref, dict) and uuid_text(ref.get("id")), "Invalid frame manifest identity")
    data = read_reference(root, ref)
    require(data.get("step_key") == "generate_frames" and
            isinstance(data.get("processor_version"), str) and data["processor_version"],
            "Incompatible frame manifest")
    require(len(batches(root, data)) == data["total_batches"], "Incomplete frame manifest")


def read_artifact(root, artifact, kind):
    required = {"id", "kind", "relative_path", "content_fingerprint", "metadata"}
    require(isinstance(artifact, dict) and required <= set(artifact) and
            uuid_text(artifact.get("id")) and artifact.get("kind") == kind, "Invalid artifact envelope")
    data = read_reference(root, artifact)
    require(data == artifact["metadata"], "Artifact metadata differs from file content")
    if kind == "checkpoint":
        batches(root, data, step_key=artifact["relative_path"].split("/")[1])
    else:
        key = data.get("step_key")
        # Older generic producers did not tag the processor. Batch shape, not the
        # business key or producer version, distinguishes their schema-1 output.
        dynamic = (data.get("processor") == "mock-step" or
                   (data.get("processor") is None and "total_batches" in data and "completed_batches" in data))
        require(isinstance(key, str) and (SAFE_STEP_KEY.fullmatch(key) if dynamic else key in STEPS)
                and artifact["relative_path"].split("/")[1] == key,
                "Output step/namespace mismatch")
        resource_valid = (data.get("resource_kind") in ("cpu", "mock-gpu") if dynamic else
                          data.get("resource_kind") == ("mock-gpu" if key == "generate_frames" else "cpu"))
        require(type(data.get("schema_version")) is int and data["schema_version"] == 1 and
                data.get("source") == "mock" and data.get("execution_resource") == "cpu" and
                isinstance(data.get("processor_version"), str) and data["processor_version"] and resource_valid,
                "Incompatible MOCK output")
        require(isinstance(data.get("compatibility_key"), str) and data["compatibility_key"] and
                isinstance(data.get("input"), dict) and data["input"].get("kind") == "synthetic" and
                integer(data.get("seed")) and integer(data.get("completed_units")) and
                integer(data.get("resumed_batches")), "Missing MOCK output fields")
        require(isinstance(data.get("dependencies"), list) and
                all(isinstance(d, str) and FINGERPRINT.fullmatch(d) for d in data["dependencies"]),
                "Invalid upstream fingerprints")
        if dynamic or key == "generate_frames":
            require(len(batches(root, data)) == data["total_batches"], "Incomplete frame output")
        if not dynamic and key in ("encode_package", "publish_result"):
            validate_manifest(root, data.get("result_manifest"))
        if not dynamic and key == "publish_result":
            ref = data.get("encoded_artifact")
            require(isinstance(ref, dict) and uuid_text(ref.get("id")), "Invalid encoded_artifact")
            encoded = read_reference(root, ref)
            require(encoded.get("step_key") == "encode_package" and
                    encoded.get("result_manifest") == data["result_manifest"] and
                    data.get("publication") == "local-only-mock", "Invalid local publication")
    return data


def committed_inputs(root, claim):
    by_step = {}
    for artifact in claim["dependencies"]:
        data = read_artifact(root, artifact, "output")
        key = data["step_key"]
        require(key not in by_step, "Duplicate dependency artifact")
        by_step[key] = artifact
    require(set(by_step) == set(claim["spec"]["depends_on"]), "Committed dependencies do not match depends_on")
    prefix = []
    if claim["checkpoint"] is not None:
        data = read_artifact(root, claim["checkpoint"], "checkpoint")
        require(data["compatibility_key"] == claim["compatibility_key"], "Checkpoint compatibility mismatch")
        require(claim["checkpoint"]["relative_path"].split("/")[1] == claim["step_key"], "Wrong checkpoint step")
        default_count = 1 if claim["spec"]["processor"] == "mock-step" else 3
        prefix = batches(root, data, claim["spec"]["parameters"].get("batch_count", default_count), claim["step_key"])
    return by_step, prefix


def write_artifact(root, claim, label, data, kind="output"):
    artifact_id = str(uuid4())
    relative = f"{claim['task_id']}/{claim['step_key']}/{claim['attempt_id']}/{artifact_id}-{label}.json"
    final = shared_path(root, relative)
    final.parent.mkdir(parents=True, exist_ok=True)
    require(final.parent.resolve().is_relative_to(root), "Artifact directory escapes shared root")
    temporary = final.with_name(f".{artifact_id}.tmp")
    raw = canonical(data)
    try:
        with temporary.open("xb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        # Atomic no-replace publication on NTFS/POSIX; never overwrite old files.
        os.link(temporary, final)
        if os.name != "nt":
            fd = os.open(final.parent, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
    finally:
        temporary.unlink(missing_ok=True)
    return {"id": artifact_id, "kind": kind, "relative_path": relative,
            "content_fingerprint": hashlib.sha256(raw).hexdigest(), "metadata": data}


def emit(event):
    sys.stdout.buffer.write(canonical(event) + b"\n")
    sys.stdout.buffer.flush()


def compute(payload):
    exact(payload, {"claim", "artifact_root"}, "Internal compute payload")
    claim = payload["claim"]
    root = Path(payload["artifact_root"]).resolve()
    upstream, prefix = committed_inputs(root, claim)
    dynamic = claim["spec"]["processor"] == "mock-step"
    # Computation-local defaults: never amend the stored immutable claim/fingerprint.
    params = {"duration_seconds": 0.05, "batch_count": 1 if dynamic else 3, "frames_per_batch": 1,
              "fail_first_attempts": 0, "seed": claim["input"].get("seed", 0),
              **claim["spec"]["parameters"]}
    params.setdefault("batch_duration_seconds", 0.1 if dynamic else params["duration_seconds"] / params["batch_count"])
    key = claim["step_key"]
    fingerprints = [upstream[k]["content_fingerprint"] for k in sorted(upstream)]
    resumed = len(prefix)
    common = {"schema_version": 1, "source": "mock", "execution_resource": "cpu",
              "resource_kind": claim["spec"]["resource_kind"], "compatibility_key": claim["compatibility_key"],
              "step_key": key, "processor_version": VERSION, "input": claim["input"],
              "seed": params["seed"], "dependencies": fingerprints,
              "completed_units": 1, "resumed_batches": resumed}
    if dynamic:
        common["processor"] = "mock-step"  # Business keys never select animation semantics.
        time.sleep(params["duration_seconds"])
    seed = int(hashlib.sha256(canonical([claim["input"], params["seed"], fingerprints])).hexdigest()[:16], 16)
    inject = claim["attempt_number"] <= params["fail_first_attempts"]

    def injected_failure():
        emit({"type": "error", "error_code": "TRANSIENT_ERROR", "retryable": True,
              "message": "Injected MOCK failure; committed candidates remain available to the center"})
        return 1

    if dynamic or key == "generate_frames":
        total = params["batch_count"]
        completed = list(prefix)  # Preserve supplied committed references verbatim.
        for index in range(resumed, total):
            time.sleep(params["batch_duration_seconds"])
            batch = dict(common, batch_index=index, completed_units=params["frames_per_batch"],
                         frames=[{"index": index * params["frames_per_batch"] + frame,
                                  "value": (seed + (index * params["frames_per_batch"] + frame) * 37) % 65536}
                                 for frame in range(params["frames_per_batch"])])
            artifact = write_artifact(root, claim, f"batch-{index}", batch)
            completed.append({"index": index, "relative_path": artifact["relative_path"],
                              "content_fingerprint": artifact["content_fingerprint"]})
            checkpoint = {"schema_version": 1, "compatibility_key": claim["compatibility_key"],
                          "total_batches": total, "completed_batches": list(completed)}
            emit({"type": "checkpoint", "artifact": write_artifact(root, claim, "checkpoint", checkpoint, "checkpoint")})
            emit({"type": "progress", "progress": {"completed_units": len(completed), "total_units": total,
                  "resumed_batches": resumed, "percent": 100 * len(completed) / total}})
            if inject and (len(completed) - resumed >= 3 or len(completed) == total):
                return injected_failure()
        if inject:
            return injected_failure()  # Full prefix: no fabricated batch/progress/checkpoint.
        output = dict(common, total_batches=total, completed_batches=completed, completed_units=total)
    else:
        time.sleep(params["duration_seconds"])
        if inject:
            return injected_failure()
        output = dict(common)
        if key == "normalize_asset":
            output["normalized_points"] = [[(seed + i * 17) % 256, (seed // 256 + i * 29) % 256] for i in range(8)]
        elif key == "segment_subject":
            output["subject_mask"] = [(seed >> i) & 1 for i in range(16)]
        elif key == "analyze_motion":
            output["motion_vectors"] = [[(seed + i) % 9 - 4, (seed // 9 + i * 2) % 9 - 4] for i in range(8)]
        elif key == "encode_package":
            output["result_manifest"] = {"source": "mock", "format": "mock-animation-package-v1",
                                         "frame_manifests": [short_reference(upstream["generate_frames"])]}
        else:
            encoded = upstream["encode_package"]
            output.update(result_manifest=encoded["metadata"]["result_manifest"],
                          encoded_artifact=short_reference(encoded), publication="local-only-mock")
        emit({"type": "progress", "progress": {"completed_units": 1, "total_units": 1,
                                                "resumed_batches": 0, "percent": 100}})
    emit({"type": "result", "artifact": write_artifact(root, claim, "result", output)})
    return 0


class DirectoryLock:
    """One owning service per durable state directory, including across ports."""
    def __init__(self, path):
        self.stream = path.open("a+b")
        self.stream.seek(0)
        self.stream.write(b"0")
        self.stream.flush()
        self.stream.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(self.stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.stream.close()
            raise RuntimeError("The state directory already has an owning service") from None

    def close(self):
        self.stream.close()


class Executor:
    def __init__(self, executor_id, secret, artifact_root, state_dir, *, node_id=None,
                 storage_id="pipeline-artifacts", cpu_capacity=2, mock_gpu_slots=("gpu-0",)):
        require(identity(executor_id), "Invalid executor identity")
        require(isinstance(secret, str) and len(secret) >= 32, "Executor secret must have at least 32 characters")
        require(node_id is None or (isinstance(node_id, str) and SAFE_STEP_KEY.fullmatch(node_id)
                and SAFE_STEP_KEY.fullmatch(executor_id)), "Invalid configured node identity")
        require(identity(storage_id) and integer(cpu_capacity, 0), "Invalid storage identity or CPU capacity")
        require(isinstance(mock_gpu_slots, (list, tuple)) and
                all(isinstance(device, str) and SAFE_STEP_KEY.fullmatch(device) and device not in ("cpu", "inspection")
                    for device in mock_gpu_slots) and len(set(mock_gpu_slots)) == len(mock_gpu_slots),
                "Invalid or duplicate MOCK GPU device name")
        self.node_id, self.storage_id = node_id, storage_id
        self.cpu_capacity, self.mock_gpu_slots = cpu_capacity, tuple(mock_gpu_slots)
        self.executor_id, self.secret = executor_id, secret.encode("utf-8")
        self.root, self.directory = Path(artifact_root).resolve(), Path(state_dir).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.owner = DirectoryLock(self.directory / "owner.lock")
        self.lock = threading.RLock()
        self.host = socket.gethostname()
        self.live = {}
        self.closed = threading.Event()
        self.db = sqlite3.connect(self.directory / "commands.sqlite3", check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS settings (name TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS jobs (
                job_id TEXT PRIMARY KEY, binding TEXT NOT NULL, claim TEXT,
                command_fingerprint TEXT, lease REAL NOT NULL, hard_deadline REAL,
                state TEXT NOT NULL, start_count INTEGER NOT NULL,
                stop_requested INTEGER NOT NULL DEFAULT 0, pid INTEGER, process_created REAL,
                host TEXT, returncode INTEGER, stream_lost INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS events (
                job_id TEXT NOT NULL REFERENCES jobs(job_id), seq INTEGER NOT NULL,
                event TEXT NOT NULL, PRIMARY KEY(job_id, seq));
        """)
        try:
            with self.db:
                existing = self.db.execute("SELECT 1 FROM settings LIMIT 1").fetchone()
                node_setting = self.db.execute("SELECT value FROM settings WHERE name='node_id'").fetchone()
                if existing and node_setting is None and node_id is not None:
                    raise RuntimeError("Legacy state directory cannot be adopted by a configured node")
                for name, value in (("executor_id", executor_id), ("artifact_root", str(self.root)),
                                    ("node_id", node_id or ""), ("storage_id", storage_id)):
                    row = self.db.execute("SELECT value FROM settings WHERE name=?", (name,)).fetchone()
                    if row and row[0] != value:
                        raise RuntimeError(f"State directory {name} does not match deployment")
                    self.db.execute("INSERT OR IGNORE INTO settings VALUES (?,?)", (name, value))
                self.db.execute("INSERT OR IGNORE INTO settings VALUES ('state_id',?)", (str(uuid4()),))
                self.state_id = self.db.execute("SELECT value FROM settings WHERE name='state_id'").fetchone()[0]
                # Restart cannot recover the old stdout pipe. Reconcile before serving.
                self.db.execute("UPDATE jobs SET state='UNCERTAIN', stop_requested=1, stream_lost=1 "
                                "WHERE state!='EXITED'")
            self._tick()
        except Exception:
            self.db.close()
            self.owner.close()
            raise
        self.watchdog = threading.Thread(target=self._watch, name="executor-watchdog", daemon=True)
        self.watchdog.start()

    def _resources(self):
        cpu_in_use = 0
        slots = {device: {"device_id": device, "state": "AVAILABLE", "owner_attempt_id": None,
                          "owner_generation": None} for device in self.mock_gpu_slots}
        for row in self.db.execute("SELECT * FROM jobs WHERE state!='EXITED'"):
            binding = json.loads(row["binding"])
            resource = binding["resource_id"]
            if resource == "cpu":
                cpu_in_use += 1
            else:
                uncertain = row["state"] != "RUNNING" or row["stop_requested"] or row["lease"] <= time.time()
                slots[resource] = {"device_id": resource, "state": "QUARANTINED" if uncertain else "RUNNING",
                                   "owner_attempt_id": binding["attempt_id"], "owner_generation": binding["generation"]}
        return cpu_in_use, list(slots.values())

    def capabilities(self):
        result = {"protocol_version": 1, "executor_id": self.executor_id,
                "capabilities": list(CAPABILITIES), "processors": {"mock": [VERSION], "mock-step": [VERSION]},
                "named_step_processors": ["mock-step"],
                "step_keys": list(STEPS), "input_kinds": ["synthetic"], "artifact_schema_versions": [1]}
        if self.node_id is not None:
            with self.lock:
                cpu, slots = self._resources()
            result["capabilities"] += ["node-binding-v1", "shared-storage-inspection-v1"]
            result.update(node_id=self.node_id, storage_id=self.storage_id, state_id=self.state_id, cpu_capacity=self.cpu_capacity,
                          cpu_in_use=cpu, mock_gpu_slots=slots)
        return result

    def inspect_storage(self, body):
        exact(body, {"storage_id", "references"}, "Storage inspection")
        require(identity(body["storage_id"]) and isinstance(body["references"], list), "Invalid storage inspection")
        readable = body["storage_id"] == self.storage_id
        try:
            if readable:
                for reference in body["references"]:
                    require(isinstance(reference, dict), "Invalid storage reference")
                    kind = reference.get("kind")
                    if kind in ("output", "checkpoint"):
                        read_artifact(self.root, reference, kind)
                        continue
                    require(kind in ("input", "storage-probe"), "Unsupported storage reference kind")
                    relative = reference.get("relative_path")
                    require(identity(relative) and "\\" not in relative and ":" not in relative,
                            "Invalid inspected path")
                    parts = relative.split("/")
                    require(all(part not in ("", ".", "..") for part in parts), "Invalid inspected path")
                    require((kind == "storage-probe" and relative == "_system/storage-id.json") or
                            (kind == "input" and parts[0] != "_system"), "Invalid storage management path")
                    path = self.root.joinpath(*parts)
                    require(path.resolve().is_relative_to(self.root), "Inspected path escapes shared root")
                    raw = read_blob(path, reference)
                    if kind == "storage-probe":
                        data = read_json(raw)
                        require(isinstance(data, dict) and data.get("storage_id") == self.storage_id
                                and identity(data.get("id")), "Shared storage namespace mismatch")
                        if "metadata" in reference:
                            require(reference["metadata"] == data, "Storage marker metadata mismatch")
        except (Rejection, OSError, ValueError, RuntimeError):
            readable = False
        result = self.capabilities()
        result["references_readable"] = readable
        if not readable:
            result["inspection_error"] = "ARTIFACT_UNAVAILABLE"
        return result

    def authenticate(self, headers, job_id=None, *, capabilities=False):
        require(headers.get("X-Execution-Protocol") == "1", "Only execution protocol 1 is supported", 426)
        media_type = headers.get("Content-Type", "").lower().replace(" ", "")
        require(media_type in ("application/json", "application/json;charset=utf-8"),
                "Content-Type must be application/json UTF-8")
        scope = headers.get("X-Execution-Scope")
        require(scope is None or scope == "capabilities", "Unsupported execution scope", 403)
        require(scope != "capabilities" or (capabilities and job_id is None),
                "Capabilities-only credentials cannot authorize job actions", 403)
        try:
            encoded = headers.get("X-Execution-Binding", "")
            raw = base64.b64decode(encoded.encode("ascii"), altchars=b"-_", validate=True)
            binding = read_json(raw)
            validate_binding(binding)
            require(canonical(binding) == raw and base64.urlsafe_b64encode(raw).decode("ascii") == encoded,
                    "Noncanonical binding", 401)
            message = b"capabilities:" + raw if scope == "capabilities" else raw
            expected = "Bearer " + hmac.new(self.secret, message, hashlib.sha256).hexdigest()
            require(hmac.compare_digest(headers.get("Authorization", "").encode("utf-8"), expected.encode("ascii")),
                    "Invalid authentication", 401)
        except (Rejection, ValueError, TypeError, UnicodeError) as exc:
            raise Rejection(401, "Invalid Attempt authentication") from exc
        require(binding["executor_id"] == self.executor_id and
                (job_id is None or binding["job_id"] == job_id), "Executor/path binding mismatch", 403)
        require(("node_id" in binding) == (self.node_id is not None) and
                binding.get("node_id") == self.node_id, "Configured node binding mismatch", 403)
        if binding.get("resource_id") == "inspection":
            require(capabilities and scope == "capabilities", "Inspection resource cannot authorize jobs", 403)
        with self.lock:
            self._bound(binding)
        return binding

    def _bound(self, binding):
        row = self.db.execute("SELECT * FROM jobs WHERE job_id=?", (binding["job_id"],)).fetchone()
        if row:
            require(row["binding"] == canonical(binding).decode(), "Persisted binding mismatch", 403)
        return row

    def _status(self, row, after=0, include_events=False):
        events = []
        if include_events:
            events = [{"seq": e[0], "event": json.loads(e[1])} for e in self.db.execute(
                "SELECT seq,event FROM events WHERE job_id=? AND seq>? ORDER BY seq", (row["job_id"], after))]
        return {"binding": json.loads(row["binding"]), "state": row["state"],
                "exit_confirmed": row["state"] == "EXITED", "returncode": row["returncode"],
                "start_count": row["start_count"], "observed_at": now_text(), "events": events, "state_id": self.state_id}

    def status(self, binding, after=0):
        with self.lock:
            row = self._bound(binding)
            require(row is not None, "Unknown job", 404)
            return self._status(row, after, True)

    def _event(self, job_id, event):
        last = self.db.execute("SELECT seq,event FROM events WHERE job_id=? ORDER BY seq DESC LIMIT 1", (job_id,)).fetchone()
        require(last is None or json.loads(last[1])["type"] not in ("result", "error"),
                "Business event after terminal event")
        self.db.execute("INSERT INTO events VALUES (?,?,?)", (job_id, 1 if last is None else last[0] + 1,
                                                               canonical(event).decode()))

    def put(self, binding, body):
        exact(body, {"claim", "lease_expires_at"}, "Start request")
        claim = body["claim"]
        validate_claim(claim, binding)
        if self.node_id is not None:
            require(claim["node_id"] == self.node_id, "Configured node binding mismatch", 403)
            require(claim["artifact_storage_id"] == self.storage_id, "Artifact storage identity mismatch")
            require(claim["executor_state_id"] == self.state_id, "Executor state store identity mismatch")
            resource = claim["resource_device_id"]
        lease = utc(body["lease_expires_at"])
        command = canonical(claim).decode()
        job_id = binding["job_id"]
        with self.lock:
            row = self._bound(binding)
            if row:
                require(row["claim"] is not None, "Stopped-before-start tombstone", 409)
                require(row["claim"] == command, "Immutable command conflicts with original", 409)
                return self._status(row)  # A replay does NOT renew the lease.
            if self.node_id is not None:
                require(resource == "cpu" if claim["spec"]["resource_kind"] == "cpu" else
                        resource in self.mock_gpu_slots, "Resource does not match configured node/step")
            require(lease > time.time(), "Start authorization has expired", 409)
            committed_inputs(self.root, claim)
            deadlines = []
            if claim["deadline_at"] is not None:
                deadlines.append(utc(claim["deadline_at"]))
            for field, start in (("attempt_hard_timeout_seconds", claim["started_at"]),
                                 ("task_deadline_seconds", claim["first_started_at"] or claim["created_at"])):
                duration = claim["runtime_policy"][field]
                if duration is not None and duration > 0:
                    deadlines.append((utc(start) if start is not None else time.time()) + duration)
            hard_deadline = min(deadlines) if deadlines else None
            with self.db:
                if self.node_id is not None:
                    self.db.execute("BEGIN IMMEDIATE")
                    cpu, slots = self._resources()
                    if resource == "cpu":
                        require(cpu < self.cpu_capacity, "Node CPU capacity is occupied by unconfirmed jobs", 409)
                    else:
                        slot = next(slot for slot in slots if slot["device_id"] == resource)
                        require(slot["state"] == "AVAILABLE", "MOCK GPU device has an unconfirmed owner", 409)
                self.db.execute("INSERT INTO jobs (job_id,binding,claim,command_fingerprint,lease,hard_deadline,"
                                "state,start_count,host) VALUES (?,?,?,?,?,?,'STARTING',0,?)",
                                (job_id, canonical(binding).decode(), command,
                                 hashlib.sha256(command.encode()).hexdigest(), lease, hard_deadline, self.host))
            # The STARTING row is committed before Popen. A crash in this gap stays UNCERTAIN.
            environment = {key: os.environ[key] for key in
                           ("SYSTEMROOT", "SystemRoot", "WINDIR", "TEMP", "TMP", "TMPDIR", "LANG", "LC_ALL")
                           if key in os.environ and os.environ[key].encode("utf-8") != self.secret}
            try:
                child = subprocess.Popen([COMPUTE_PYTHON, "-I", str(HERE), "--compute"],
                                         cwd=self.directory, env=environment, stdin=subprocess.PIPE,
                                         stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                         close_fds=True, creationflags=(subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0))
            except OSError:
                with self.db:
                    self._event(job_id, {"type": "error", "error_code": "EXECUTOR_START_FAILED",
                                         "message": "Fixed compute process could not start", "retryable": False})
                    self.db.execute("UPDATE jobs SET state='EXITED',stop_requested=1 WHERE job_id=?", (job_id,))
                return self._status(self._bound(binding))
            try:
                created = psutil.Process(child.pid).create_time()
            except psutil.Error:
                created = None
            with self.db:
                self.db.execute("UPDATE jobs SET pid=?,process_created=?,start_count=1,state=? WHERE job_id=?",
                                (child.pid, created, "RUNNING" if created is not None else "UNCERTAIN", job_id))
            live = {"process": child, "drained": threading.Event(), "stream_lost": False}
            self.live[job_id] = live
            threading.Thread(target=self._pump, args=(job_id, live, {"claim": claim, "artifact_root": str(self.root)}),
                             name=f"stdout-{job_id}", daemon=True).start()
            return self._status(self._bound(binding))

    def renew(self, binding, body):
        exact(body, {"lease_expires_at"}, "Lease request")
        lease = utc(body["lease_expires_at"])
        with self.lock, self.db:
            row = self._bound(binding)
            require(row is not None, "Unknown job", 404)
            require(row["state"] in ("STARTING", "RUNNING") and not row["stop_requested"] and
                    row["lease"] > time.time() and lease > time.time(), "Job lease cannot be revived", 409)
            self.db.execute("UPDATE jobs SET lease=MAX(lease,?) WHERE job_id=?", (lease, binding["job_id"]))
            return self._status(self._bound(binding))

    def stop(self, binding, body):
        exact(body, set(), "Stop request")
        with self.lock, self.db:
            row = self._bound(binding)
            if row is None:
                self.db.execute("INSERT INTO jobs (job_id,binding,lease,state,start_count,stop_requested,host) "
                                "VALUES (?, ?, 0, 'EXITED', 0, 1, ?)",
                                (binding["job_id"], canonical(binding).decode(), self.host))
            else:
                self.db.execute("UPDATE jobs SET stop_requested=1 WHERE job_id=?", (binding["job_id"],))
            return self._status(self._bound(binding))

    def _pump(self, job_id, live, payload):
        child = live["process"]
        try:
            try:
                child.stdin.write(canonical(payload))
                child.stdin.close()
            except BrokenPipeError:
                pass  # Still drain anything emitted before the child closed its input.
            for line in child.stdout:
                event = read_json(line)
                require(isinstance(event, dict) and event.get("type") in
                        ("progress", "checkpoint", "result", "error"), "Invalid compute event")
                if event["type"] in ("checkpoint", "result"):
                    artifact = event.get("artifact")
                    data = read_artifact(self.root, artifact, "checkpoint" if event["type"] == "checkpoint" else "output")
                    claim = payload["claim"]
                    require(artifact["relative_path"].startswith(f"{claim['task_id']}/{claim['step_key']}/{claim['attempt_id']}/")
                            and data["compatibility_key"] == claim["compatibility_key"], "Compute output escaped Attempt")
                with self.lock, self.db:
                    self._event(job_id, event)
        except Exception:
            live["stream_lost"] = True
            with self.lock, self.db:
                self.db.execute("UPDATE jobs SET stream_lost=1,stop_requested=1 WHERE job_id=?", (job_id,))
        finally:
            try:
                child.stdin.close()
            except OSError:
                pass
            child.stdout.close()
            live["drained"].set()

    def _identity(self, row):
        if row["pid"] is None or row["process_created"] is None or row["host"] != self.host:
            return "unknown", None
        try:
            process = psutil.Process(row["pid"])
            if process.create_time() != row["process_created"] or not process.is_running():
                return "gone", None  # A reused PID is never killed.
            return "alive", process
        except psutil.NoSuchProcess:
            return "gone", None
        except psutil.Error:
            return "unknown", None

    def _tick(self):
        with self.lock:
            rows = self.db.execute("SELECT * FROM jobs WHERE state!='EXITED'").fetchall()
            for row in rows:
                job_id = row["job_id"]
                live = self.live.get(job_id)
                if live and live["process"].poll() is not None:
                    if not live["drained"].is_set():
                        continue  # All stdout must be durably drained before EXITED.
                    result = self.db.execute("SELECT event FROM events WHERE job_id=? ORDER BY seq DESC LIMIT 1", (job_id,)).fetchone()
                    code = live["process"].returncode
                    if live["stream_lost"] or row["stream_lost"] or (code == 0 and
                            (result is None or json.loads(result[0]).get("type") != "result")):
                        code = None
                    with self.db:
                        self.db.execute("UPDATE jobs SET state='EXITED',returncode=? WHERE job_id=?", (code, job_id))
                    del self.live[job_id]
                    continue
                status, process = self._identity(row)
                if not live and status == "gone":
                    with self.db:
                        self.db.execute("UPDATE jobs SET state='EXITED',returncode=NULL WHERE job_id=?", (job_id,))
                    continue
                if status == "unknown":
                    with self.db:
                        self.db.execute("UPDATE jobs SET state='UNCERTAIN',returncode=NULL WHERE job_id=?", (job_id,))
                    continue
                expired = row["lease"] <= time.time() or (row["hard_deadline"] is not None and row["hard_deadline"] <= time.time())
                if row["stop_requested"] or expired:
                    with self.db:
                        self.db.execute("UPDATE jobs SET stop_requested=1 WHERE job_id=?", (job_id,))
                    if process is not None:
                        try:
                            process.kill()
                        except psutil.NoSuchProcess:
                            pass
                        except psutil.Error:
                            with self.db:
                                self.db.execute("UPDATE jobs SET state='UNCERTAIN' WHERE job_id=?", (job_id,))

    def _watch(self):
        while not self.closed.wait(0.1):
            try:
                self._tick()
            except Exception as exc:
                print(f"watchdog reconciliation failed: {type(exc).__name__}", file=sys.stderr, flush=True)

    def close(self):
        with self.lock, self.db:
            self.db.execute("UPDATE jobs SET stop_requested=1 WHERE state!='EXITED'")
        end = time.monotonic() + 3
        while self.live and time.monotonic() < end:
            self._tick()
            time.sleep(0.05)
        self.closed.set()
        self.watchdog.join()
        with self.lock:
            self.db.close()
            self.owner.close()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass  # Do not log credentials or task contents.

    def _serve(self):
        self._body_consumed = False
        try:
            status, body = self._dispatch()
        except Rejection as exc:
            status, body = exc.status, {"detail": exc.detail}
            # Keep authentication ahead of body validation, but drain a framed
            # denial request before closing. Unread bytes can reset the TCP
            # connection on Windows and hide an otherwise valid 403 response.
            length = self.headers.get("Content-Length", "")
            if (self.command in ("PUT", "POST") and not self._body_consumed
                    and length.isascii() and length.isdecimal() and int(length) <= 8 * 1024 * 1024):
                try:
                    self.connection.settimeout(1)
                    self.rfile.read(int(length))
                except OSError:
                    pass
        except Exception as exc:
            print(f"request failed: {type(exc).__name__}", file=sys.stderr, flush=True)
            status, body = 500, {"detail": "Executor state could not be confirmed"}
        raw = canonical(body)
        self.close_connection = True
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("X-Execution-Protocol", "1")
            self.send_header("Content-Length", str(len(raw)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(raw)
        except (ConnectionError, OSError):
            pass  # Lost responses never reverse durable starts or create another child.

    def _body(self):
        length = self.headers.get("Content-Length", "")
        require(length.isascii() and length.isdecimal(), "A JSON Content-Length is required")
        require(not self.headers.get("Transfer-Encoding"), "Chunked requests are unsupported")
        raw = self.rfile.read(int(length))
        self._body_consumed = True
        require(len(raw) == int(length), "Truncated JSON body")
        return read_json(raw)

    def _dispatch(self):
        parts = urlsplit(self.path)
        executor = self.server.executor
        if self.command == "GET" and parts.path == "/health":
            return 200, {"status": "ok"}
        if self.command in ("GET", "POST") and parts.path == "/v1/capabilities":
            executor.authenticate(self.headers, capabilities=True)
            require(not parts.query, "Unexpected capability query")
            if self.command == "POST":
                require(self.headers.get("X-Execution-Scope") == "capabilities",
                        "Storage inspection requires capabilities-only scope", 403)
                return 200, executor.inspect_storage(self._body())
            return 200, executor.capabilities()
        match = re.fullmatch(r"/v1/jobs/([^/]+)(?:/(lease|stop))?", parts.path)
        require(match is not None, "Unsupported endpoint", 404)
        job_id, action = match.groups()
        binding = executor.authenticate(self.headers, job_id)
        if self.command == "GET" and action is None:
            try:
                query = parse_qs(parts.query, keep_blank_values=True, strict_parsing=True)
            except ValueError as exc:
                raise Rejection(422, "Invalid event cursor") from exc
            require(not (set(query) - {"after_seq"}) and len(query.get("after_seq", ["0"])) == 1,
                    "Only one after_seq cursor is supported")
            cursor = query.get("after_seq", ["0"])[0]
            require(cursor.isascii() and cursor.isdecimal(), "after_seq must be nonnegative")
            return 200, executor.status(binding, int(cursor))
        require(not parts.query, "Unexpected command query")
        if self.command == "PUT" and action is None:
            return 200, executor.put(binding, self._body())
        if self.command == "POST" and action == "lease":
            return 200, executor.renew(binding, self._body())
        if self.command == "POST" and action == "stop":
            return 200, executor.stop(binding, self._body())
        raise Rejection(404, "Unsupported endpoint")

    do_GET = do_PUT = do_POST = _serve


def main():
    if sys.argv[1:] == ["--compute"]:
        try:
            return compute(read_json(sys.stdin.buffer.read()))
        except Exception as exc:
            emit({"type": "error", "error_code": "INVALID_CONFIG" if isinstance(exc, Rejection) else "COMPUTE_ERROR",
                  "message": exc.detail if isinstance(exc, Rejection) else "Independent MOCK computation failed",
                  "retryable": False})
            return 1
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--executor-id", required=True)
    parser.add_argument("--secret-env", default="EXTERNAL_EXECUTOR_SECRET")
    parser.add_argument("--artifact-root", required=True, type=Path)
    parser.add_argument("--state-dir", required=True, type=Path)
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--node-id")
    parser.add_argument("--storage-id", default="pipeline-artifacts")
    parser.add_argument("--cpu-capacity", default=2, type=int)
    parser.add_argument("--mock-gpu-slots", default="gpu-0")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--tls-cert", type=Path)
    parser.add_argument("--tls-key", type=Path)
    args = parser.parse_args()
    try:
        loopback = ipaddress.ip_address(args.host).is_loopback
    except ValueError:
        loopback = args.host.lower() == "localhost"
    if bool(args.tls_cert) != bool(args.tls_key):
        parser.error("--tls-cert and --tls-key must be supplied together")
    if not loopback and args.tls_cert is None:
        parser.error("Non-loopback listening requires --tls-cert and --tls-key")
    tls = None
    if args.tls_cert is not None:
        try:
            tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            tls.load_cert_chain(args.tls_cert, args.tls_key)
        except (OSError, ssl.SSLError):
            parser.error("TLS certificate or key could not be loaded")
    try:
        executor = Executor(args.executor_id, os.environ.get(args.secret_env, ""), args.artifact_root, args.state_dir,
                            node_id=args.node_id, storage_id=args.storage_id, cpu_capacity=args.cpu_capacity,
                            mock_gpu_slots=[device.strip() for device in args.mock_gpu_slots.split(",") if device.strip()])
    except (Rejection, RuntimeError) as exc:
        parser.error(str(exc))
    server = None
    try:
        class Server(ThreadingHTTPServer):
            address_family = socket.AF_INET6 if ":" in args.host else socket.AF_INET

        server = Server((args.host, args.port), Handler)
        if tls is not None:
            server.socket = tls.wrap_socket(server.socket, server_side=True)
        server.executor = executor
        server.daemon_threads = True

        def shutdown(*_):
            threading.Thread(target=server.shutdown, daemon=True).start()

        signal.signal(signal.SIGINT, shutdown)
        signal.signal(signal.SIGTERM, shutdown)
        print(f"Independent MOCK executor listening on {'https' if tls else 'http'}://{args.host}:{server.server_port}", flush=True)
        server.serve_forever(poll_interval=0.1)
    finally:
        if server is not None:
            server.server_close()
        executor.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
