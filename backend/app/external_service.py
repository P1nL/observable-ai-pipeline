"""Single-machine external executor: authenticated HTTP, durable SQLite, no PG.

Only the existing fixed app.executor is launched. Files remain candidates until
the central repository fences and commits them; this service cannot commit them.
"""

from __future__ import annotations

import argparse
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath, PureWindowsPath
import re
import sqlite3
import subprocess
import sys
from threading import Event, Lock, RLock, Thread
from uuid import UUID, uuid4

from fastapi import FastAPI, HTTPException, Query, Request

from .domain import StepSpec
from .external_protocol import (PROTOCOL_VERSION, REQUIRED_CAPABILITIES, authenticate,
    binding_for, canonical_json, utc_time, validate_capabilities, validate_event)
from .processes import executor_state, identify_executor, stop_executor
from .artifacts import _read_verified, validate_artifact


STEP_KEYS = {"normalize_asset", "segment_subject", "analyze_motion", "generate_frames", "encode_package", "publish_result", "mock"}
CLAIM_KEYS = {
    "task_id", "step_id", "step_key", "attempt_id", "generation", "attempt_number", "recovery_round",
    "worker_id", "lease_expires_at", "started_at", "first_started_at", "created_at", "deadline_at",
    "runtime_policy", "retry_policy", "spec", "input", "input_asset", "compatibility_key", "checkpoint",
    "dependencies", "resource_slot_id", "execution_backend", "executor_id", "external_job_id",
}
CLAIM_KEYS |= {"node_id", "resource_device_id", "artifact_storage_id", "executor_state_id"}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _reference(value: object, *, kind: str | None = None) -> None:
    if not isinstance(value, dict):
        raise ValueError("Invalid artifact reference")
    UUID(str(value["id"]))
    path = value.get("relative_path")
    if (not isinstance(path, str) or not path or PurePosixPath(path).is_absolute()
            or PureWindowsPath(path).drive or "\\" in path
            or any(part in {"", ".", ".."} for part in path.split("/"))):
        raise ValueError("Invalid relative artifact reference")
    if not isinstance(value.get("content_fingerprint"), str) or not re.fullmatch(r"[0-9a-f]{64}", value["content_fingerprint"]):
        raise ValueError("Invalid artifact fingerprint")
    if kind is not None and (value.get("kind") != kind or not isinstance(value.get("metadata"), dict)):
        raise ValueError("Invalid artifact kind or metadata")


def _claim(value: object, binding: dict) -> dict:
    if not isinstance(value, dict) or set(value) - CLAIM_KEYS or binding_for(value) != binding:
        raise ValueError("Claim does not match its authenticated execution binding")
    if value.get("execution_backend") != "external":
        raise ValueError("Expected external execution backend")
    spec = StepSpec.model_validate(value.get("spec"))
    generic = spec.processor == "mock-step"
    if value.get("step_key") != spec.key or (not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", spec.key) if generic else spec.key not in STEP_KEYS):
        raise ValueError("Unapproved step")
    if not generic and (spec.resource_kind != ("mock-gpu" if spec.key == "generate_frames" else "cpu")
            or spec.checkpoint_mode != ("committed-batch-manifest" if spec.key == "generate_frames" else "none")
            or any(key not in STEP_KEYS or key == spec.key for key in spec.depends_on)
            or len(set(spec.depends_on)) != len(spec.depends_on)):
        raise ValueError("Invalid step resource or checkpoint contract")
    if generic and (spec.checkpoint_mode != "committed-batch-manifest"
            or len(spec.depends_on) > 1 or any(not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", k) or k == spec.key for k in spec.depends_on)
            or value.get("input", {}).get("kind") != "synthetic" or value.get("input_asset") is not None):
        raise ValueError("Invalid registered mock-step contract")
    allowed = {"duration_seconds", "batch_count", "batch_duration_seconds", "frames_per_batch", "fail_first_attempts", "seed"}
    if set(spec.parameters) - allowed:
        raise ValueError("Unapproved computation parameters")
    for key, item in spec.parameters.items():
        if key in {"duration_seconds", "batch_duration_seconds"}:
            if type(item) not in (float, int) or not math.isfinite(item) or item < 0:
                raise ValueError("Invalid computation duration")
        elif type(item) is not int or item < (1 if key in {"batch_count", "frames_per_batch"} else 0):
            raise ValueError("Invalid computation count")
    if type(value.get("attempt_number")) is not int or value["attempt_number"] < 1:
        raise ValueError("Invalid Attempt number")
    if not isinstance(value.get("compatibility_key"), str) or not value["compatibility_key"]:
        raise ValueError("Missing compatibility key")
    if not isinstance(value.get("input", {}), dict):
        raise ValueError("Invalid input")
    slot = value.get("resource_slot_id")
    if slot is not None and (type(slot) is not int or slot < 1 or spec.resource_kind != "mock-gpu"):
        raise ValueError("Invalid resource slot")
    checkpoint = value.get("checkpoint")
    if checkpoint is not None:
        _reference(checkpoint, kind="checkpoint")
        if spec.checkpoint_mode == "none" or checkpoint["metadata"].get("compatibility_key") != value["compatibility_key"]:
            raise ValueError("Incompatible checkpoint")
    dependencies = value.get("dependencies", [])
    if not isinstance(dependencies, list) or len(dependencies) > 6:
        raise ValueError("Invalid dependencies")
    for artifact in dependencies:
        _reference(artifact, kind="output")
    if value.get("input_asset") is not None:
        _reference(value["input_asset"])
    # JSON is fingerprinted as supplied, not silently rewritten by Pydantic.
    canonical_json(value)
    return value


class ExternalService:
    def __init__(self, executor_id: str, secret: str, artifact_root: Path, state_dir: Path, *, watchdog_interval: float = .1,
                 node_id: str | None = None, storage_id: str = "pipeline-artifacts", cpu_capacity: int = 2,
                 mock_gpu_slots=("gpu-0",)):
        if not isinstance(secret, str) or len(secret) < 32:
            raise ValueError("Executor secret must contain at least 32 characters")
        if not executor_id or len(executor_id) > 128 or any(ord(char) < 32 for char in executor_id):
            raise ValueError("Invalid executor identity")
        if not Path(artifact_root).is_absolute():
            raise ValueError("artifact-root must be an absolute shared path")
        self.executor_id, self.secret = executor_id, secret
        if node_id is not None and (not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", node_id) or node_id == "legacy"):
            raise ValueError("Invalid node identity")
        if type(cpu_capacity) is not int or not 0 <= cpu_capacity <= 64:
            raise ValueError("Invalid CPU scheduling capacity")
        if len(set(mock_gpu_slots)) != len(mock_gpu_slots) or any(not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", d) or d in {"cpu", "inspection"} for d in mock_gpu_slots):
            raise ValueError("Invalid mock device identities")
        self.node_id, self.storage_id, self.cpu_capacity = node_id, storage_id, cpu_capacity
        self.devices = tuple(mock_gpu_slots)
        self.root = Path(artifact_root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        directory = Path(state_dir).resolve()
        directory.mkdir(parents=True, exist_ok=True)
        self.database = directory / "executor.sqlite3"
        self.interval = watchdog_interval
        self._locks: dict[str, RLock] = {}
        self._registry_lock = Lock()
        self._children: dict[str, subprocess.Popen] = {}
        self._readers: dict[str, Thread] = {}
        self._halt = Event()
        self._watchdog: Thread | None = None
        with self._db() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS service_identity (id INTEGER PRIMARY KEY CHECK(id=1), executor_id TEXT NOT NULL, artifact_root TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS service_configuration (key TEXT PRIMARY KEY,value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY, binding TEXT NOT NULL, payload TEXT, fingerprint TEXT,
                    lease_expires_at TEXT, state TEXT NOT NULL, exit_confirmed INTEGER NOT NULL DEFAULT 0,
                    returncode INTEGER, start_count INTEGER NOT NULL DEFAULT 0, pid INTEGER,
                    created_at REAL, host TEXT, stop_requested INTEGER NOT NULL DEFAULT 0,
                    reader_complete INTEGER NOT NULL DEFAULT 0, tombstone INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS events (
                    job_id TEXT NOT NULL REFERENCES jobs(job_id), seq INTEGER NOT NULL, event TEXT NOT NULL,
                    PRIMARY KEY(job_id, seq)
                );
            """)
            identity = conn.execute("SELECT * FROM service_identity WHERE id=1").fetchone()
            if identity and (identity["executor_id"] != executor_id or identity["artifact_root"] != str(self.root)):
                raise ValueError("State directory belongs to another executor or artifact root")
            conn.execute("INSERT OR IGNORE INTO service_identity VALUES (1,?,?)", (executor_id, str(self.root)))
            for key, value in (("node_id", node_id), ("storage_id", storage_id)):
                old = conn.execute("SELECT value FROM service_configuration WHERE key=?", (key,)).fetchone()
                if old and json.loads(old["value"]) != value:
                    raise ValueError("State directory belongs to another node/storage identity")
                if key == "node_id" and old is None and node_id and conn.execute("SELECT 1 FROM jobs LIMIT 1").fetchone():
                    raise ValueError("Legacy jobs must retain their original service, not be adopted as a new node")
                conn.execute("INSERT OR IGNORE INTO service_configuration VALUES (?,?)", (key, canonical_json(value)))
            conn.execute("INSERT OR IGNORE INTO service_configuration VALUES ('state_id',?)", (canonical_json(str(uuid4())),))
            self.state_id = json.loads(conn.execute("SELECT value FROM service_configuration WHERE key='state_id'").fetchone()[0])

    @contextmanager
    def _db(self):
        conn = sqlite3.connect(self.database, timeout=10)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=FULL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _lock(self, job_id: str) -> RLock:
        with self._registry_lock:
            return self._locks.setdefault(job_id, RLock())

    def start(self) -> None:
        # A lost reader cannot be reconstructed from a PID. Stop a verified
        # orphan rather than pretending its unrecorded result was delivered.
        with self._db() as conn:
            conn.execute("UPDATE jobs SET state='UNCERTAIN',stop_requested=1 WHERE exit_confirmed=0")
        self._watchdog = Thread(target=self._watch, name="external-watchdog", daemon=True)
        self._watchdog.start()

    def close(self) -> None:
        with self._db() as conn:
            conn.execute("UPDATE jobs SET stop_requested=1 WHERE exit_confirmed=0")
        self._halt.set()
        if self._watchdog:
            self._watchdog.join(timeout=2)
        self._tick()
        for reader in list(self._readers.values()):
            reader.join(timeout=2)

    def _row(self, conn, binding):
        row = conn.execute("SELECT * FROM jobs WHERE job_id=?", (binding["job_id"],)).fetchone()
        if row and row["binding"] != canonical_json(binding):
            raise HTTPException(403, "Execution binding does not own this job")
        return row

    def status(self, binding: dict, after_seq: int = 0, *, events: bool = True) -> dict:
        with self._lock(binding["job_id"]), self._db() as conn:
            row = self._row(conn, binding)
            if row is None:
                raise HTTPException(404, "Unknown job")
            # No LIMIT: confirmed exit is returned only with every remaining
            # durable event, including result, in this same database snapshot.
            messages = conn.execute("SELECT seq,event FROM events WHERE job_id=? AND seq>? ORDER BY seq", (binding["job_id"], after_seq)).fetchall() if events else []
            return {"binding": binding, "state": row["state"], "exit_confirmed": bool(row["exit_confirmed"]),
                    "returncode": row["returncode"], "events": [{"seq": item["seq"], "event": json.loads(item["event"])} for item in messages],
                    "start_count": row["start_count"], "observed_at": _now(), "state_id": self.state_id}

    def put(self, binding: dict, body: dict) -> dict:
        if not isinstance(body, dict) or set(body) != {"claim", "lease_expires_at"}:
            raise HTTPException(422, "Expected claim and lease_expires_at only")
        try:
            claim = _claim(body["claim"], binding)
            validate_capabilities(self.capabilities(), binding, claim)
            if self.node_id:
                if (claim.get("node_id") != self.node_id or claim.get("artifact_storage_id") != self.storage_id
                        or claim.get("executor_state_id") != self.state_id
                        or claim.get("resource_device_id") != binding.get("resource_id")
                        or (claim["spec"]["resource_kind"] == "cpu" and binding["resource_id"] != "cpu")
                        or (claim["spec"]["resource_kind"] == "mock-gpu" and binding["resource_id"] not in self.devices)):
                    raise ValueError("Node/resource/storage differs from execution authorization")
            elif "node_id" in claim:
                raise ValueError("This service is in legacy mode")
            expires = utc_time(body["lease_expires_at"])
            payload = canonical_json(claim)
        except (ValueError, KeyError, TypeError) as error:
            raise HTTPException(422, "Invalid immutable execution envelope") from error
        fingerprint = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        with self._lock(binding["job_id"]):
            with self._db() as conn:
                previous = self._row(conn, binding)
                if previous:
                    if previous["tombstone"] or previous["fingerprint"] != fingerprint:
                        raise HTTPException(409, "Command was stopped or its immutable payload differs")
                else:
                    if expires <= datetime.now(timezone.utc):
                        raise HTTPException(409, "Execution lease already expired")
                    if self.node_id:
                        active = conn.execute("SELECT * FROM jobs WHERE exit_confirmed=0").fetchall()
                        resources = [json.loads(row["binding"]).get("resource_id") for row in active]
                        resource = binding["resource_id"]
                        if ((resource == "cpu" and resources.count("cpu") >= self.cpu_capacity)
                                or (resource != "cpu" and resource in resources)):
                            raise HTTPException(409, "Node scheduling resource is occupied or unconfirmed")
                    # The stable command and its fingerprint commit before Popen.
                    conn.execute("INSERT INTO jobs(job_id,binding,payload,fingerprint,lease_expires_at,state) VALUES (?,?,?,?,?,'STARTING')",
                                 (binding["job_id"], canonical_json(binding), payload, fingerprint, expires.isoformat()))
            if previous is None:
                self._launch(binding, claim)
            return self.status(binding, events=False)

    def capabilities(self) -> dict:
        with self._db() as conn:
            active = conn.execute("SELECT * FROM jobs WHERE exit_confirmed=0").fetchall()
        owners = {}
        for row in active:
            binding = json.loads(row["binding"])
            resource = binding.get("resource_id")
            if resource is None:
                claim = json.loads(row["payload"] or "{}")
                resource = "gpu-0" if claim.get("spec", {}).get("resource_kind") == "mock-gpu" else "cpu"
            owners.setdefault(resource, []).append((row, binding))
        slots = []
        for device in sorted(set(self.devices) | (set(owners)-{"cpu", "inspection"})):
            rows = owners.get(device, [])
            uncertain = len(rows) > 1 or any(row[0]["state"] in {"UNCERTAIN", "STARTING"} for row in rows)
            slots.append({"device_id": device, "state": "QUARANTINED" if uncertain else "RUNNING" if rows else "AVAILABLE",
                          "owner_attempt_id": rows[0][1]["attempt_id"] if len(rows) == 1 else None,
                          "owner_generation": rows[0][1]["generation"] if len(rows) == 1 else None})
        return {"protocol_version": PROTOCOL_VERSION, "executor_id": self.executor_id,
                "node_id": self.node_id, "storage_id": self.storage_id, "state_id": self.state_id, "cpu_capacity": self.cpu_capacity,
                "cpu_in_use": len(owners.get("cpu", [])), "mock_gpu_slots": slots,
                "capabilities": sorted(REQUIRED_CAPABILITIES | ({"node-binding-v1", "shared-storage-inspection-v1"} if self.node_id else set())),
                "processors": {"mock": ["1"], "mock-step": ["1"]},
                "named_step_processors": ["mock-step"],
                "step_keys": sorted(STEP_KEYS), "input_kinds": ["synthetic", "uploaded"],
                "artifact_schema_versions": [1]}

    def inspect(self, body):
        if not isinstance(body, dict) or set(body) != {"storage_id", "references"} or not isinstance(body["references"], list):
            raise HTTPException(422, "Expected storage_id and committed references")
        readable = body["storage_id"] == self.storage_id
        try:
            for reference in body["references"]:
                if reference.get("kind") == "storage-probe":
                    if reference.get("relative_path") != "_system/storage-id.json" or json.loads(_read_verified(self.root, reference)).get("storage_id") != self.storage_id:
                        raise ValueError("Shared storage namespace mismatch")
                elif reference.get("kind") in {"output", "checkpoint"}:
                    validate_artifact(self.root, reference)
                else:
                    _read_verified(self.root, reference)
        except (ValueError, TypeError, KeyError, OSError):
            readable = False
        return {**self.capabilities(), "references_readable": readable,
                **({"inspection_error": "ARTIFACT_UNAVAILABLE"} if not readable else {})}

    def _launch(self, binding: dict, claim: dict) -> None:
        identifier = binding["job_id"]
        environment = {key: value for key, value in os.environ.items() if key in {
            "SYSTEMROOT", "SystemRoot", "WINDIR", "PATH", "TEMP", "TMP", "TMPDIR", "LANG", "LC_ALL"}}
        environment["PYTHONPATH"] = str(Path(__file__).resolve().parent.parent)
        try:
            process = subprocess.Popen(
                [sys.executable, "-m", "app.executor", "--attempt-id", binding["attempt_id"], "--generation", str(binding["generation"])],
                cwd=Path(__file__).resolve().parent.parent, env=environment,
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                text=True, encoding="utf-8", bufsize=1,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
        except OSError:
            with self._db() as conn:
                conn.execute("UPDATE jobs SET state='EXITED',exit_confirmed=1,reader_complete=1 WHERE job_id=?", (identifier,))
            return
        self._children[identifier] = process
        try:
            identity = identify_executor(process.pid, claim)
            with self._db() as conn:
                conn.execute("UPDATE jobs SET pid=?,created_at=?,host=?,start_count=1,state='RUNNING' WHERE job_id=?",
                             (identity["pid"], identity["created_at"], identity["host"], identifier))
            reader = Thread(target=self._read, args=(identifier, process), name="external-output-" + identifier, daemon=True)
            self._readers[identifier] = reader
            reader.start()
            # No computation starts until its durable PID/creation-time/host is
            # registered, and the already-committed lease still authorizes it.
            with self._db() as conn:
                row = conn.execute("SELECT * FROM jobs WHERE job_id=?", (identifier,)).fetchone()
            if utc_time(row["lease_expires_at"]) <= datetime.now(timezone.utc):
                self._request_stop(identifier)
            else:
                process.stdin.write(canonical_json({"claim": claim, "artifact_root": str(self.root)}) + "\n")
        except (OSError, ValueError, sqlite3.Error):
            self._request_stop(identifier)
        finally:
            try:
                process.stdin.close()
            except (OSError, ValueError):
                pass

    def _read(self, identifier: str, process: subprocess.Popen) -> None:
        try:
            for line in process.stdout:
                try:
                    event = validate_event(json.loads(line))
                except (ValueError, TypeError):
                    self._request_stop(identifier)
                    event = {"type": "error", "error_code": "INVALID_INPUT", "message": "Invalid executor output protocol", "retryable": False}
                with self._lock(identifier), self._db() as conn:
                    conn.execute("INSERT INTO events(job_id,seq,event) SELECT ?,COALESCE(MAX(seq),0)+1,? FROM events WHERE job_id=?",
                                 (identifier, canonical_json(event), identifier))
            code = process.wait()
            with self._lock(identifier), self._db() as conn:
                conn.execute("UPDATE jobs SET state='EXITED',exit_confirmed=1,returncode=?,reader_complete=1 WHERE job_id=?", (code, identifier))
        except (OSError, ValueError, sqlite3.Error):
            self._request_stop(identifier)
            with self._lock(identifier), self._db() as conn:
                conn.execute("UPDATE jobs SET state='UNCERTAIN' WHERE job_id=? AND exit_confirmed=0", (identifier,))
        finally:
            process.stdout.close()

    def _request_stop(self, identifier: str) -> None:
        with self._lock(identifier), self._db() as conn:
            conn.execute("UPDATE jobs SET stop_requested=1 WHERE job_id=?", (identifier,))

    def lease(self, binding: dict, body: dict) -> dict:
        try:
            if not isinstance(body, dict) or set(body) != {"lease_expires_at"}:
                raise ValueError("Invalid lease envelope")
            expires = utc_time(body["lease_expires_at"])
        except (ValueError, TypeError) as error:
            raise HTTPException(422, "Expected an absolute UTC lease expiry") from error
        with self._lock(binding["job_id"]):
            with self._db() as conn:
                row = self._row(conn, binding)
                if row is None:
                    raise HTTPException(404, "Unknown job")
                now = datetime.now(timezone.utc)
                if row["exit_confirmed"] or row["stop_requested"] or utc_time(row["lease_expires_at"]) <= now or expires <= now:
                    raise HTTPException(409, "Stopped or expired execution cannot be revived")
                if expires > utc_time(row["lease_expires_at"]):
                    conn.execute("UPDATE jobs SET lease_expires_at=? WHERE job_id=?", (expires.isoformat(), binding["job_id"]))
            return self.status(binding, events=False)

    def stop(self, binding: dict) -> dict:
        identifier = binding["job_id"]
        with self._lock(identifier):
            with self._db() as conn:
                row = self._row(conn, binding)
                if row is None:
                    conn.execute("INSERT INTO jobs(job_id,binding,state,exit_confirmed,stop_requested,reader_complete,tombstone) VALUES (?,?,'EXITED',1,1,1,1)", (identifier, canonical_json(binding)))
                else:
                    conn.execute("UPDATE jobs SET stop_requested=1 WHERE job_id=?", (identifier,))
            self._reconcile(identifier)
            return self.status(binding, events=False)

    def _reconcile(self, identifier: str) -> None:
        with self._lock(identifier):
            with self._db() as conn:
                row = conn.execute("SELECT * FROM jobs WHERE job_id=?", (identifier,)).fetchone()
            if row["exit_confirmed"]:
                return
            if row["lease_expires_at"] and utc_time(row["lease_expires_at"]) <= datetime.now(timezone.utc):
                self._request_stop(identifier)
                stopping = True
            else:
                stopping = bool(row["stop_requested"])
            state = executor_state(row["pid"], row["created_at"], row["host"])
            if stopping and state == "RUNNING":
                if stop_executor(row["pid"], row["created_at"], row["host"]):
                    state = "EXITED"
            if identifier in self._children:
                # The reader, not poll(), makes normal completion visible.
                reader = self._readers.get(identifier)
                if state == "EXITED" and (reader is None or not reader.is_alive()):
                    # A failed/lost output reader cannot keep a dead computation
                    # RUNNING forever. Confirm physical exit, but do not invent
                    # a successful return code or promote missing final output.
                    with self._db() as conn:
                        conn.execute("UPDATE jobs SET state='EXITED',exit_confirmed=1,returncode=NULL WHERE job_id=? AND exit_confirmed=0", (identifier,))
                elif state == "UNKNOWN":
                    with self._db() as conn:
                        conn.execute("UPDATE jobs SET state='UNCERTAIN' WHERE job_id=? AND exit_confirmed=0", (identifier,))
                return
            with self._db() as conn:
                if state == "EXITED":
                    conn.execute("UPDATE jobs SET state='EXITED',exit_confirmed=1,returncode=NULL WHERE job_id=?", (identifier,))
                else:
                    conn.execute("UPDATE jobs SET state='UNCERTAIN' WHERE job_id=?", (identifier,))

    def _tick(self) -> None:
        with self._db() as conn:
            identifiers = [row["job_id"] for row in conn.execute("SELECT job_id FROM jobs WHERE exit_confirmed=0")]
        for identifier in identifiers:
            self._reconcile(identifier)

    def _watch(self) -> None:
        while not self._halt.wait(self.interval):
            try:
                self._tick()
            except (sqlite3.Error, OSError):
                # A storage interruption does not convert uncertainty into exit.
                continue


def create_app(service: ExternalService) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app):
        service.start()
        try:
            yield
        finally:
            service.close()

    app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)

    def authorize(request: Request, job_id: str | None = None) -> dict:
        scope = request.headers.get("X-Execution-Scope", "job")
        try:
            binding = authenticate(service.secret, request.headers.get("Authorization", ""), request.headers.get("X-Execution-Binding", ""), scope=scope)
        except ValueError as error:
            raise HTTPException(401, "Invalid execution authentication") from error
        if (job_id is not None and job_id != binding["job_id"]) or binding["executor_id"] != service.executor_id:
            raise HTTPException(403, "Execution binding does not match this executor or path")
        if binding.get("node_id") != service.node_id:
            raise HTTPException(403, "Execution binding belongs to another node")
        if request.headers.get("X-Execution-Protocol") != str(PROTOCOL_VERSION):
            raise HTTPException(426, "Unsupported execution protocol version")
        if job_id is not None and scope != "job":
            raise HTTPException(403, "Capability inspection does not authorize execution")
        return binding

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.get("/v1/capabilities")
    def get_capabilities(request: Request):
        authorize(request)
        return service.capabilities()

    @app.post("/v1/capabilities")
    def inspect_capabilities(request: Request, body: dict):
        authorize(request)
        return service.inspect(body)

    @app.put("/v1/jobs/{job_id}")
    def put_job(job_id: str, request: Request, body: dict):
        return service.put(authorize(request, job_id), body)

    @app.get("/v1/jobs/{job_id}")
    def get_job(job_id: str, request: Request, after_seq: int = Query(default=0, ge=0)):
        return service.status(authorize(request, job_id), after_seq)

    @app.post("/v1/jobs/{job_id}/lease")
    def lease_job(job_id: str, request: Request, body: dict):
        return service.lease(authorize(request, job_id), body)

    @app.post("/v1/jobs/{job_id}/stop")
    def stop_job(job_id: str, request: Request, body: dict):
        binding = authorize(request, job_id)
        if body:
            raise HTTPException(422, "Stop accepts an empty object only")
        return service.stop(binding)

    return app


def main() -> None:
    parser = argparse.ArgumentParser(description="Independent fixed-step external executor")
    parser.add_argument("--executor-id", required=True)
    parser.add_argument("--secret-env", default="EXTERNAL_EXECUTOR_SECRET")
    parser.add_argument("--artifact-root", type=Path, required=True)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=8090)
    parser.add_argument("--node-id")
    parser.add_argument("--storage-id", default="pipeline-artifacts")
    parser.add_argument("--cpu-capacity", type=int, default=2)
    parser.add_argument("--mock-gpu-slots", default="gpu-0")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--tls-cert")
    parser.add_argument("--tls-key")
    arguments = parser.parse_args()
    secret = os.environ.pop(arguments.secret_env, "")
    os.environ.pop("DATABASE_URL", None)
    os.environ.pop("TEST_DATABASE_URL", None)
    try:
        if bool(arguments.tls_cert) != bool(arguments.tls_key) or (arguments.host not in {"127.0.0.1", "localhost", "::1"} and not arguments.tls_cert):
            raise ValueError("Non-loopback listeners require TLS certificate and key")
        service = ExternalService(arguments.executor_id, secret, arguments.artifact_root, arguments.state_dir,
            node_id=arguments.node_id, storage_id=arguments.storage_id, cpu_capacity=arguments.cpu_capacity,
            mock_gpu_slots=[d for d in arguments.mock_gpu_slots.split(",") if d])
    except (ValueError, OSError) as error:
        parser.error(str(error))
    import uvicorn

    uvicorn.run(create_app(service), host=arguments.host, port=arguments.port, log_level="warning", access_log=False,
                ssl_certfile=arguments.tls_cert, ssl_keyfile=arguments.tls_key)


if __name__ == "__main__":
    main()
