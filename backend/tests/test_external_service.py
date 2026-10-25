"""Real loopback HTTP + existing executor, isolated from the development DB."""

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import socket
import sqlite3
import subprocess
import sys
import time
from uuid import UUID, uuid4

import httpx
import pytest

from app.artifacts import validate_artifact
from app.domain import StepSpec
from app.external_protocol import auth_headers, binding_for, canonical_json, validate_status
from app.external_service import ExternalService
from app.processes import executor_state, stop_executor


ROOT = Path(__file__).resolve().parents[2]
SECRET = "external-service-test-secret-not-production-0123456789"
EXECUTOR = "executor-one"


def expiry(seconds=30):
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat()


def claim_for(key="mock", duration=.05, *, batches=1):
    attempt = str(uuid4())
    spec = StepSpec(key=key, parameters={"duration_seconds": duration, "batch_count": batches, "batch_duration_seconds": 0},
                    resource_kind="mock-gpu" if key == "generate_frames" else "cpu",
                    checkpoint_mode="committed-batch-manifest" if key == "generate_frames" else "none")
    return {"task_id": str(uuid4()), "step_id": str(uuid4()), "step_key": key, "attempt_id": attempt,
            "generation": 1, "attempt_number": 1, "worker_id": "service-test-worker", "execution_backend": "external",
            "executor_id": EXECUTOR, "external_job_id": attempt, "spec": spec.model_dump(mode="json"),
            "input": {"kind": "synthetic", "seed": 42}, "compatibility_key": "service-test-compatible",
            "dependencies": [], "checkpoint": None, "resource_slot_id": 1 if key == "generate_frames" else None}


class Server:
    def __init__(self, directory):
        self.directory = directory
        self.root = directory / "artifacts"
        self.state = directory / "state"
        self.process = None
        self.logs = []
        self.client = None
        self.port = None

    def start(self):
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            self.port = sock.getsockname()[1]
        env = os.environ.copy()
        env["PYTHONPATH"] = str(ROOT / "backend")
        env["EXTERNAL_EXECUTOR_SECRET"] = SECRET
        env.pop("DATABASE_URL", None)
        env.pop("TEST_DATABASE_URL", None)
        output = (self.directory / f"service-{len(self.logs)}.log").open("w", encoding="utf-8")
        self.logs.append(output)
        self.process = subprocess.Popen(
            [sys.executable, "-m", "app.external_service", "--executor-id", EXECUTOR,
             "--secret-env", "EXTERNAL_EXECUTOR_SECRET", "--artifact-root", str(self.root), "--state-dir", str(self.state), "--port", str(self.port)],
            cwd=ROOT, env=env, stdout=output, stderr=subprocess.STDOUT,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        self.client = httpx.Client(base_url=f"http://127.0.0.1:{self.port}", timeout=10)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            assert self.process.poll() is None, "External server exited; see temporary service log"
            try:
                response = self.client.get("/health")
                if response.status_code == 200:
                    assert response.json() == {"status": "ok"}
                    return
            except httpx.TransportError:
                pass
            time.sleep(.03)
        raise AssertionError("External service did not start")

    def kill(self):
        if self.process and self.process.poll() is None:
            self.process.kill()
            self.process.wait(timeout=10)
        if self.client:
            self.client.close()

    def request(self, method, claim, suffix="", **kwargs):
        binding = binding_for(claim)
        return self.client.request(method, f"/v1/jobs/{binding['job_id']}{suffix}", headers=auth_headers(SECRET, binding), **kwargs)

    def put(self, claim, lease=None):
        return self.request("PUT", claim, json={"claim": claim, "lease_expires_at": lease or expiry()})

    def get(self, claim, after=0):
        response = self.request("GET", claim, params={"after_seq": after})
        assert response.status_code == 200, response.text
        return validate_status(response.json(), binding_for(claim))

    def wait(self, claim, predicate=lambda status: status["exit_confirmed"], timeout=12):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            status = self.get(claim)
            if predicate(status):
                return status
            time.sleep(.03)
        raise AssertionError(f"External job did not reach expected state: {status}")

    def close(self):
        self.kill()
        # Own temporary children only: a killed service intentionally leaves
        # physical process reconciliation to its next start (or test cleanup).
        database = self.state / "executor.sqlite3"
        if database.exists():
            with sqlite3.connect(database) as conn:
                rows = conn.execute("SELECT pid,created_at,host FROM jobs WHERE pid IS NOT NULL").fetchall()
            for row in rows:
                stop_executor(*row)
        for stream in self.logs:
            stream.close()


@pytest.fixture
def server(tmp_path):
    value = Server(tmp_path)
    value.start()
    try:
        yield value
    finally:
        value.close()


def test_real_executor_and_concurrent_response_loss_replay_start_once(server):
    claim = claim_for(duration=.4)
    # Treat the first successful response as lost. Concurrent retransmissions
    # must all return the same durable command, never start another child.
    with ThreadPoolExecutor(max_workers=6) as pool:
        responses = list(pool.map(lambda _: server.put(claim), range(6)))
    assert all(item.status_code == 200 for item in responses), [item.text for item in responses]
    assert all(item.json()["start_count"] == 1 for item in responses)
    final = server.wait(claim)
    assert final["state"] == "EXITED" and final["returncode"] == 0 and final["start_count"] == 1
    assert [item["seq"] for item in final["events"]] == [1, 2]
    assert [item["event"]["type"] for item in final["events"]] == ["progress", "result"]
    artifact = final["events"][-1]["event"]["artifact"]
    assert validate_artifact(server.root, artifact)
    assert artifact["metadata"]["source"] == "mock"
    assert server.get(claim, after=1)["events"] == final["events"][1:]
    changed = deepcopy(claim)
    changed["spec"]["parameters"]["duration_seconds"] = .3
    assert server.put(changed).status_code == 409
    server.kill()
    server.start()
    assert server.put(claim).status_code == 200
    reopened = server.get(claim)
    assert reopened["start_count"] == 1 and reopened["events"] == final["events"]


def test_dead_computation_with_lost_reader_is_not_renewed_as_running(tmp_path, monkeypatch):
    service = ExternalService(EXECUTOR, SECRET, (tmp_path / "artifacts").resolve(), tmp_path / "state")
    claim = claim_for(duration=.05)
    binding = binding_for(claim)

    def lost_reader(identifier, process):
        for _ in process.stdout:
            pass  # Simulate losing event delivery, not a successful computation.
        process.wait()
        process.stdout.close()

    monkeypatch.setattr(service, "_read", lost_reader)
    try:
        service.put(binding, {"claim": claim, "lease_expires_at": expiry()})
        service._readers[binding["job_id"]].join(timeout=10)
        assert not service._readers[binding["job_id"]].is_alive()
        service._tick()
        status = validate_status(service.status(binding), binding)
        assert status["state"] == "EXITED" and status["exit_confirmed"]
        assert status["returncode"] is None and status["events"] == []
    finally:
        service.close()


def test_authentication_and_cross_attempt_or_executor_binding_are_rejected(server):
    claim = claim_for()
    binding = binding_for(claim)
    path = f"/v1/jobs/{binding['job_id']}"
    assert server.client.get(path).status_code == 401
    assert server.client.get(path, headers=auth_headers("wrong-" * 8, binding)).status_code == 401
    assert server.put(claim).status_code == 200
    for field, value in (("task_id", str(uuid4())), ("generation", 2), ("worker_id", "another-worker"), ("executor_id", "another-executor")):
        other = {**binding, field: value}
        assert server.client.get(path, headers=auth_headers(SECRET, other)).status_code == 403
    different = binding_for(claim_for())
    assert server.client.post(path + "/stop", json={}, headers=auth_headers(SECRET, different)).status_code == 403
    altered = {**claim, "attempt_id": str(uuid4())}
    assert server.request("PUT", claim, json={"claim": altered, "lease_expires_at": expiry()}).status_code == 422
    server.wait(claim)


def test_stop_before_start_is_durable_tombstone_and_stop_after_start_waits_for_exit(server):
    never_started = claim_for()
    stopped = server.request("POST", never_started, "/stop", json={})
    assert stopped.status_code == 200
    status = validate_status(stopped.json(), binding_for(never_started))
    assert status["exit_confirmed"] and status["start_count"] == 0 and status["returncode"] is None
    assert server.put(never_started).status_code == 409
    server.kill()
    server.start()
    assert server.put(never_started).status_code == 409
    claim = claim_for(duration=10)
    assert server.put(claim).status_code == 200
    server.wait(claim, lambda value: bool(value["events"]))
    assert server.request("POST", claim, "/stop", json={}).status_code == 200
    final = server.wait(claim)
    assert final["returncode"] != 0 and final["start_count"] == 1
    assert not any(item["event"]["type"] == "result" for item in final["events"])
    assert server.put(claim).json()["start_count"] == 1
    assert server.request("POST", claim, "/lease", json={"lease_expires_at": expiry()}).status_code == 409


def test_lease_expiry_cannot_be_revived_and_renewal_is_independent_of_progress(server):
    expired = claim_for()
    assert server.put(expired, expiry(-1)).status_code == 409
    assert server.request("GET", expired).status_code == 404
    claim = claim_for(duration=10)
    assert server.put(claim, expiry(.5)).status_code == 200
    server.wait(claim, lambda value: bool(value["events"]))
    assert server.request("POST", claim, "/lease", json={"lease_expires_at": expiry(.7)}).status_code == 200
    time.sleep(.8)
    assert server.request("POST", claim, "/lease", json={"lease_expires_at": expiry()}).status_code == 409
    final = server.wait(claim)
    assert final["returncode"] != 0 and final["start_count"] == 1


def test_reopen_starting_without_pid_stays_uncertain_without_second_popen(server):
    claim = claim_for()
    binding = binding_for(claim)
    server.kill()
    payload = canonical_json(claim)
    with sqlite3.connect(server.state / "executor.sqlite3") as conn:
        conn.execute("INSERT INTO jobs(job_id,binding,payload,fingerprint,lease_expires_at,state) VALUES (?,?,?,?,?,'STARTING')",
                     (binding["job_id"], canonical_json(binding), payload, hashlib.sha256(payload.encode()).hexdigest(), expiry()))
    server.start()
    assert server.put(claim).status_code == 200
    status = server.get(claim)
    assert status["state"] == "UNCERTAIN" and not status["exit_confirmed"] and status["start_count"] == 0
    assert server.request("POST", claim, "/stop", json={}).json()["exit_confirmed"] is False
    assert server.request("POST", claim, "/lease", json={"lease_expires_at": expiry()}).status_code == 409


def test_restart_reconciles_live_child_identity_without_relaunching(server):
    claim = claim_for(duration=10)
    assert server.put(claim).status_code == 200
    server.wait(claim, lambda value: bool(value["events"]))
    with sqlite3.connect(server.state / "executor.sqlite3") as conn:
        identity = conn.execute("SELECT pid,created_at,host FROM jobs WHERE job_id=?", (claim["attempt_id"],)).fetchone()
    server.kill()
    server.start()
    final = server.wait(claim)
    assert final["returncode"] is None and final["start_count"] == 1
    assert executor_state(*identity) == "EXITED"
    assert server.put(claim).json()["start_count"] == 1


def test_exit_is_reported_with_all_remaining_events_not_a_truncated_page(server):
    claim = claim_for("generate_frames", batches=125)
    assert server.put(claim).status_code == 200
    final = server.wait(claim)
    assert final["returncode"] == 0
    # More than a typical page: initial progress, each checkpoint/progress pair,
    # and the final result must all be durable before exit_confirmed=true.
    assert len(final["events"]) == 252
    assert final["events"][-1]["event"]["type"] == "result"
    remaining = server.get(claim, after=3)
    assert remaining["exit_confirmed"] and remaining["events"] == final["events"][3:]


def test_envelope_disallows_arbitrary_paths_commands_and_unapproved_steps(server):
    claim = claim_for()
    for extra in ({"command": "calc.exe"}, {"artifact_root": str(server.root)}):
        assert server.request("PUT", claim, json={"claim": claim, "lease_expires_at": expiry(), **extra}).status_code == 422
        changed = {**claim, **extra}
        assert server.put(changed).status_code == 422
    changed = deepcopy(claim)
    changed["step_key"] = changed["spec"]["key"] = "execute_anything"
    assert server.put(changed).status_code == 422
    changed = deepcopy(claim)
    changed["spec"]["processor"] = "shell"
    assert server.put(changed).status_code == 422
    assert server.request("GET", claim).status_code == 404


def test_protocol_normalizes_uuids_and_rejects_unbound_exit_evidence():
    claim = claim_for()
    objects = {**claim, **{field: UUID(claim[field]) for field in ("task_id", "step_id", "attempt_id", "external_job_id")}}
    binding = binding_for(objects)
    assert binding == binding_for(claim)
    status = {"binding": binding, "state": "EXITED", "exit_confirmed": True, "returncode": 0,
              "events": [], "start_count": 1, "observed_at": expiry(0)}
    assert validate_status(status, binding) == status
    other = binding_for(claim_for())
    with pytest.raises(ValueError):
        validate_status(status, other)
    for update in ({"state": "RUNNING"}, {"exit_confirmed": False}, {"returncode": True},
                   {"events": [{"seq": 1, "event": {"type": "_eof"}}]}, {"state": []}):
        with pytest.raises(ValueError):
            validate_status({**status, **update}, binding)


def test_authenticated_capabilities_and_version_refusal_before_start(server):
    claim = claim_for()
    headers = auth_headers(SECRET, binding_for(claim))
    assert server.client.get("/v1/capabilities").status_code == 401
    response = server.client.get("/v1/capabilities", headers=headers)
    assert response.status_code == 200
    from app.external_protocol import validate_capabilities
    validate_capabilities(response.json(), binding_for(claim), claim)
    path = f"/v1/jobs/{claim['attempt_id']}"
    assert server.client.put(path, headers={**headers, "X-Execution-Protocol": "2"}, json={"claim": claim, "lease_expires_at": expiry()}).status_code == 426
    claim["spec"]["processor_version"] = "not-installed"
    assert server.put(claim).status_code == 422
    assert server.request("GET", claim).status_code == 404


def test_service_rejects_short_secret_relative_root_or_state_identity_reuse(tmp_path):
    with pytest.raises(ValueError):
        ExternalService(EXECUTOR, "short", tmp_path / "artifacts", tmp_path / "state")
    with pytest.raises(ValueError):
        ExternalService(EXECUTOR, SECRET, Path("relative"), tmp_path / "state")
    ExternalService(EXECUTOR, SECRET, tmp_path / "artifacts", tmp_path / "state")
    with pytest.raises(ValueError):
        ExternalService("another-executor", SECRET, tmp_path / "artifacts", tmp_path / "state")
