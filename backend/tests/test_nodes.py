"""Two real executor processes; node scheduling simulation, not cross-device QA."""
from dataclasses import dataclass
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
from threading import Thread
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import uuid4

import httpx
from pydantic import ValidationError
import pytest

from app.backends import ExternalClient
from app.config import Settings
from app.db import connect
from app.external_protocol import auth_headers, binding_for
from app.repository import Repository
from conftest import ROOT
from test_execution import attempts, step
from test_external_execution import command, free_port
from test_managed import register, finish

pytestmark = pytest.mark.integration


@dataclass
class Service:
    kind: str
    config: dict
    root: Path
    state: Path
    port: int
    process: object = None


class Nodes:
    def __init__(self, engine, options):
        self.engine = engine
        self.services = []
        self.tls = options.get("tls", False)
        kinds = options.get("kinds", ("standalone", "standalone"))
        for index, kind in enumerate(kinds):
            name = chr(ord("a") + index)
            port = free_port()
            cfg = {"node_id": name, "executor_id": "executor-" + name,
                   "url": f"http://127.0.0.1:{port}", "secret_env": "NODE_" + name.upper() + "_SECRET",
                   "cpu_capacity": 1, "mock_gpu_slots": ["gpu-0", "gpu-1"],
                   "processors": {"mock": ["1" if kind == "builtin" else "independent-mock-v1"],
                                  "mock-step": ["1" if kind == "builtin" else "independent-mock-v1"]},
                   "accepting": not (name == "a" and options.get("fault") == "draining")}
            engine.environment[cfg["secret_env"]] = uuid4().hex + uuid4().hex
            root = engine.settings.artifact_root if not (name == "a" and options.get("fault") == "wrong-mount") else engine.directory / "wrong-mount"
            self.services.append(Service(kind, cfg, root, engine.directory / ("node-" + name), port))
        if self.tls:
            openssl = shutil.which("openssl") or (r"C:\Program Files\Git\usr\bin\openssl.exe" if Path(r"C:\Program Files\Git\usr\bin\openssl.exe").is_file() else None)
            if not openssl:
                pytest.skip("Local TLS acceptance needs openssl to create an ephemeral test certificate")
            self.cert, self.key = engine.directory / "node-test.crt", engine.directory / "node-test.key"
            subprocess.run([openssl, "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                "-subj", "/CN=node-test", "-addext", "subjectAltName=IP:127.0.0.1", "-keyout", str(self.key), "-out", str(self.cert)],
                check=True, capture_output=True, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
            for service in self.services:
                service.config.update(url=f"https://127.0.0.1:{service.port}", ca_file=str(self.cert))
        self.configure()
        for service in self.services:
            self.start(service)
        if options.get("fault") == "down":
            self.kill(self.services[0])

    def configure(self):
        engine = self.engine
        engine.environment.update(EXECUTION_NODES_JSON=json.dumps([s.config for s in self.services]),
            NODE_PROBE_INTERVAL_SECONDS=".25", CPU_MAX_CONCURRENCY="1")
        engine.settings = Settings.from_env(engine.environment)
        engine.repo = Repository(engine.settings)
        engine.api.terminate(); engine.api.wait(timeout=5); engine.start_api()
        engine.repo.sync_nodes()
        engine.managed_backend, engine.managed_version = "external", "independent-mock-v1"

    def start(self, service):
        args = ["--executor-id", service.config["executor_id"], "--node-id", service.config["node_id"],
                "--secret-env", service.config["secret_env"], "--artifact-root", str(service.root),
                "--state-dir", str(service.state), "--port", str(service.port), "--cpu-capacity", "1",
                "--mock-gpu-slots", ",".join(service.config["mock_gpu_slots"])]
        if self.tls:
            args += ["--tls-cert", str(self.cert), "--tls-key", str(self.key)]
        engine = self.engine
        if service.kind == "builtin":
            service.process = engine.start("app.external_service", *args)
        else:
            output = (engine.directory / f"node-{service.config['node_id']}-{len(engine.processes)}.log").open("w", encoding="utf-8")
            engine.handles.append(output)
            env = {key: value for key, value in engine.environment.items() if key not in {"PYTHONPATH", "DATABASE_URL", "TEST_DATABASE_URL"}}
            service.process = subprocess.Popen([sys.executable, "-I", str(ROOT / "executors" / "standalone_mock.py"), *args],
                cwd=engine.directory, env=env, stdout=output, stderr=subprocess.STDOUT,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
            engine.processes.append(service.process)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            assert service.process.poll() is None, engine.logs()
            try:
                with socket.create_connection(("127.0.0.1", service.port), timeout=.1):
                    return
            except OSError:
                time.sleep(.05)
        raise AssertionError("Node service did not bind")

    def kill(self, service):
        if service.process.poll() is None:
            service.process.kill(); service.process.wait(timeout=5)

    def status(self, attempt_id):
        record = command(self.engine, attempt_id)
        return ExternalClient(self.engine.settings, record["binding"], record["route"]).call("GET")

    def task(self, key="work", *, version="independent-mock-v1", resource="cpu", batches=1, duration=.1):
        engine = self.engine
        task_id = engine.submit({"pipeline_id": "managed-v1", "execution_backend": "external"})
        register(engine, task_id, key, processor_version=version, resource_kind=resource,
                 parameters={"batch_count": batches, "batch_duration_seconds": duration})
        finish(engine, task_id, key)
        return task_id

    def close(self):
        for service in self.services:
            if service.process.poll() is not None:
                self.start(service)
        with connect(self.engine.settings) as conn:
            rows = conn.execute("SELECT a.*,s.task_id FROM step_attempts a JOIN steps s ON s.id=a.step_id WHERE a.node_id<>'legacy' AND a.process_exit_confirmed_at IS NULL").fetchall()
        for row in rows:
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if self.engine.repo._stop_attempt(row):
                    break
                time.sleep(.1)


@pytest.fixture
def nodes(engine, request):
    rig = Nodes(engine, getattr(request, "param", {}))
    try:
        yield rig
    finally:
        rig.close()


def test_two_nodes_four_gpu_devices_and_node_cpu_budgets(nodes):
    engine = nodes.engine
    # Monitoring is read-only: inventory sync alone has not probed the mount or run jobs.
    assert len(engine.client.get("/api/v1/nodes").json()["nodes"]) == 2
    assert not (engine.settings.artifact_root / "_system" / "storage-id.json").exists()
    tasks = [nodes.task(resource="mock-gpu", batches=50, duration=.2) for _ in range(4)]
    for index in range(5):
        engine.worker("node-slot-" + str(index))
    for task_id in tasks:
        engine.wait(task_id, lambda t: t["steps"][0]["status"] == "RUNNING", timeout=20)
    resources = engine.repo.resources()
    occupied = [(s["node_id"], s["device_id"]) for n in resources["nodes"] for s in n["slots"] if s["owner_attempt_id"]]
    assert set(occupied) == {("a", "gpu-0"), ("a", "gpu-1"), ("b", "gpu-0"), ("b", "gpu-1")}
    blocked = nodes.task(resource="mock-gpu")
    time.sleep(.2)
    assert attempts(engine, blocked, "work") == []
    for task_id in [*tasks, blocked]:
        assert engine.wait(task_id, timeout=25)["status"] == "SUCCEEDED", engine.logs()
        history = attempts(engine, task_id, "work")
        assert len(history) == 1 and history[0]["status"] == "SUCCEEDED"
    cpu = [nodes.task(batches=30, duration=.15) for _ in range(2)]
    for task_id in cpu:
        engine.wait(task_id, lambda t: t["steps"][0]["status"] == "RUNNING")
    owners = [attempts(engine, task_id, "work")[0] for task_id in cpu]
    assert {a["node_id"] for a in owners} == {"a", "b"}  # Legacy CPU budget is only 1.
    assert all(n["cpu"]["in_use"] == n["cpu"]["capacity"] == 1 for n in engine.repo.resources()["nodes"])
    third = nodes.task()
    time.sleep(.2)
    assert attempts(engine, third, "work") == []
    for task_id in [*cpu, third]:
        assert engine.wait(task_id, timeout=25)["status"] == "SUCCEEDED"
    assert all(s["state"] == "AVAILABLE" for n in engine.repo.resources()["nodes"] for s in n["slots"])


@pytest.mark.parametrize("nodes", [{"kinds": ("builtin", "standalone")}], indirect=True)
def test_heterogeneous_successor_consumes_actual_shared_committed_output(nodes):
    engine = nodes.engine
    task_id = engine.submit({"pipeline_id": "managed-v1", "execution_backend": "external"})
    register(engine, task_id, "prepare", processor_version="1", parameters={"batch_count": 2})
    engine.worker("shared-consumer")
    first = engine.wait(task_id, lambda t: t["phase"] == "WAITING_STEPS")
    before = step(first, "prepare")["output_refs"]
    register(engine, task_id, "evaluate", "prepare", processor_version="independent-mock-v1")
    finish(engine, task_id, "evaluate")
    final = engine.wait(task_id)
    assert final["status"] == "SUCCEEDED", engine.logs()
    assert attempts(engine, task_id, "prepare")[0]["node_id"] == "a"
    assert attempts(engine, task_id, "evaluate")[0]["node_id"] == "b"
    artifacts = engine.repo.artifacts(task_id)
    producer = next(a for a in artifacts if a["id"] in before)
    result = next(a for a in artifacts if a["id"] in final["output_refs"])
    assert result["metadata"]["dependencies"] == [producer["content_fingerprint"]]
    assert json.loads((engine.settings.artifact_root / result["relative_path"]).read_bytes()) == result["metadata"]
    assert len(attempts(engine, task_id, "prepare")) == 1
    encoded = json.dumps(final)
    assert all(engine.environment[s.config["secret_env"]] not in encoded for s in nodes.services)


@pytest.mark.parametrize("nodes", [{"fault": f} for f in ("down", "wrong-mount", "draining")], indirect=True)
def test_unusable_first_node_does_not_block_compatible_other_node(nodes):
    engine = nodes.engine
    task_id = nodes.task(batches=1, duration=5.2)
    engine.worker("healthy-node-heartbeat")
    assert engine.wait(task_id, timeout=25)["status"] == "SUCCEEDED", engine.logs()
    history = attempts(engine, task_id, "work")
    assert len(history) == 1 and history[0]["node_id"] == "b"
    assert (datetime.fromisoformat(history[0]["heartbeat_at"]) - datetime.fromisoformat(history[0]["started_at"])).total_seconds() >= 4.5
    assert nodes.status(history[0]["id"])["start_count"] == 1


def test_lost_node_keeps_its_resource_and_old_messages_cannot_touch_new_owner(nodes):
    engine = nodes.engine
    task_id = engine.submit({"pipeline_id": "managed-v1", "execution_backend": "external"})
    register(engine, task_id, "prepare")
    register(engine, task_id, "render", "prepare", resource_kind="mock-gpu", parameters={"batch_count": 35, "batch_duration_seconds": .2})
    old_worker = engine.worker("node-old")
    engine.wait(task_id, lambda t: step(t, "render")["checkpoint_artifact_id"] is not None)
    old = attempts(engine, task_id, "render")[0]
    assert old["node_id"] == "a"
    old_claim = command(engine, old["id"])["payload"]
    nodes.kill(nodes.services[0]); old_worker.kill(); old_worker.wait(timeout=5)
    engine.worker("node-takeover")
    engine.wait(task_id, lambda t: step(t, "render")["status"] == "RUNNING" and step(t, "render")["execution_generation"] == 2, timeout=20)
    replacement = attempts(engine, task_id, "render")[1]
    assert replacement["node_id"] == "b"
    slots = {s["id"]: s for n in engine.repo.resources()["nodes"] for s in n["slots"]}
    assert slots[old["resource_slot_id"]]["state"] == "QUARANTINED"
    assert slots[replacement["resource_slot_id"]]["owner_attempt_id"] == replacement["id"]
    assert not engine.repo.heartbeat(old_claim) and not engine.repo.progress(old_claim, {"completed_units": 999})
    current = command(engine, replacement["id"])["payload"]
    assert not engine.repo.heartbeat({**current, "node_id": "a"})
    assert not engine.repo.progress({**current, "resource_device_id": "gpu-1"}, {"completed_units": 999})
    # Wrong-node requests and receipts cannot acknowledge/clear this owner.
    headers = auth_headers(engine.environment[nodes.services[0].config["secret_env"]], binding_for(old_claim))
    response = httpx.post(nodes.services[1].config["url"] + f"/v1/jobs/{old['id']}/stop", json={}, headers=headers)
    assert response.status_code in (401, 403)
    with pytest.raises(ValueError):
        engine.repo.record_external_exit(old_claim, nodes.status(replacement["id"]))
    # Same address/key with a fresh store is NOT proof about the original job.
    service = nodes.services[0]
    original_store = service.state
    try:
        service.state = original_store.with_name(original_store.name + "-fresh")
        nodes.start(service)
        record = command(engine, old["id"])
        fresh = ExternalClient(engine.settings, record["binding"])
        receipt = fresh.call("POST", "/stop", {})
        assert receipt["exit_confirmed"] and receipt["start_count"] == 0
        assert receipt["state_id"] != record["route"]["state_id"]
        assert not engine.repo.record_external_exit(old_claim, receipt)
        with pytest.raises(ValueError):
            ExternalClient(engine.settings, record["binding"], record["route"]).call("POST", "/stop", {})
        with pytest.raises(HTTPError) as rejected:
            fresh.call("PUT", body={"claim": old_claim, "lease_expires_at": old_claim["lease_expires_at"]})
        assert rejected.value.code == 422
        slots = {s["id"]: s for n in engine.repo.resources()["nodes"] for s in n["slots"]}
        assert slots[old["resource_slot_id"]]["state"] == "QUARANTINED"
    finally:
        nodes.kill(service)
        service.state = original_store
    nodes.start(nodes.services[0])
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        slots = {s["id"]: s for n in engine.repo.resources()["nodes"] for s in n["slots"]}
        if slots[old["resource_slot_id"]]["state"] == "AVAILABLE":
            break
        time.sleep(.1)
    assert slots[old["resource_slot_id"]]["state"] == "AVAILABLE"
    assert slots[replacement["resource_slot_id"]]["owner_attempt_id"] == replacement["id"]
    finish(engine, task_id, "render")
    assert engine.wait(task_id, timeout=25)["status"] == "SUCCEEDED", engine.logs()
    history = attempts(engine, task_id, "render")
    assert [a["status"] for a in history] == ["LOST", "SUCCEEDED"]
    checkpoint = next(a for a in engine.repo.artifacts(task_id) if a["id"] == history[1]["resumed_from_checkpoint_id"])
    output = next(a for a in engine.repo.artifacts(task_id) if a["id"] in step(engine.task(task_id), "render")["output_refs"])
    prefix = checkpoint["metadata"]["completed_batches"]
    assert output["metadata"]["resumed_batches"] == len(prefix)
    assert output["metadata"]["completed_batches"][:len(prefix)] == prefix
    assert all(history[1]["id"] in b["relative_path"] for b in output["metadata"]["completed_batches"][len(prefix):])
    assert len(attempts(engine, task_id, "prepare")) == 1
    assert all(nodes.status(a["id"])["start_count"] == 1 for a in history)


def test_runtime_capability_mismatch_does_not_allocate_attempt_or_start(nodes):
    engine = nodes.engine
    nodes.services[0].config["processors"] = {"mock-step": ["1"]}  # Deliberately wrong declaration.
    nodes.configure()
    task_id = nodes.task(version="1")
    engine.worker("no-unsupported-node-dispatch")
    task = engine.wait(task_id, lambda t: t["wait_reason"] == "NO_COMPATIBLE_NODE")
    assert task["steps"][0]["execution_generation"] == 0
    assert attempts(engine, task_id, "work") == []
    with connect(engine.settings) as conn:
        assert conn.execute("SELECT count(*) AS n FROM execution_commands").fetchone()["n"] == 0
    assert not (engine.settings.artifact_root / task_id).exists()


def test_response_loss_reuses_same_bound_node_job_and_device(nodes):
    engine = nodes.engine
    target = nodes.services[0].config["url"]
    seen = []
    class Proxy(BaseHTTPRequestHandler):
        def forward(self):
            raw = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            headers = {k: v for k, v in self.headers.items() if k.lower() not in {"host", "connection", "content-length"}}
            try:
                with urlopen(Request(target + self.path, data=raw if self.command != "GET" else None, headers=headers, method=self.command), timeout=3) as response:
                    status, content = response.status, response.read()
            except HTTPError as error:
                status, content = error.code, error.read()
            if self.command == "PUT":
                seen.append(self.path)
                if len(seen) == 1:
                    status, content = 503, b'{}'
            self.send_response(status); self.send_header("Content-Length", str(len(content))); self.end_headers()
            try:
                self.wfile.write(content)
            except OSError:
                pass
        do_GET = do_POST = do_PUT = forward
        def log_message(self, *_):
            pass
    proxy = ThreadingHTTPServer(("127.0.0.1", 0), Proxy)
    thread = Thread(target=proxy.serve_forever, daemon=True); thread.start()
    try:
        nodes.services[0].config["url"] = f"http://127.0.0.1:{proxy.server_port}"
        nodes.configure()
        task_id = nodes.task(resource="mock-gpu", batches=3, duration=.15)
        engine.worker("same-node-command")
        assert engine.wait(task_id)["status"] == "SUCCEEDED", engine.logs()
        history = attempts(engine, task_id, "work")
        assert len(history) == 1 and history[0]["node_id"] == "a"
        assert len(seen) >= 2 and len(set(seen)) == 1
        assert nodes.status(history[0]["id"])["start_count"] == 1
        record = command(engine, history[0]["id"])
        assert record["route"]["url"] == nodes.services[0].config["url"]
        changed = engine.settings.model_copy(update={"execution_nodes": [n.model_copy(update={"url": target}) if n.node_id == "a" else n for n in engine.settings.execution_nodes]})
        with pytest.raises(ValueError):
            ExternalClient(changed, record["binding"], record["route"])
    finally:
        # Completed jobs already have durable exit receipts; no live resource is
        # released by swapping the route. Keep proxy alive through cleanup.
        nodes.close()
        proxy.shutdown(); proxy.server_close(); thread.join(timeout=3)


@pytest.mark.parametrize("nodes", [{"tls": True, "kinds": (kind,)} for kind in ("builtin", "standalone")], indirect=True)
def test_local_verified_https_and_hostname_rejection(nodes):
    engine = nodes.engine
    task_id = nodes.task(version="1" if nodes.services[0].kind == "builtin" else "independent-mock-v1")
    engine.worker("verified-node-tls")
    assert engine.wait(task_id)["status"] == "SUCCEEDED", engine.logs()
    from app.nodes import inspection_binding, storage_reference
    node = engine.settings.execution_nodes[0]
    wrong_host = node.model_copy(update={"url": node.url.replace("127.0.0.1", "localhost")})
    settings = engine.settings.model_copy(update={"execution_nodes": [wrong_host]})
    with pytest.raises(URLError):
        ExternalClient(settings, inspection_binding(wrong_host)).inspect(settings.artifact_storage_id, [storage_reference(settings)])


@pytest.mark.parametrize("url", ["http://remote.example:8090", "https://user:password@remote.example", "https://remote.example/path"])
def test_configured_remote_targets_require_controlled_verified_origin(url):
    with pytest.raises(ValidationError):
        Settings.from_env({"EXECUTION_NODES_JSON": json.dumps([{"node_id": "a", "executor_id": "a", "url": url,
            "secret_env": "NODE_A_SECRET"}]), "NODE_A_SECRET": "a" * 32})
